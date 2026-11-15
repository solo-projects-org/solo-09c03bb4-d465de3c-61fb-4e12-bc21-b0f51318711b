#!/usr/bin/env python3
"""Upload one sample transfer to a running intake service (stdlib only).

Usage::

    python3 scripts/send_sample.py samples/STA-20261115-002 \\
        --base-url http://localhost:8080

Registers the transfer from ``meta.json``, then posts each ``batch-*.csv`` and
``parity.csv`` as multipart file uploads, printing the resulting status.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path


def post_json(base: str, path: str, payload: dict) -> tuple[int, dict]:
    request = urllib.request.Request(
        base + path,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read() or b"{}")


def post_csv(base: str, path: str, filename: str, content: bytes) -> tuple[int, str]:
    boundary = uuid.uuid4().hex
    body = (
        f'--{boundary}\r\nContent-Disposition: form-data; name="csv_file"; '
        f'filename="{filename}"\r\nContent-Type: text/csv\r\n\r\n'
    ).encode() + content + f"\r\n--{boundary}--\r\n".encode()
    request = urllib.request.Request(
        base + path,
        data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, response.geturl()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.geturl() or ""


def get_json(base: str, path: str) -> dict:
    with urllib.request.urlopen(base + path, timeout=30) as response:
        return json.loads(response.read())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sample_dir", type=Path)
    parser.add_argument("--base-url", default="http://localhost:8080")
    args = parser.parse_args()

    folder: Path = args.sample_dir
    meta = json.loads((folder / "meta.json").read_text(encoding="utf-8"))
    tid = meta["transfer_id"]
    base = args.base_url.rstrip("/")

    status, body = post_json(
        base,
        "/api/deliveries",
        {
            "transfer_id": tid,
            "frame_count": meta["frame_count"],
            "size_bytes": meta["size_bytes"],
            "sha256": meta["sha256"],
        },
    )
    print(f"register {tid}: {status}")
    if status not in (200, 201):
        print(json.dumps(body, ensure_ascii=False))
        return 1

    files = sorted(folder.glob("batch-*.csv")) + sorted(folder.glob("parity.csv"))
    for csv_path in files:
        # Use the HTML form route: it accepts multipart CSV files.
        status, url = post_csv(
            base, f"/deliveries/{tid}/frames", csv_path.name, csv_path.read_bytes()
        )
        note = urllib.parse.unquote(url.split("?")[-1]) if "?" in url else ""
        print(f"  {csv_path.name}: {status} {note}")

    final = get_json(base, f"/api/deliveries/{tid}")
    print(
        f"status={final['status']} received={final['received_frames']} "
        f"missing={final['missing_frames']} recovered={final['recovered_frames']}"
    )
    if final["status"] == "published":
        print(f"  download: {base}/api/deliveries/{tid}/download")
    return 0


if __name__ == "__main__":
    sys.exit(main())
