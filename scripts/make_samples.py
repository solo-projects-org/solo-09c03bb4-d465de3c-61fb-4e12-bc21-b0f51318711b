#!/usr/bin/env python3
"""Generate deterministic fictional waveform sample batches.

The payloads are synthetic (seeded pseudo-noise shaped by a seismic-like
envelope); no real station data is used. Output layout::

    samples/<transfer>/
        meta.json          declared frame count / size / sha256 / parity
        expected.bin       the exact archive a successful intake publishes
        batch-01.csv       frames in satellite-delivery order
        ...
        parity.csv         optional XOR parity frame (frame label "P")

Each transfer demonstrates one intake path:

* ``STA-20261115-001`` complete delivery across two batches.
* ``STA-20261115-002`` one frame lost, recovered with the parity frame.
* ``STA-20261115-003`` two frames lost, so intake must wait for re-transmission.
* ``STA-20261115-004`` every frame arrives but the declared digest is wrong,
  so intake must retain fragments and refuse to publish.
* ``STA-20261115-005`` publishes cleanly but contains a 100-sample silence run
  and a 20-sample clipping run, so quality interpretation quarantines it.
"""

from __future__ import annotations

import hashlib
import json
import math
import random
import struct
from pathlib import Path

FRAME_BYTES = 256
OUT = Path(__file__).resolve().parent.parent / "samples"


def synth_waveform(size_bytes: int, seed: int) -> bytes:
    """Seeded 16-bit little-endian synthetic seismogram with a P-wave onset."""
    rng = random.Random(seed)
    values = []
    onset = int(size_bytes / 4) if size_bytes > 8 else 0
    for i in range(size_bytes // 2):
        envelope = math.exp(-((i - onset) ** 2) / (2 * (size_bytes / 9) ** 2))
        value = int(3200 * envelope * math.sin(i * 0.21) + rng.gauss(0, 420))
        values.append(max(-32768, min(32767, value)))
    out = struct.pack("<" + "h" * len(values), *values)
    if len(out) < size_bytes:
        out += bytes(size_bytes - len(out))
    return out


def split_frames(archive: bytes, frame_count: int) -> list[bytes]:
    padded = archive + b"\x00" * (frame_count * FRAME_BYTES - len(archive))
    return [padded[i * FRAME_BYTES : (i + 1) * FRAME_BYTES] for i in range(frame_count)]


def parity_of(frames: list[bytes]) -> bytes:
    acc = bytearray(FRAME_BYTES)
    for frame in frames:
        for i, value in enumerate(frame):
            acc[i] ^= value
    return bytes(acc)


def write_csv(path: Path, transfer_id: str, rows: list[tuple[str, bytes]]) -> None:
    lines = ["transfer_id,frame_no,payload_hex"]
    for label, payload in rows:
        lines.append(f"{transfer_id},{label},{payload.hex()}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build(
    transfer_id: str,
    seed: int,
    frame_count: int,
    size_bytes: int,
    *,
    declare_sha: str | None = None,
    drop: tuple[int, ...] = (),
    parity: bool = False,
    batches: dict[str, list[int]] | None = None,
    archive: bytes | None = None,
    note: str = "fictional synthetic waveform; safe to upload",
) -> None:
    folder = OUT / transfer_id
    folder.mkdir(parents=True, exist_ok=True)
    if archive is None:
        archive = synth_waveform(size_bytes, seed)
    digest = hashlib.sha256(archive).hexdigest()
    frames = split_frames(archive, frame_count)
    declared = declare_sha or digest

    meta = {
        "transfer_id": transfer_id,
        "frame_count": frame_count,
        "size_bytes": size_bytes,
        "sha256": declared,
        "true_sha256": digest,
        "frame_bytes": FRAME_BYTES,
        "dropped_frames": list(drop),
        "has_parity": parity,
        "note": note,
    }
    (folder / "meta.json").write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")
    (folder / "expected.bin").write_bytes(archive)

    plan = batches or {
        "batch-01.csv": list(range(1, frame_count + 1)),
    }
    for name, numbers in plan.items():
        rows = [(str(n), frames[n - 1]) for n in numbers if n not in drop]
        write_csv(folder / name, transfer_id, rows)
    if parity:
        write_csv(folder / "parity.csv", transfer_id, [("P", parity_of(frames))])


def qc_demo_archive(size_bytes: int, seed: int) -> bytes:
    """Quiet synthetic trace with one silence run and one clipping run.

    500 samples (1000 bytes): samples 120..219 are zero (silence starts at
    sample #120), samples 400..419 sit at full scale (clipping starts at #400).
    """
    rng = random.Random(seed)
    values = [int(rng.gauss(0, 300)) for _ in range(size_bytes // 2)]
    for i in range(120, 220):
        values[i] = 0
    for i in range(400, 420):
        values[i] = 32767
    return struct.pack("<" + "h" * len(values), *values)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    build(
        "STA-20261115-001",
        seed=1871,
        frame_count=4,
        size_bytes=900,
        batches={
            "batch-01.csv": [1, 2],
            "batch-02.csv": [3, 4],
        },
    )
    build(
        "STA-20261115-002",
        seed=20261115,
        frame_count=5,
        size_bytes=1111,
        drop=(3,),
        parity=True,
        batches={
            "batch-01.csv": [1, 2],
            "batch-02.csv": [4, 5],
        },
    )
    build(
        "STA-20261115-003",
        seed=733,
        frame_count=6,
        size_bytes=1512,
        drop=(2, 5),
        parity=True,
        batches={
            "batch-01.csv": [1, 3],
            "batch-02.csv": [4, 6],
        },
    )
    build(
        "STA-20261115-004",
        seed=419,
        frame_count=3,
        size_bytes=640,
        declare_sha="0" * 64,
        batches={"batch-01.csv": [1, 2, 3]},
    )
    build(
        "STA-20261115-005",
        seed=97,
        frame_count=4,
        size_bytes=1000,
        archive=qc_demo_archive(1000, 97),
        batches={
            "batch-01.csv": [1, 2],
            "batch-02.csv": [3, 4],
        },
        note="fictional trace with silence (#120) and clipping (#400) for QC demo",
    )
    print(f"samples written to {OUT}")


if __name__ == "__main__":
    main()
