"""CSV intake for waveform frame batches.

Accepted format (header optional)::

    transfer_id,frame_no,payload_hex
    SAMPLE-2026-001,1,0011aaff...
    SAMPLE-2026-001,2,...
    SAMPLE-2026-001,P,<512 hex chars>      # parity (XOR) frame

Two-column rows (``frame_no,payload_hex``) take the transfer ID from the URL.
Frame numbers may be ``P``/``parity``/``xor``/``校验`` for the parity frame.
Blank lines and ``#`` comments are ignored.
"""

from __future__ import annotations

import csv
import io

from .storage import (
    FRAME_BYTES,
    MAX_FRAMES,
    StorageError,
    normalize_transfer_id,
    parse_hex_payload,
)

MAX_CSV_BYTES = 256 * 1024
PARITY_LABELS = {"p", "parity", "xor", "check", "校验", "校验帧"}

_HEADER_TID = {"transfer_id", "transfer", "tid", "传输号"}
_HEADER_NO = {"frame_no", "frame", "no", "帧号"}
_HEADER_PAYLOAD = {"payload_hex", "payload", "hex", "data", "载荷", "十六进制载荷"}


def _normalize_header(name: str) -> str:
    return name.strip().lower().replace(" ", "_")


def parse_frames_csv(
    raw: bytes | str, transfer_id: str
) -> tuple[list[tuple[int, bytes]], bytes | None]:
    """Parse a CSV batch. Returns ``(frames, parity_or_none)``.

    Raises :class:`StorageError` with the offending line number on malformed input.
    """
    tid = normalize_transfer_id(transfer_id)
    if isinstance(raw, bytes):
        if len(raw) > MAX_CSV_BYTES:
            raise StorageError("CSV 文件过大")
        try:
            text = raw.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise StorageError("CSV 必须为 UTF-8 编码") from exc
    else:
        text = raw.lstrip("﻿")

    if not text.strip():
        raise StorageError("CSV 为空")

    rows = list(csv.reader(io.StringIO(text)))
    frames: list[tuple[int, bytes]] = []
    parity: bytes | None = None
    columns: dict[str, int] | None = None
    known_headers = _HEADER_TID | _HEADER_NO | _HEADER_PAYLOAD

    for line_no, cells in enumerate(rows, start=1):
        stripped = [cell.strip() for cell in cells]
        if not stripped or all(not cell for cell in stripped):
            continue
        if stripped[0].startswith("#"):
            continue

        # Header row detection: only when recognisable column names appear. Data
        # rows (even ones beginning with a text transfer ID) are never mistaken for
        # headers.
        normalized = [_normalize_header(cell) for cell in stripped]
        if columns is None and _HEADER_NO & set(normalized) and (
            _HEADER_PAYLOAD & set(normalized) or _HEADER_TID & set(normalized)
        ):
            columns = {}
            for index, name in enumerate(normalized):
                if name in _HEADER_TID:
                    columns["tid"] = index
                elif name in _HEADER_NO:
                    columns["no"] = index
                elif name in _HEADER_PAYLOAD:
                    columns["payload"] = index
            if "payload" not in columns:
                raise StorageError(f"CSV 第 {line_no} 行表头需包含十六进制载荷列")
            continue

        if columns is not None:
            tid_idx = columns.get("tid")
            no_idx = columns["no"]
            payload_idx = columns["payload"]
        else:
            tid_idx = None
            if len(stripped) >= 3:
                # Documented headerless layout: transfer_id,frame_no,payload.
                tid_idx = 0
                no_idx = 1
                payload_idx = 2
            elif len(stripped) == 2:
                no_idx = 0
                payload_idx = 1
            else:
                raise StorageError(f"CSV 第 {line_no} 行需至少两列（帧号,载荷）")

        if max(no_idx, payload_idx) >= len(stripped) or (
            tid_idx is not None and tid_idx >= len(stripped)
        ):
            raise StorageError(f"CSV 第 {line_no} 行列数不足")

        row_tid = stripped[tid_idx] if tid_idx is not None else ""
        if row_tid and normalize_transfer_id(row_tid) != tid:
            raise StorageError(
                f"CSV 第 {line_no} 行传输号 {row_tid} 与本次上传的 {tid} 不一致"
            )

        frame_label = stripped[no_idx]
        payload_hex = stripped[payload_idx]

        if frame_label.lower() in PARITY_LABELS:
            if parity is not None:
                raise StorageError("CSV 中包含多个校验帧")
            parity = parse_hex_payload(payload_hex)
            continue

        try:
            frame_no = int(frame_label)
        except ValueError as exc:
            raise StorageError(f"CSV 第 {line_no} 行帧号不是整数：{frame_label!r}") from exc
        if not 1 <= frame_no <= MAX_FRAMES:
            raise StorageError(f"CSV 第 {line_no} 行帧号超出 1～{MAX_FRAMES}")
        # Duplicate numbers inside one batch are resolved by the storage layer
        # (identical duplicates are de-duplicated, conflicting ones rejected).
        frames.append((frame_no, parse_hex_payload(payload_hex, frame_no=frame_no)))
        if len(frames) > MAX_FRAMES:
            raise StorageError(f"单批最多 {MAX_FRAMES} 帧")

    if not frames and parity is None:
        raise StorageError("CSV 中没有可用的数据帧或校验帧")
    return frames, parity


def xor_parity(frames: list[bytes]) -> bytes:
    """XOR of 256-byte frames; the parity frame definition for this service."""
    acc = bytearray(FRAME_BYTES)
    for frame in frames:
        if len(frame) != FRAME_BYTES:
            raise ValueError("frame must be 256 bytes")
        for i, value in enumerate(frame):
            acc[i] ^= value
    return bytes(acc)
