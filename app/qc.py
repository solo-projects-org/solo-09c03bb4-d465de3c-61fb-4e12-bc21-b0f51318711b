"""Waveform quality interpretation (质量判读) for published archives.

The inspector confirms the archive is **100 Hz, signed 16-bit little-endian**
sampled, then a run scans the *real bytes of the published original*:

* **静默 (silence)**: ``SILENCE_RUN`` consecutive zero-valued samples.
* **削顶 (clipping)**: ``CLIP_RUN`` consecutive samples with ``abs(v) >= CLIP_ABS``.

For each anomaly class the **starting sample index of the first violating run**
is recorded (0-based; at 100 Hz sample ``i`` corresponds to ``i/100`` s).

Run lifecycle (per transfer, monotonically increasing ``run_no``):

* ``running``         – created at 发起判读; new downloads pause immediately.
* ``passed``          – scan finished, no anomaly: downloads resume.
* ``quarantined``     – anomaly found: downloads stay blocked, reason kept.
* ``pending_review``  – digest/format of the original is abnormal: 维持待复核.
* ``interrupted``     – process died before finishing (swept at startup): 待复核.

The *effective* QC state of a transfer is always derived from its **latest**
run (highest ``run_no``). A stale run that finishes late only rewrites its own
history row, so an old result can never overwrite a newer conclusion, even when
two web instances interleave runs.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Sequence

# Only this acquisition format can be interpreted; the inspector confirms it
# explicitly when launching a run.
SAMPLE_RATE = 100
SAMPLE_FORMAT = "s16le"
SAMPLE_FORMAT_LABEL = "有符号 16 位小端"

SILENCE_RUN = 100  # consecutive zero samples => silence
CLIP_RUN = 20  # consecutive |sample| >= CLIP_ABS => clipping
CLIP_ABS = 32760

# Effective (delivery-level) QC states.
QC_NONE = "none"  # never interpreted: downloads unaffected
QC_PENDING = "pending"  # 待复核: downloads paused
QC_QUARANTINED = "quarantined"  # 隔离: downloads blocked, reason shown
QC_PASSED = "passed"  # latest run passed: downloads allowed

QC_STATUS_LABELS = {
    QC_NONE: "未判读",
    QC_PENDING: "待复核",
    QC_QUARANTINED: "已隔离",
    QC_PASSED: "判读通过",
}

# Per-run statuses.
RUN_RUNNING = "running"
RUN_PASSED = "passed"
RUN_QUARANTINED = "quarantined"
RUN_PENDING_REVIEW = "pending_review"
RUN_INTERRUPTED = "interrupted"

RUN_STATUS_LABELS = {
    RUN_RUNNING: "判读中",
    RUN_PASSED: "通过",
    RUN_QUARANTINED: "隔离",
    RUN_PENDING_REVIEW: "待复核",
    RUN_INTERRUPTED: "已中断",
}

# How each run status maps to the effective delivery-level QC state.
RUN_TO_QC = {
    RUN_RUNNING: QC_PENDING,
    RUN_INTERRUPTED: QC_PENDING,
    RUN_PENDING_REVIEW: QC_PENDING,
    RUN_QUARANTINED: QC_QUARANTINED,
    RUN_PASSED: QC_PASSED,
}


def decode_s16le(data: bytes) -> tuple[int, ...]:
    """Decode signed 16-bit little-endian samples. Caller guarantees even length."""
    return struct.unpack(f"<{len(data) // 2}h", data)


def scan_samples(samples: Sequence[int]) -> tuple[int | None, int | None]:
    """Return ``(silence_index, clip_index)``: 0-based start of the first run
    reaching each threshold, or ``None`` when that anomaly never occurs."""
    silence_start: int | None = None
    clip_start: int | None = None
    zero_run = 0
    clip_run = 0
    for i, value in enumerate(samples):
        zero_run = zero_run + 1 if value == 0 else 0
        if silence_start is None and zero_run >= SILENCE_RUN:
            silence_start = i - SILENCE_RUN + 1
        if value >= CLIP_ABS or value <= -CLIP_ABS:
            clip_run += 1
        else:
            clip_run = 0
        if clip_start is None and clip_run >= CLIP_RUN:
            clip_start = i - CLIP_RUN + 1
        if silence_start is not None and clip_start is not None:
            break
    return silence_start, clip_start


@dataclass(frozen=True)
class QcRun:
    """One interpretation round; immutable history row."""

    transfer_id: str
    run_no: int
    status: str
    sample_rate: int
    sample_format: str
    sample_count: int | None
    silence_index: int | None
    clip_index: int | None
    digest: str | None  # sha256 of the bytes actually scanned
    reason: str
    started_at: str
    finished_at: str | None

    @property
    def status_label(self) -> str:
        return RUN_STATUS_LABELS.get(self.status, self.status)

    def seconds(self, index: int | None) -> str:
        """Human time offset of a sample index at the confirmed sample rate."""
        if index is None:
            return ""
        return f"{index / self.sample_rate:.2f} s"


@dataclass(frozen=True)
class QcStatus:
    """Effective QC state of one transfer, derived from its latest run."""

    status: str  # QC_NONE / QC_PENDING / QC_QUARANTINED / QC_PASSED
    latest: QcRun | None

    @property
    def label(self) -> str:
        return QC_STATUS_LABELS.get(self.status, self.status)

    @property
    def download_allowed(self) -> bool:
        """未发起判读的原档照旧下载；仅最新一轮判读通过才恢复下载。"""
        return self.status in (QC_NONE, QC_PASSED)
