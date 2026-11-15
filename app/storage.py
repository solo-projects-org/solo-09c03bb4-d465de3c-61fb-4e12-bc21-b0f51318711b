"""Waveform archive intake: frame assembly, XOR parity recovery, verified publishing.

Design invariants:

* A data frame is exactly 256 bytes, transmitted as 512 hex characters.
* The last frame is zero-padded; the true archive is the first ``size_bytes`` bytes
  of the concatenated frames (at most ``frame_count * 256``).
* One optional 256-byte parity frame holds the byte-wise XOR of all data frames.
  It can reconstruct **at most one** missing frame.
* A reconstructed frame is *tentative*: it is used in memory for assembly but is
  never stored as a received frame, so the genuine re-transmission is always
  accepted instead of being rejected as a conflicting payload.
* Two or more missing frames: wait for re-transmission; nothing is published.
* Publication requires a complete archive whose truncated bytes match the declared
  SHA-256. On mismatch all received fragments are retained for a later repair.
* Publication is atomic (temp file -> fsync -> rename -> fsync directory -> DB row).
  A crash therefore leaves either no download or the complete published archive.
* A transfer is published at most once; late/duplicate fragments never alter it.
* Same transfer + same frame number with different content is rejected.
"""

from __future__ import annotations

import hashlib
import os
import re
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator, Sequence

try:  # POSIX only; Docker image is Linux.
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None  # type: ignore[assignment]

FRAME_BYTES = 256
FRAME_HEX_LEN = FRAME_BYTES * 2
MIN_FRAMES = 2
MAX_FRAMES = 16
HEX_RE = re.compile(r"^[0-9a-fA-F]*$")
TID_RE = re.compile(r"[A-Za-z0-9._-]+")

STATUS_WAITING = "waiting"
STATUS_MISMATCH = "mismatch"
STATUS_PUBLISHED = "published"


class StorageError(Exception):
    """Base class for user-facing intake errors."""

    http_status = 400

    def __init__(self, message: str, *, status: int | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status or self.http_status


class ConflictError(StorageError):
    http_status = 409


class NotFoundError(StorageError):
    http_status = 404


def utcnow() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def normalize_transfer_id(raw: str) -> str:
    """Transfer IDs are opaque but must be safe as a directory name."""
    value = (raw or "").strip()
    if not value:
        raise StorageError("传输号不能为空")
    if len(value) > 64:
        raise StorageError("传输号过长（最多 64 个字符）")
    if not TID_RE.fullmatch(value) or value in {".", ".."}:
        raise StorageError("传输号只能包含字母、数字、点、下划线和连字符")
    return value


def parse_hex_payload(raw: str, *, frame_no: int | None = None) -> bytes:
    data = (raw or "").strip()
    # Tolerate whitespace and an optional 0x prefix from CSV exports.
    if data[:2] in {"0x", "0X"}:
        data = data[2:]
    data = re.sub(r"\s+", "", data)
    where = f"第 {frame_no} 帧" if frame_no is not None else "校验帧"
    if len(data) != FRAME_HEX_LEN:
        raise StorageError(
            f"{where}载荷必须为 {FRAME_HEX_LEN} 个十六进制字符（256 字节），实际 {len(data)}"
        )
    if not HEX_RE.fullmatch(data):
        raise StorageError(f"{where}载荷包含非十六进制字符")
    try:
        return bytes.fromhex(data)
    except ValueError as exc:  # pragma: no cover - guarded by the regex
        raise StorageError(f"{where}载荷无法解析") from exc


def parse_size_bytes(raw: int | str) -> int:
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError) as exc:
        raise StorageError("原档字节数必须为整数") from exc
    if value < 1:
        raise StorageError("原档字节数必须大于 0")
    return value


def parse_frame_count(raw: int | str) -> int:
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError) as exc:
        raise StorageError("数据帧数必须为整数") from exc
    if not MIN_FRAMES <= value <= MAX_FRAMES:
        raise StorageError(f"数据帧数必须在 {MIN_FRAMES}～{MAX_FRAMES} 之间")
    return value


def parse_sha256(raw: str) -> str:
    value = (raw or "").strip().lower()
    if value.startswith("sha256:"):
        value = value[7:]
    if not re.fullmatch(r"[0-9a-f]{64}", value):
        raise StorageError("SHA-256 必须为 64 位十六进制字符串")
    return value


def validate_declared_size(count: int, size_bytes: int) -> None:
    """A padded archive of ``count`` full frames truncates to 1..count*256 bytes.

    With at least two frames the archive must use more than ``count-1`` full
    frames, otherwise the declared frame count is inconsistent.
    """
    minimum = (count - 1) * FRAME_BYTES + 1
    maximum = count * FRAME_BYTES
    if not minimum <= size_bytes <= maximum:
        raise StorageError(
            f"原档字节数需与 {count} 个数据帧相容（{minimum}～{maximum} 字节，末帧补零）"
        )


def xor_bytes(parts: Iterable[bytes]) -> bytes:
    acc = bytearray(FRAME_BYTES)
    for part in parts:
        if len(part) != FRAME_BYTES:
            raise ValueError("XOR operand must be 256 bytes")
        for i, value in enumerate(part):
            acc[i] ^= value
    return bytes(acc)


@dataclass(frozen=True)
class Delivery:
    transfer_id: str
    frame_count: int
    size_bytes: int
    sha256: str
    created_at: str


@dataclass(frozen=True)
class ArchiveState:
    delivery: Delivery
    frames: dict[int, bytes]
    parity: bytes | None
    status: str
    published_sha256: str | None
    published_size: int | None
    published_at: str | None
    missing_frames: tuple[int, ...]
    parity_recoverable: bool
    actual_sha256: str | None
    recovered_frames: tuple[int, ...]
    tentative_frame: int | None
    """Frame currently missing but reconstructed in memory by the parity frame."""

    @property
    def received_frames(self) -> tuple[int, ...]:
        return tuple(sorted(self.frames))

    @property
    def is_complete(self) -> bool:
        return not self.missing_frames

    @property
    def can_attempt(self) -> bool:
        """Enough material exists to try assembling the archive."""
        return not self.missing_frames or (len(self.missing_frames) == 1 and self.parity is not None)

    @property
    def tentative_frame_no(self) -> int | None:
        """Missing frame that parity could rebuild, if verification has not passed."""
        if self.status != STATUS_PUBLISHED and len(self.missing_frames) == 1 and self.parity is not None:
            return self.missing_frames[0]
        return None


class Storage:
    """SQLite metadata + filesystem fragment store rooted at ``root``."""

    def __init__(self, root: str | os.PathLike[str]) -> None:
        self.root = Path(root)
        self.tmp_dir = self.root / "tmp"
        self.published_dir = self.root / "published"
        for directory in (self.root, self.tmp_dir, self.published_dir):
            directory.mkdir(parents=True, exist_ok=True)
        self.db_path = self.root / "archive.sqlite3"
        self._lock_path = self.root / ".intake.lock"
        self._write_lock = threading.RLock()
        self._init_db()
        self.sanitize_startup()

    # ------------------------------------------------------------------ locking
    @contextmanager
    def _critical(self) -> Iterator[None]:
        """Serialize mutation + publication within a process and across processes."""
        with self._write_lock:
            if fcntl is None:
                yield
                return
            fd = os.open(self._lock_path, os.O_CREAT | os.O_RDWR, 0o600)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX)
                yield
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
                os.close(fd)

    # ------------------------------------------------------------------ schema
    @contextmanager
    def _db(self) -> Iterator[sqlite3.Connection]:
        conn = self._connect()
        try:
            yield conn
        finally:
            conn.close()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        # busy_timeout first: journal-mode switching also needs the write lock.
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_db(self) -> None:
        with self._db() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS deliveries (
                    transfer_id  TEXT PRIMARY KEY,
                    frame_count  INTEGER NOT NULL,
                    size_bytes   INTEGER NOT NULL,
                    sha256       TEXT NOT NULL,
                    status       TEXT NOT NULL DEFAULT 'waiting',
                    pub_sha256   TEXT,
                    pub_size     INTEGER,
                    pub_path     TEXT,
                    pub_at       TEXT,
                    actual_sha256 TEXT,
                    pub_recovered TEXT,
                    created_at   TEXT NOT NULL,
                    updated_at   TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS frames (
                    transfer_id TEXT NOT NULL,
                    frame_no    INTEGER NOT NULL,
                    payload     BLOB NOT NULL,
                    sha256      TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    PRIMARY KEY (transfer_id, frame_no),
                    FOREIGN KEY (transfer_id) REFERENCES deliveries(transfer_id)
                );

                CREATE TABLE IF NOT EXISTS parities (
                    transfer_id TEXT PRIMARY KEY,
                    payload     BLOB NOT NULL,
                    sha256      TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    FOREIGN KEY (transfer_id) REFERENCES deliveries(transfer_id)
                );

                CREATE TABLE IF NOT EXISTS audit (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    transfer_id TEXT NOT NULL,
                    event       TEXT NOT NULL,
                    detail      TEXT NOT NULL,
                    at          TEXT NOT NULL
                );
                """
            )
        with self._db() as conn:
            self._migrate(conn)

    @staticmethod
    def _migrate(conn: sqlite3.Connection) -> None:
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(deliveries)")}
        if "actual_sha256" not in columns:
            conn.execute("ALTER TABLE deliveries ADD COLUMN actual_sha256 TEXT")
        if "pub_recovered" not in columns:
            conn.execute("ALTER TABLE deliveries ADD COLUMN pub_recovered TEXT")

    def sanitize_startup(self) -> None:
        """Clean up artifacts of an interrupted write, then resume safe work.

        Partial files are never referenced by a published row, so deleting them
        cannot damage a valid archive. A published row whose file vanished is left
        alone: downloads fail closed. Waiting deliveries that already have enough
        material are retried, which covers a crash between rename and commit.
        """
        partials = list(self.tmp_dir.glob("*.part"))
        if self.published_dir.is_dir():
            partials += list(self.published_dir.rglob("*.part"))
        with self._critical():
            for path in partials:
                try:
                    path.unlink()
                except OSError:
                    pass
        with self._db() as conn:
            rows = conn.execute(
                "SELECT transfer_id FROM deliveries WHERE status=?", (STATUS_WAITING,)
            ).fetchall()
        for row in rows:
            tid = row["transfer_id"]
            try:
                with self._critical():
                    if self.state(tid).can_attempt:
                        self._publish_locked(tid)
            except Exception:
                # Startup must not fail because one delivery cannot be assembled.
                continue

    # ------------------------------------------------------------------ audit
    def _audit(self, conn: sqlite3.Connection, transfer_id: str, event: str, detail: str = "") -> None:
        conn.execute(
            "INSERT INTO audit(transfer_id, event, detail, at) VALUES (?,?,?,?)",
            (transfer_id, event, detail, utcnow()),
        )

    def audit_log(self, transfer_id: str) -> list[sqlite3.Row]:
        with self._db() as conn:
            return conn.execute(
                "SELECT event, detail, at FROM audit WHERE transfer_id=? ORDER BY id",
                (normalize_transfer_id(transfer_id),),
            ).fetchall()

    # -------------------------------------------------------------- registration
    def register_delivery(
        self, transfer_id: str, frame_count: int | str, size_bytes: int | str, sha256: str
    ) -> Delivery:
        tid = normalize_transfer_id(transfer_id)
        count = parse_frame_count(frame_count)
        size = parse_size_bytes(size_bytes)
        digest = parse_sha256(sha256)
        validate_declared_size(count, size)
        now = utcnow()
        with self._critical(), self._db() as conn:
            row = conn.execute(
                "SELECT * FROM deliveries WHERE transfer_id=?", (tid,)
            ).fetchone()
            if row is not None:
                existing = _delivery_from_row(row)
                if (
                    existing.frame_count == count
                    and existing.size_bytes == size
                    and existing.sha256 == digest
                ):
                    return existing
                raise ConflictError(
                    f"传输号 {tid} 已登记且参数不同，拒绝覆盖（原登记：{existing.frame_count} 帧 / "
                    f"{existing.size_bytes} 字节 / {existing.sha256[:12]}…）"
                )
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    """
                    INSERT INTO deliveries(transfer_id, frame_count, size_bytes, sha256,
                                           status, created_at, updated_at)
                    VALUES (?,?,?,?,?,?,?)
                    """,
                    (tid, count, size, digest, STATUS_WAITING, now, now),
                )
                self._audit(conn, tid, "registered", f"{count} 帧 / {size} 字节 / {digest}")
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return Delivery(tid, count, size, digest, now)

    # --------------------------------------------------------------- frame ingest
    def ingest_frames(self, transfer_id: str, items: Sequence[tuple[int, bytes]]) -> dict:
        """Insert data frames atomically, rejecting conflicting payloads.

        The whole batch is rejected if any (frame number, payload) conflicts with a
        stored frame or with another row of the same batch.
        """
        tid = normalize_transfer_id(transfer_id)
        if not items:
            raise StorageError("CSV 中没有数据帧")
        accepted: list[int] = []
        duplicates: list[int] = []
        conflicts: list[int] = []
        batch: dict[int, bytes] = {}

        with self._critical():
            entry = self._require_delivery(tid)
            for frame_no, payload in items:
                self._validate_frame_no(entry, frame_no)
                if len(payload) != FRAME_BYTES:
                    raise StorageError(f"第 {frame_no} 帧载荷必须为 256 字节")
                if frame_no in batch:
                    if batch[frame_no] != payload:
                        conflicts.append(frame_no)
                    else:
                        duplicates.append(frame_no)
                    continue
                stored = self._get_frame(tid, frame_no)
                if stored is not None:
                    if stored != payload:
                        conflicts.append(frame_no)
                    else:
                        duplicates.append(frame_no)
                    continue
                batch[frame_no] = payload
                accepted.append(frame_no)

            if conflicts:
                raise ConflictError(
                    "同传输号同帧号已存在不同内容，拒收本批："
                    + "、".join(str(n) for n in sorted(set(conflicts)))
                )

            if accepted:
                now = utcnow()
                with self._db() as conn:
                    conn.execute("BEGIN IMMEDIATE")
                    try:
                        for frame_no in accepted:
                            payload = batch[frame_no]
                            conn.execute(
                                """
                                INSERT INTO frames(transfer_id, frame_no, payload, sha256, received_at)
                                VALUES (?,?,?,?,?)
                                """,
                                (
                                    tid,
                                    frame_no,
                                    payload,
                                    hashlib.sha256(payload).hexdigest(),
                                    now,
                                ),
                            )
                        conn.execute(
                            "UPDATE deliveries SET updated_at=? WHERE transfer_id=?", (now, tid)
                        )
                        detail = "接受帧 " + ",".join(str(n) for n in accepted)
                        if duplicates:
                            detail += "；重复帧 " + ",".join(str(n) for n in sorted(set(duplicates)))
                        self._audit(conn, tid, "frames", detail)
                        conn.execute("COMMIT")
                    except Exception:
                        conn.execute("ROLLBACK")
                        raise

            outcome = self._after_ingest_locked(tid)
        return {
            "transfer_id": tid,
            "accepted": sorted(accepted),
            "duplicates": sorted(set(duplicates)),
            "conflicts": [],
            "result": outcome,
        }

    def ingest_parity(self, transfer_id: str, payload: bytes) -> dict:
        tid = normalize_transfer_id(transfer_id)
        if len(payload) != FRAME_BYTES:
            raise StorageError("校验帧载荷必须为 256 字节")
        parity_status = "accepted"
        with self._critical():
            self._require_delivery(tid)
            stored = self._get_parity(tid)
            if stored is not None:
                if stored != payload:
                    raise ConflictError("同传输号已存在内容不同的校验帧，拒收")
                parity_status = "duplicate"
            else:
                now = utcnow()
                with self._db() as conn:
                    conn.execute("BEGIN IMMEDIATE")
                    try:
                        conn.execute(
                            """
                            INSERT INTO parities(transfer_id, payload, sha256, received_at)
                            VALUES (?,?,?,?)
                            """,
                            (tid, payload, hashlib.sha256(payload).hexdigest(), now),
                        )
                        conn.execute(
                            "UPDATE deliveries SET updated_at=? WHERE transfer_id=?", (now, tid)
                        )
                        self._audit(conn, tid, "parity", "校验帧已接收")
                        conn.execute("COMMIT")
                    except Exception:
                        conn.execute("ROLLBACK")
                        raise
            outcome = self._after_ingest_locked(tid)
        return {"transfer_id": tid, "parity": parity_status, "result": outcome}

    # ------------------------------------------------------------------- state
    def state(self, transfer_id: str) -> ArchiveState:
        tid = normalize_transfer_id(transfer_id)
        row = self._delivery_row(tid)
        entry = _delivery_from_row(row)
        frames = self._get_frames(tid)
        parity = self._get_parity(tid)
        missing = tuple(
            n for n in range(1, entry.frame_count + 1) if n not in frames
        )
        status = row["status"]
        recoverable = len(missing) == 1 and parity is not None
        return ArchiveState(
            delivery=entry,
            frames=frames,
            parity=parity,
            status=status,
            published_sha256=row["pub_sha256"],
            published_size=row["pub_size"],
            published_at=row["pub_at"],
            missing_frames=missing,
            parity_recoverable=recoverable,
            actual_sha256=row["actual_sha256"],
            recovered_frames=_parse_frame_list(row["pub_recovered"]),
            tentative_frame=(
                missing[0]
                if recoverable and status != STATUS_PUBLISHED
                else None
            ),
        )

    def verify_now(self, transfer_id: str) -> dict:
        """Re-attempt assembly/verification on demand (e.g. after a bad batch)."""
        tid = normalize_transfer_id(transfer_id)
        with self._critical():
            return self._after_ingest_locked(tid)

    def list_deliveries(self) -> list[ArchiveState]:
        with self._db() as conn:
            rows = conn.execute(
                "SELECT transfer_id FROM deliveries ORDER BY created_at DESC, transfer_id"
            ).fetchall()
        return [self.state(row["transfer_id"]) for row in rows]

    # -------------------------------------------------------------- publication
    def _after_ingest_locked(self, tid: str) -> dict:
        """Attempt publication. Caller must hold ``_critical``."""
        state = self.state(tid)
        if state.status == STATUS_PUBLISHED:
            return {
                "status": STATUS_PUBLISHED,
                "published": False,
                "reason": "already-published",
            }
        if state.can_attempt:
            return self._publish_locked(tid, state)
        return {
            "status": STATUS_WAITING,
            "published": False,
            "reason": "missing-frames",
            "missing_frames": list(state.missing_frames),
        }

    def _publish_locked(self, tid: str, state: ArchiveState | None = None) -> dict:
        """Assemble, truncate, verify, then atomically publish. Holds ``_critical``."""
        state = state or self.state(tid)
        entry = state.delivery
        frames = dict(state.frames)
        recovered: list[int] = []

        if state.missing_frames:
            # Exactly one missing frame and a parity frame: recover it in memory.
            # It is not persisted; the real frame may still arrive later.
            missing_no = state.missing_frames[0]
            assert state.parity is not None
            recovered_frame = self._recover(frames, state.parity, missing_no, entry.frame_count)
            if recovered_frame is None:  # pragma: no cover - guarded by state.can_attempt
                raise StorageError("校验帧不足以恢复缺失的数据帧")
            frames[missing_no] = recovered_frame
            recovered.append(missing_no)

        padded = b"".join(frames[n] for n in range(1, entry.frame_count + 1))
        body = padded[: entry.size_bytes]
        digest = hashlib.sha256(body).hexdigest()

        if digest != entry.sha256:
            now = utcnow()
            with self._db() as conn:
                conn.execute("BEGIN IMMEDIATE")
                try:
                    previous = conn.execute(
                        "SELECT status, actual_sha256 FROM deliveries WHERE transfer_id=?",
                        (tid,),
                    ).fetchone()
                    conn.execute(
                        """
                        UPDATE deliveries
                           SET status=?, actual_sha256=?, updated_at=?
                         WHERE transfer_id=? AND status!=?
                        """,
                        (STATUS_MISMATCH, digest, now, tid, STATUS_PUBLISHED),
                    )
                    # Record each distinct mismatch once; repeated verify clicks
                    # with the same material must not flood the audit log.
                    if previous is None or previous["actual_sha256"] != digest:
                        self._audit(
                            conn,
                            tid,
                            "digest-mismatch",
                            f"声明 {entry.sha256}，实际 {digest}；保留分片待补帧重验"
                            + (
                                f"（第 {recovered[0]} 帧为校验帧临时恢复，未落库）"
                                if recovered
                                else ""
                            ),
                        )
                    conn.execute("COMMIT")
                except Exception:
                    conn.execute("ROLLBACK")
                    raise
            return {
                "status": STATUS_MISMATCH,
                "published": False,
                "reason": "digest-mismatch",
                "expected_sha256": entry.sha256,
                "actual_sha256": digest,
                "recovered_frames": recovered,
            }

        # Write into tmp, fsync, move to a same-filesystem staging name, then
        # atomically rename into the content-addressed final path.
        folder = self.published_dir / tid
        folder.mkdir(parents=True, exist_ok=True)
        final_name = f"{tid}-{digest}.bin"
        final_path = folder / final_name
        tmp_path = self.tmp_dir / f"{uuid.uuid4().hex}.part"
        # Deterministic staging name: after a crash the file is still recognisable
        # as the content-addressed final file minus its rename step.
        staging_path = folder / f".{final_name}.part"

        try:
            # Never trust a leftover staging file from a crashed writer; rewrite it.
            with open(tmp_path, "wb") as fh:
                fh.write(body)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_path, staging_path)
            with open(staging_path, "rb+") as fh:
                os.fsync(fh.fileno())
            os.replace(staging_path, final_path)
            _fsync_dir(folder)
            _fsync_dir(self.tmp_dir)
        except BaseException:
            for path in (tmp_path, staging_path):
                try:
                    path.unlink()
                except OSError:
                    pass
            raise

        now = utcnow()
        published_here = False
        with self._db() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT status FROM deliveries WHERE transfer_id=?", (tid,)
                ).fetchone()
                if row is None:
                    conn.execute("ROLLBACK")
                    raise NotFoundError(f"传输号 {tid} 已不存在")
                if row["status"] != STATUS_PUBLISHED:
                    conn.execute(
                        """
                        UPDATE deliveries
                           SET status=?, pub_sha256=?, pub_size=?, pub_path=?,
                               pub_at=?, actual_sha256=?, pub_recovered=?, updated_at=?
                         WHERE transfer_id=?
                        """,
                        (
                            STATUS_PUBLISHED,
                            digest,
                            entry.size_bytes,
                            str(final_path.relative_to(self.root)),
                            now,
                            digest,
                            ",".join(str(n) for n in recovered) or None,
                            now,
                            tid,
                        ),
                    )
                    self._audit(
                        conn,
                        tid,
                        "published",
                        f"{entry.size_bytes} 字节 / {digest}"
                        + (f"；第 {recovered[0]} 帧由校验帧恢复" if recovered else ""),
                    )
                    published_here = True
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return {
            "status": STATUS_PUBLISHED,
            "published": published_here,
            "sha256": digest,
            "size_bytes": entry.size_bytes,
            "recovered_frames": recovered,
        }

    def published_path(self, transfer_id: str) -> tuple[Path, str]:
        """Return (path, digest) for a published archive, verifying before download."""
        tid = normalize_transfer_id(transfer_id)
        row = self._delivery_row(tid)
        if row["status"] != STATUS_PUBLISHED or not row["pub_path"]:
            raise NotFoundError("该传输号尚未发布可下载档案")
        path = self.root / row["pub_path"]
        if not path.is_file():
            raise NotFoundError("已发布档案文件缺失，拒绝下载")
        if path.stat().st_size != int(row["pub_size"]):
            raise ConflictError("已发布档案大小与登记不符，拒绝下载")
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != row["pub_sha256"]:
            raise ConflictError("已发布档案摘要与登记不符，拒绝下载")
        return path, digest

    # ---------------------------------------------------------------- helpers
    def _require_delivery(self, tid: str) -> Delivery:
        return _delivery_from_row(self._delivery_row(tid))

    def _delivery_row(self, tid: str) -> sqlite3.Row:
        with self._db() as conn:
            row = conn.execute(
                "SELECT * FROM deliveries WHERE transfer_id=?", (tid,)
            ).fetchone()
        if row is None:
            raise NotFoundError(f"传输号 {tid} 未登记")
        return row

    def _validate_frame_no(self, entry: Delivery, frame_no: int) -> None:
        if not 1 <= frame_no <= entry.frame_count:
            raise StorageError(f"帧号必须在 1～{entry.frame_count} 之间")

    def _get_frame(self, tid: str, frame_no: int) -> bytes | None:
        with self._db() as conn:
            row = conn.execute(
                "SELECT payload, sha256 FROM frames WHERE transfer_id=? AND frame_no=?",
                (tid, frame_no),
            ).fetchone()
        if row is None:
            return None
        payload = bytes(row["payload"])
        if hashlib.sha256(payload).hexdigest() != row["sha256"]:
            raise ConflictError(f"第 {frame_no} 帧存储内容校验失败，请重新上传")
        return payload

    def _get_frames(self, tid: str) -> dict[int, bytes]:
        with self._db() as conn:
            rows = conn.execute(
                "SELECT frame_no, payload, sha256 FROM frames WHERE transfer_id=?", (tid,)
            ).fetchall()
        frames: dict[int, bytes] = {}
        for row in rows:
            payload = bytes(row["payload"])
            if hashlib.sha256(payload).hexdigest() != row["sha256"]:
                raise ConflictError(f"第 {row['frame_no']} 帧存储内容校验失败，请重新上传")
            frames[int(row["frame_no"])] = payload
        return frames

    def _get_parity(self, tid: str) -> bytes | None:
        with self._db() as conn:
            row = conn.execute(
                "SELECT payload, sha256 FROM parities WHERE transfer_id=?", (tid,)
            ).fetchone()
        if row is None:
            return None
        payload = bytes(row["payload"])
        if hashlib.sha256(payload).hexdigest() != row["sha256"]:
            raise ConflictError("校验帧存储内容校验失败，请重新上传")
        return payload

    @staticmethod
    def _recover(
        frames: dict[int, bytes], parity: bytes, missing_no: int, frame_count: int
    ) -> bytes | None:
        """Recover one missing frame as parity XOR every other *padded* frame."""
        others = [
            frames[n] for n in range(1, frame_count + 1) if n != missing_no and n in frames
        ]
        if len(others) != frame_count - 1:
            return None
        return xor_bytes([parity, *others])


def _parse_frame_list(value: str | None) -> tuple[int, ...]:
    if not value:
        return ()
    return tuple(int(part) for part in value.split(",") if part)


def _delivery_from_row(row: sqlite3.Row) -> Delivery:
    return Delivery(
        transfer_id=row["transfer_id"],
        frame_count=int(row["frame_count"]),
        size_bytes=int(row["size_bytes"]),
        sha256=row["sha256"],
        created_at=row["created_at"],
    )


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)
