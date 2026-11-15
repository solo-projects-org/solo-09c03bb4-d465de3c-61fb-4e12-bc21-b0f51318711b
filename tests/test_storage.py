import hashlib
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.csvintake import parse_frames_csv  # noqa: E402
from app.storage import (  # noqa: E402
    FRAME_BYTES,
    STATUS_MISMATCH,
    STATUS_PUBLISHED,
    STATUS_WAITING,
    ConflictError,
    Storage,
    StorageError,
    xor_bytes,
)


def make_archive(size: int, seed: int = 1) -> bytes:
    return bytes((i * 37 + seed * 11) % 256 for i in range(size))


def pad_frames(archive: bytes, count: int) -> list[bytes]:
    padded = archive + b"\x00" * (count * FRAME_BYTES - len(archive))
    return [padded[i * FRAME_BYTES : (i + 1) * FRAME_BYTES] for i in range(count)]


def setup_delivery(root: Path, *, count=3, size=None, frames=None, digest=None, tid="T-1"):
    archive = make_archive(size or (count * FRAME_BYTES - 100))
    frames = frames or pad_frames(archive, count)
    parity = xor_bytes(frames)
    digest = digest or hashlib.sha256(archive).hexdigest()
    store = Storage(root)
    store.register_delivery(tid, count, len(archive), digest)
    return store, archive, frames, parity, digest


@pytest.fixture
def root(tmp_path: Path) -> Path:
    return tmp_path / "data"


# ------------------------------------------------------------------ pure helpers
def test_parse_frames_csv_headers_and_parity():
    payload = bytes(range(256)).hex()
    text = f"transfer_id,frame_no,payload_hex\nS-1,1,{payload}\nS-1,P,{payload}\n"
    frames, parity = parse_frames_csv(text, "S-1")
    assert frames == [(1, bytes(range(256)))]
    assert parity == bytes(range(256))


def test_parse_frames_csv_two_columns_and_comments():
    payload = b"x" * 256
    text = f"# comment\nframe_no,payload\n1,{payload.hex()}\n\n4,{payload.hex()}\n"
    frames, parity = parse_frames_csv(text, "S-2")
    assert [n for n, _ in frames] == [1, 4]
    assert parity is None


def test_parse_frames_csv_rejects_transfer_mismatch():
    with pytest.raises(StorageError, match="不一致"):
        parse_frames_csv(f"OTHER,1,{'00' * 256}\n", "MINE")


def test_parse_frames_csv_rejects_short_and_nonhex_payloads():
    with pytest.raises(StorageError, match="512"):
        parse_frames_csv("1,00\n", "S")
    with pytest.raises(StorageError, match="非十六进制"):
        parse_frames_csv(f"1,{'zz' * 256}\n", "S")


# ------------------------------------------------------------------ registration
def test_register_validates_frame_count_and_size(root):
    store = Storage(root)
    with pytest.raises(StorageError):
        store.register_delivery("T", 1, 256, "00" * 32)  # frame count too low
    with pytest.raises(StorageError):
        store.register_delivery("T", 17, 4096, "00" * 32)  # too many frames
    with pytest.raises(StorageError, match="相容"):
        store.register_delivery("T", 4, 256, "00" * 32)  # size implies 1 frame
    store.register_delivery("OK", 4, 900, "00" * 32)
    assert store.state("OK").delivery.frame_count == 4


def test_reregister_same_params_is_idempotent_and_changes_conflict(root):
    store, _, _, _, digest = setup_delivery(root, count=3, tid="IDEMP")
    store.register_delivery("IDEMP", 3, len(make_archive(3 * FRAME_BYTES - 100)), digest)
    with pytest.raises(ConflictError):
        store.register_delivery("IDEMP", 4, 900, digest)


# ------------------------------------------------------------------ happy path
def test_complete_delivery_publishes_exact_truncated_archive(root):
    store, archive, frames, _, digest = setup_delivery(root, count=4, tid="FULL")
    for i, frame in enumerate(frames, start=1):
        store.ingest_frames("FULL", [(i, frame)])
    state = store.state("FULL")
    assert state.status == STATUS_PUBLISHED
    path, published_digest = store.published_path("FULL")
    assert path.read_bytes() == archive
    assert published_digest == digest
    assert len(archive) == state.published_size
    # No temporary or staging files survive publication.
    assert list(store.tmp_dir.glob("*")) == []


def test_two_missing_frames_wait_and_publish_after_retransmission(root):
    store, archive, frames, _, digest = setup_delivery(root, count=5, tid="LATE")
    for i in (1, 3, 5):
        store.ingest_frames("LATE", [(i, frames[i - 1])])
    state = store.state("LATE")
    assert state.status == STATUS_WAITING
    assert state.missing_frames == (2, 4)
    with pytest.raises(Exception):
        store.published_path("LATE")
    store.ingest_frames("LATE", [(4, frames[3])])
    assert store.state("LATE").missing_frames == (2,)
    store.ingest_frames("LATE", [(2, frames[1])])
    assert store.state("LATE").status == STATUS_PUBLISHED
    assert store.published_path("LATE")[0].read_bytes() == archive


# ------------------------------------------------------------------ parity recovery
def test_single_missing_frame_recovered_from_parity(root):
    store, archive, frames, parity, digest = setup_delivery(root, count=5, tid="PAR")
    for i in (1, 2, 4, 5):
        store.ingest_frames("PAR", [(i, frames[i - 1])])
    store.ingest_parity("PAR", parity)
    state = store.state("PAR")
    assert state.status == STATUS_PUBLISHED
    assert state.recovered_frames == (3,)
    path, _ = store.published_path("PAR")
    assert path.read_bytes() == archive

    # Recovery is not persisted as a received frame: the genuine frame can still
    # arrive and is accepted even if it differs from an earlier bad recovery.
    store.ingest_frames("PAR", [(3, frames[2])])
    assert store.state("PAR").received_frames == (1, 2, 3, 4, 5)
    assert store.published_path("PAR")[0].read_bytes() == archive


def test_parity_with_two_missing_frames_does_not_publish(root):
    store, _, frames, parity, _ = setup_delivery(root, count=6, tid="PAR2")
    for i in (1, 3, 4, 6):
        store.ingest_frames("PAR2", [(i, frames[i - 1])])
    store.ingest_parity("PAR2", parity)
    state = store.state("PAR2")
    assert state.status == STATUS_WAITING
    assert state.missing_frames == (2, 5)
    with pytest.raises(Exception):
        store.published_path("PAR2")
    # Supplying one real frame leaves exactly one missing frame; parity recovers it.
    store.ingest_frames("PAR2", [(2, frames[1])])
    assert store.state("PAR2").status == STATUS_PUBLISHED


def test_parity_decode_is_xor_of_padded_frames(root):
    store, archive, frames, parity, _ = setup_delivery(root, count=2, tid="PAD")
    assert len(archive) < 2 * FRAME_BYTES  # last frame is zero-padded
    store.ingest_frames("PAD", [(1, frames[0])])
    store.ingest_parity("PAD", parity)
    assert store.state("PAD").status == STATUS_PUBLISHED
    assert store.published_path("PAD")[0].read_bytes() == archive


def test_conflicting_parity_is_rejected(root):
    store, _, _, parity, _ = setup_delivery(root, count=3, tid="PC")
    store.ingest_parity("PC", parity)
    with pytest.raises(ConflictError):
        store.ingest_parity("PC", bytes(256))


# ------------------------------------------------------------------ conflicts
def test_same_frame_number_different_content_is_rejected(root):
    store, _, frames, _, _ = setup_delivery(root, count=3, tid="CONF")
    store.ingest_frames("CONF", [(1, frames[0])])
    with pytest.raises(ConflictError, match="1"):
        store.ingest_frames("CONF", [(1, bytes(256))])
    # Rejected batch must not partially apply.
    with pytest.raises(ConflictError):
        store.ingest_frames("CONF", [(2, frames[1]), (1, bytes(256))])
    assert store.state("CONF").received_frames == (1,)


def test_identical_retransmission_is_idempotent(root):
    store, _, frames, _, _ = setup_delivery(root, count=3, tid="REPEAT")
    first = store.ingest_frames("REPEAT", [(1, frames[0])])
    second = store.ingest_frames("REPEAT", [(1, frames[0]), (2, frames[1])])
    assert first["accepted"] == [1]
    assert second["accepted"] == [2]
    assert second["duplicates"] == [1]
    assert store.state("REPEAT").received_frames == (1, 2)


def test_same_batch_conflicting_duplicates_rejected(root):
    store, _, frames, _, _ = setup_delivery(root, count=3, tid="BATCH")
    with pytest.raises(ConflictError):
        store.ingest_frames("BATCH", [(1, frames[0]), (1, bytes(256))])
    assert store.state("BATCH").received_frames == ()


def test_frames_outside_declared_count_rejected(root):
    store, _, frames, _, _ = setup_delivery(root, count=3, tid="RANGE")
    with pytest.raises(StorageError):
        store.ingest_frames("RANGE", [(4, frames[0])])
    with pytest.raises(StorageError):
        store.ingest_frames("RANGE", [(0, frames[0])])


# ------------------------------------------------------------------ digest mismatch
def test_digest_mismatch_retains_fragments_and_blocks_download(root):
    bad = "0" * 64
    store, archive, frames, _, _ = setup_delivery(root, count=3, tid="BAD", digest=bad)
    store.ingest_frames("BAD", [(i, f) for i, f in enumerate(frames, 1)])
    state = store.state("BAD")
    assert state.status == STATUS_MISMATCH
    assert state.received_frames == (1, 2, 3)  # fragments retained
    assert state.actual_sha256 == hashlib.sha256(archive).hexdigest()
    with pytest.raises(Exception):
        store.published_path("BAD")
    assert not (store.published_dir / "BAD").exists()


def test_mismatch_fragments_are_retained_and_conflicts_still_rejected(root):
    store, _, frames, _, _ = setup_delivery(root, count=3, tid="FIX", digest="0" * 64)
    store.ingest_frames("FIX", [(1, frames[0])])
    assert store.state("FIX").received_frames == (1,)
    with pytest.raises(ConflictError):
        store.ingest_frames("FIX", [(1, bytes(256))])
    # The retained fragment is still intact after the rejected overwrite attempt.
    store.ingest_frames("FIX", [(1, frames[0])])
    assert store.state("FIX").received_frames == (1,)
    with pytest.raises(ConflictError, match="已登记且参数不同"):
        store.register_delivery("FIX", 3, 3 * FRAME_BYTES, "00" * 32)


# ------------------------------------------------------------------ crash safety
def test_interrupted_write_never_leaves_downloadable_partial(tmp_path):
    store, archive, frames, _, _ = setup_delivery(tmp_path / "d", count=3, tid="CRASH")
    # Simulate a crash mid-publication: a .part file exists, DB row not updated.
    store.tmp_dir.mkdir(parents=True, exist_ok=True)
    (store.tmp_dir / "deadbeef.part").write_bytes(b"partial garbage")
    state = store.state("CRASH")
    assert state.status == STATUS_WAITING
    with pytest.raises(Exception):
        store.published_path("CRASH")
    # Restart: startup cleanup removes partials; pending delivery stays pending.
    restarted = Storage(tmp_path / "d")
    assert not list((tmp_path / "d" / "tmp").glob("*.part"))
    with pytest.raises(Exception):
        restarted.published_path("CRASH")
    # Completing after restart publishes cleanly.
    restarted.ingest_frames("CRASH", [(i, f) for i, f in enumerate(frames, 1)])
    assert restarted.published_path("CRASH")[0].read_bytes() == archive


def test_startup_republishes_committed_but_unfinished_assembly(tmp_path):
    root = tmp_path / "d2"
    store, archive, frames, _, _ = setup_delivery(root, count=2, tid="RESUME")
    store.ingest_frames("RESUME", [(1, frames[0])])
    assert store.state("RESUME").missing_frames == (2,)
    # Simulate: all fragments present in DB but process died before publishing.
    store.ingest_frames("RESUME", [(2, frames[1])])
    import shutil

    shutil.rmtree(store.published_dir / "RESUME")  # published file lost
    with store._db() as conn:
        conn.execute(
            "UPDATE deliveries SET status=?, pub_path=NULL, pub_sha256=NULL WHERE transfer_id=?",
            ("waiting", "RESUME"),
        )
    restarted = Storage(root)  # startup scan retries pending, complete deliveries
    assert restarted.state("RESUME").status == STATUS_PUBLISHED
    assert restarted.published_path("RESUME")[0].read_bytes() == archive


def test_truncated_published_file_is_not_served(tmp_path):
    store, archive, frames, _, _ = setup_delivery(tmp_path / "d3", count=2, tid="TRUNC")
    store.ingest_frames("TRUNC", [(1, frames[0]), (2, frames[1])])
    path, _ = store.published_path("TRUNC")
    path.write_bytes(b"chopped")
    with pytest.raises(Exception, match="大小|摘要|不符"):
        store.published_path("TRUNC")


def test_published_archive_is_stable_after_late_fragments(tmp_path):
    root = tmp_path / "d4"
    store, archive, frames, parity, digest = setup_delivery(root, count=4, tid="STABLE")
    store.ingest_frames("STABLE", [(1, frames[0]), (2, frames[1]), (4, frames[3])])
    store.ingest_parity("STABLE", parity)
    published = store.published_path("STABLE")[0]
    first_bytes = published.read_bytes()
    # Late genuine frame and duplicate frames arrive after publication.
    store.ingest_frames("STABLE", [(3, frames[2]), (1, frames[0])])
    assert published.read_bytes() == first_bytes
    assert store.published_path("STABLE")[1] == digest
    assert store.state("STABLE").status == STATUS_PUBLISHED
    assert store.state("STABLE").published_sha256 == digest


# ------------------------------------------------------------------ concurrency
def test_concurrent_identical_batches_publish_exactly_one_archive(tmp_path):
    import threading

    root = tmp_path / "conc"
    store, archive, frames, parity, digest = setup_delivery(root, count=6, tid="RACE")
    # Register twice concurrently and upload everything from many threads.
    barrier = threading.Barrier(8)
    errors: list[BaseException] = []

    def worker(worker_id: int) -> None:
        local = Storage(root)
        try:
            barrier.wait(timeout=10)
            local.ingest_frames("RACE", [(i, frames[i - 1]) for i in range(1, 7)])
            local.ingest_parity("RACE", parity)
        except BaseException as exc:  # pragma: no cover - failure branch
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
    assert not errors
    state = store.state("RACE")
    assert state.status == STATUS_PUBLISHED
    files = list((store.published_dir / "RACE").iterdir())
    assert len(files) == 1
    assert files[0].read_bytes() == archive
    assert store.published_path("RACE")[1] == digest


def test_concurrent_conflicting_frames_accept_only_one(tmp_path):
    import threading

    root = tmp_path / "confrace"
    store, _, frames, _, _ = setup_delivery(root, count=3, tid="C-RACE")
    alt = bytes(256)
    barrier = threading.Barrier(2)
    outcomes: list[str] = []

    def worker(payload: bytes) -> None:
        local = Storage(root)
        barrier.wait(timeout=10)
        try:
            local.ingest_frames("C-RACE", [(1, payload)])
            outcomes.append("accepted")
        except ConflictError:
            outcomes.append("conflict")

    threads = [threading.Thread(target=worker, args=(p,)) for p in (frames[0], alt)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
    assert sorted(outcomes) == ["accepted", "conflict"]
    assert store.state("C-RACE").received_frames == (1,)


def test_repeated_verify_does_not_flood_audit_log(root):
    store, _, frames, _, _ = setup_delivery(root, count=3, tid="AUDIT", digest="0" * 64)
    store.ingest_frames("AUDIT", [(i, f) for i, f in enumerate(frames, 1)])
    for _ in range(10):
        store.verify_now("AUDIT")
    events = [row["event"] for row in store.audit_log("AUDIT")]
    assert events.count("digest-mismatch") == 1


def test_parse_frames_csv_crlf_bom_and_extra_columns():
    payload = "ab" * 256
    text = "﻿transfer_id,frame_no,payload_hex,note\r\nS-9,1,%s,ok\r\n" % payload
    frames, parity = parse_frames_csv(text.encode("utf-8"), "S-9")
    assert frames == [(1, bytes.fromhex(payload))]
    assert parity is None
