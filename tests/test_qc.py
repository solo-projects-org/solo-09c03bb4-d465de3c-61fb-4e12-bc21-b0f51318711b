"""Quality-interpretation (质量判读) tests.

Covers the sample scanner thresholds, the run lifecycle (pending → passed /
quarantined / pending_review / interrupted), download gating, restart
persistence, and run-number fencing between two interleaving instances.
"""

import hashlib
import struct
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.main import create_app  # noqa: E402
from app.qc import (  # noqa: E402
    CLIP_ABS,
    CLIP_RUN,
    SILENCE_RUN,
    decode_s16le,
    scan_samples,
)
from app.storage import (  # noqa: E402
    FRAME_BYTES,
    STATUS_PUBLISHED,
    ConflictError,
    NotFoundError,
    Storage,
    StorageError,
)


# ------------------------------------------------------------------ builders
def clean_samples(n: int = 500) -> list[int]:
    return [7] * n


def anomaly_samples(n: int = 500) -> list[int]:
    """Silence run at samples 120..219, clipping run at samples 400..419."""
    samples = [7] * n
    for i in range(120, 220):
        samples[i] = 0
    for i in range(400, 420):
        samples[i] = 32767
    return samples


def pack_archive(archive: bytes) -> tuple[list[bytes], int]:
    count = max(2, (len(archive) + FRAME_BYTES - 1) // FRAME_BYTES)
    padded = archive + b"\x00" * (count * FRAME_BYTES - len(archive))
    frames = [padded[i * FRAME_BYTES : (i + 1) * FRAME_BYTES] for i in range(count)]
    return frames, count


def setup_published_bytes(root: Path, archive: bytes, tid: str, digest: str | None = None):
    frames, count = pack_archive(archive)
    store = Storage(root)
    store.register_delivery(tid, count, len(archive), digest or hashlib.sha256(archive).hexdigest())
    store.ingest_frames(tid, [(i, f) for i, f in enumerate(frames, 1)])
    assert store.state(tid).status == STATUS_PUBLISHED
    return store, archive


def setup_published(root: Path, samples: list[int], tid: str):
    archive = struct.pack(f"<{len(samples)}h", *samples)
    return setup_published_bytes(root, archive, tid)


def interpret(store: Storage, tid: str):
    return store.interpret(tid, confirm=True, sample_rate=100, sample_format="s16le")


# ------------------------------------------------------------- pure scanner
def test_decode_s16le_little_endian_signed():
    assert decode_s16le(b"\x01\x00\xff\xff\x00\x80") == (1, -1, -32768)


def test_scan_silence_threshold_exact():
    assert scan_samples([0] * (SILENCE_RUN - 1)) == (None, None)
    assert scan_samples([0] * SILENCE_RUN) == (0, None)
    assert scan_samples([5] * 10 + [0] * SILENCE_RUN + [5] * 10) == (10, None)


def test_scan_clip_threshold_exact_and_signed():
    assert scan_samples([32767] * (CLIP_RUN - 1)) == (None, None)
    assert scan_samples([32767] * CLIP_RUN) == (None, 0)
    assert scan_samples([CLIP_ABS] * CLIP_RUN) == (None, 0)  # 32760 counts
    assert scan_samples([CLIP_ABS - 1] * CLIP_RUN) == (None, None)  # 32759 does not
    assert scan_samples([-CLIP_ABS] * CLIP_RUN) == (None, 0)
    assert scan_samples([-32768] * CLIP_RUN) == (None, 0)  # full-scale negative counts


def test_scan_records_first_violating_run_only():
    samples = [0] * SILENCE_RUN + [1] * 50 + [0] * (SILENCE_RUN + 50)
    assert scan_samples(samples) == (0, None)
    samples = [7] * 120 + [0] * 100 + [7] * 180 + [32767] * 20 + [7] * 80
    assert scan_samples(samples) == (120, 400)


# --------------------------------------------------------------- run lifecycle
def test_uninterpreted_archive_downloads_as_usual(tmp_path):
    store, archive = setup_published(tmp_path / "d", clean_samples(), "PLAIN")
    assert store.qc_status("PLAIN").status == "none"
    assert store.qc_status("PLAIN").download_allowed
    assert store.published_path("PLAIN")[0].read_bytes() == archive


def test_clean_archive_passes_and_download_stays_open(tmp_path):
    store, archive = setup_published(tmp_path / "d", clean_samples(), "PASS")
    run = interpret(store, "PASS")
    assert run.status == "passed"
    assert run.sample_count == 500
    assert run.silence_index is None and run.clip_index is None
    assert run.digest == hashlib.sha256(archive).hexdigest()
    assert store.qc_status("PASS").status == "passed"
    assert store.published_path("PASS")[0].read_bytes() == archive


def test_anomalies_quarantine_with_first_sample_indices(tmp_path):
    store, _ = setup_published(tmp_path / "d", anomaly_samples(), "ANOM")
    run = interpret(store, "ANOM")
    assert run.status == "quarantined"
    assert run.silence_index == 120
    assert run.clip_index == 400
    assert "#120" in run.reason and "#400" in run.reason
    assert store.qc_status("ANOM").status == "quarantined"
    with pytest.raises(ConflictError, match="隔离"):
        store.published_path("ANOM")


def test_quarantine_survives_restart_with_reason(tmp_path):
    root = tmp_path / "d"
    store, _ = setup_published(root, anomaly_samples(), "Q")
    interpret(store, "Q")
    restarted = Storage(root)
    qc = restarted.qc_status("Q")
    assert qc.status == "quarantined"
    assert "#120" in qc.latest.reason and "#400" in qc.latest.reason
    with pytest.raises(ConflictError, match="隔离"):
        restarted.published_path("Q")
    history = restarted.qc_history("Q")
    assert len(history) == 1 and history[0].status == "quarantined"


def test_download_pauses_the_moment_a_run_starts(tmp_path):
    store, archive = setup_published(tmp_path / "d", clean_samples(), "GATE")
    first = interpret(store, "GATE")
    assert first.status == "passed"
    assert store.published_path("GATE")  # passed: downloads open
    run = store.begin_qc_run("GATE", 100, "s16le")
    assert store.qc_status("GATE").status == "pending"
    with pytest.raises(ConflictError, match="待复核"):
        store.published_path("GATE")
    done = store.finish_qc_run("GATE", run.run_no)
    assert done.status == "passed"
    assert store.published_path("GATE")[0].read_bytes() == archive


def test_interrupted_run_is_swept_at_startup_and_stays_pending(tmp_path):
    root = tmp_path / "d"
    store, archive = setup_published(root, clean_samples(), "INT")
    run = store.begin_qc_run("INT", 100, "s16le")
    # Process "dies" before finishing; the restart sweeps the stale running row.
    restarted = Storage(root)
    qc = restarted.qc_status("INT")
    assert qc.status == "pending"
    assert qc.latest.status == "interrupted"
    assert "中断" in qc.latest.reason
    with pytest.raises(ConflictError, match="待复核"):
        restarted.published_path("INT")
    events = [row["event"] for row in restarted.audit_log("INT")]
    assert "qc-started" in events and "qc-interrupted" in events
    # Only a newer passing run restores downloads.
    done = interpret(restarted, "INT")
    assert done.status == "passed" and done.run_no == run.run_no + 1
    assert restarted.published_path("INT")[0].read_bytes() == archive


def test_digest_abnormality_concludes_pending_review(tmp_path):
    root = tmp_path / "d"
    store, archive = setup_published(root, clean_samples(), "DIG")
    pub = store.published_path("DIG")[0]
    pub.write_bytes(b"\xff" * len(archive))  # same length, wrong digest
    run = interpret(store, "DIG")
    assert run.status == "pending_review"
    assert "摘要异常" in run.reason
    assert store.qc_status("DIG").status == "pending"
    with pytest.raises(ConflictError):
        store.published_path("DIG")


def test_missing_file_concludes_pending_review(tmp_path):
    root = tmp_path / "d"
    store, _ = setup_published(root, clean_samples(), "LOST")
    store.published_path("LOST")[0].unlink()
    run = interpret(store, "LOST")
    assert run.status == "pending_review"
    assert "缺失" in run.reason
    assert store.qc_status("LOST").status == "pending"


def test_odd_byte_count_is_format_abnormal(tmp_path):
    archive = struct.pack("<499h", *([7] * 499)) + b"\x00"  # 999 bytes: odd
    store, _ = setup_published_bytes(tmp_path / "d", archive, "ODD")
    run = interpret(store, "ODD")
    assert run.status == "pending_review"
    assert "格式异常" in run.reason
    with pytest.raises(ConflictError, match="待复核"):
        store.published_path("ODD")


def test_interpret_requires_confirmation_and_supported_format(tmp_path):
    store, _ = setup_published(tmp_path / "d", clean_samples(), "VAL")
    with pytest.raises(StorageError, match="确认"):
        store.interpret("VAL", confirm=False, sample_rate=100, sample_format="s16le")
    with pytest.raises(StorageError, match="100"):
        store.interpret("VAL", confirm=True, sample_rate=50, sample_format="s16le")
    with pytest.raises(StorageError, match="16"):
        store.interpret("VAL", confirm=True, sample_rate=100, sample_format="f32le")
    assert store.qc_status("VAL").status == "none"  # rejected launches leave no run


def test_interpret_requires_published_archive(tmp_path):
    store = Storage(tmp_path / "d")
    store.register_delivery("NP", 2, 300, "0" * 64)
    with pytest.raises(ConflictError, match="发布"):
        interpret(store, "NP")
    with pytest.raises(NotFoundError):
        interpret(store, "NOPE")


def test_runs_are_numbered_monotonically(tmp_path):
    store, _ = setup_published(tmp_path / "d", clean_samples(), "SEQ")
    r1 = interpret(store, "SEQ")
    r2 = interpret(store, "SEQ")
    assert (r1.run_no, r2.run_no) == (1, 2)
    assert [r.run_no for r in store.qc_history("SEQ")] == [2, 1]
    events = [row["event"] for row in store.audit_log("SEQ")]
    assert events.count("qc-started") == 2 and events.count("qc-passed") == 2


def test_ingest_and_publish_unaffected_by_quarantine(tmp_path):
    root = tmp_path / "d"
    store, _ = setup_published(root, anomaly_samples(), "BAD")
    interpret(store, "BAD")
    assert store.qc_status("BAD").status == "quarantined"
    # Another delivery still ingests frames and publishes normally.
    store2, archive2 = setup_published(root, clean_samples(), "GOOD")
    assert store2.state("GOOD").status == STATUS_PUBLISHED
    assert store2.published_path("GOOD")[0].read_bytes() == archive2


# ------------------------------------------------------- interleaved instances
def test_stale_run_cannot_overwrite_newer_conclusion(tmp_path):
    """Two instances interleave runs; the older run's late result must not win."""
    root = tmp_path / "fencing"
    store_a, archive = setup_published(root, clean_samples(), "FENCE")
    store_b = Storage(root)  # second "web instance" on the same data dir

    run1 = store_a.begin_qc_run("FENCE", 100, "s16le")
    run2 = store_b.begin_qc_run("FENCE", 100, "s16le")
    assert (run1.run_no, run2.run_no) == (1, 2)

    # The newer run finishes first and passes.
    done2 = store_b.finish_qc_run("FENCE", run2.run_no)
    assert done2.status == "passed"
    assert store_a.qc_status("FENCE").status == "passed"

    # The older run finishes later, against a briefly corrupted file, and
    # concludes pending_review. Its conclusion is recorded as history only.
    pub = store_a.published_path("FENCE")[0]
    original = pub.read_bytes()
    pub.write_bytes(b"\x00" * len(original))
    done1 = store_a.finish_qc_run("FENCE", run1.run_no)
    assert done1.status == "pending_review"
    pub.write_bytes(original)  # repair the original bytes

    # Effective state still comes from the newest run: downloads stay open.
    assert store_a.qc_status("FENCE").status == "passed"
    assert store_a.published_path("FENCE")[0].read_bytes() == original
    history = store_a.qc_history("FENCE")
    assert [r.run_no for r in history] == [2, 1]
    assert history[0].status == "passed"
    assert history[1].status == "pending_review"


def test_stale_pending_review_cannot_lift_quarantine(tmp_path):
    """The mirror interleaving: an old 'softer' result must not lift a newer
    quarantine either."""
    root = tmp_path / "fencing2"
    store_a, _ = setup_published(root, anomaly_samples(), "FENCE2")
    store_b = Storage(root)

    pub = store_a.published_path("FENCE2")[0]  # located before any run starts
    run1 = store_a.begin_qc_run("FENCE2", 100, "s16le")
    run2 = store_b.begin_qc_run("FENCE2", 100, "s16le")

    # The newer run finishes first and quarantines the archive.
    done2 = store_b.finish_qc_run("FENCE2", run2.run_no)
    assert done2.status == "quarantined"
    assert store_a.qc_status("FENCE2").status == "quarantined"

    # The older run finishes later while the file is briefly missing, so it
    # concludes pending_review — recorded as history, no effect on the state.
    hidden = pub.with_suffix(".hidden")
    pub.rename(hidden)
    done1 = store_a.finish_qc_run("FENCE2", run1.run_no)
    assert done1.status == "pending_review"
    hidden.rename(pub)

    assert store_a.qc_status("FENCE2").status == "quarantined"
    with pytest.raises(ConflictError, match="隔离"):
        store_a.published_path("FENCE2")
    history = store_a.qc_history("FENCE2")
    assert [r.run_no for r in history] == [2, 1]
    assert history[0].status == "quarantined"
    assert history[1].status == "pending_review"


def test_reinterpret_quarantined_archive_stays_quarantined(tmp_path):
    """Re-interpreting the same immutable bytes reaches the same verdict; the
    archive only leaves quarantine if a *newer* run passes."""
    root = tmp_path / "d"
    store, _ = setup_published(root, anomaly_samples(), "REQC")
    first = interpret(store, "REQC")
    assert first.status == "quarantined"
    second = interpret(store, "REQC")
    assert second.status == "quarantined" and second.run_no == first.run_no + 1
    assert store.qc_status("REQC").status == "quarantined"
    with pytest.raises(ConflictError, match="隔离"):
        store.published_path("REQC")


# --------------------------------------------------------------------- web
@pytest.fixture
def client(tmp_path):
    from fastapi.testclient import TestClient

    app = create_app(tmp_path / "data")
    return TestClient(app)


def publish_via_web(client, tid: str, samples: list[int]) -> bytes:
    archive = struct.pack(f"<{len(samples)}h", *samples)
    frames, count = pack_archive(archive)
    client.post(
        "/deliveries",
        data={
            "transfer_id": tid,
            "frame_count": str(count),
            "size_bytes": str(len(archive)),
            "sha256": hashlib.sha256(archive).hexdigest(),
        },
    )
    csv_text = "".join(f"{i},{f.hex()}\n" for i, f in enumerate(frames, 1))
    client.post(f"/deliveries/{tid}/frames", data={"csv_text": csv_text})
    return archive


QC_FORM = {"confirm": "on", "sample_rate": "100", "sample_format": "s16le"}


def test_web_quarantine_flow(client):
    publish_via_web(client, "QC-W1", anomaly_samples())
    detail = client.get("/deliveries/QC-W1").text
    assert "未判读" in detail and "发起质量判读" in detail
    # 未发起判读的原档照旧下载
    assert client.get("/deliveries/QC-W1/download").status_code == 200

    # The confirmation checkbox is required.
    resp = client.post(
        "/deliveries/QC-W1/qc",
        data={"sample_rate": "100", "sample_format": "s16le"},
        follow_redirects=False,
    )
    assert "err=" in resp.headers["location"]

    resp = client.post("/deliveries/QC-W1/qc", data=QC_FORM, follow_redirects=False)
    assert "ok=" in resp.headers["location"]
    detail = client.get("/deliveries/QC-W1").text
    assert "已隔离" in detail and "#120" in detail and "#400" in detail
    assert "下载 QC-W1.bin" not in detail  # download button hidden
    resp = client.get("/deliveries/QC-W1/download")
    assert resp.status_code == 409 and "隔离" in resp.text
    # 判读轮次历史与处理记录可见
    assert "判读轮次" in detail and "判读隔离" in detail


def test_web_pass_flow_keeps_download(client):
    archive = publish_via_web(client, "QC-W2", clean_samples())
    client.post("/deliveries/QC-W2/qc", data=QC_FORM)
    detail = client.get("/deliveries/QC-W2").text
    assert "判读通过" in detail
    resp = client.get("/deliveries/QC-W2/download")
    assert resp.status_code == 200 and resp.content == archive


def test_web_quarantine_survives_restart(tmp_path):
    from fastapi.testclient import TestClient

    data = tmp_path / "data"
    first = TestClient(create_app(data))
    publish_via_web(first, "QC-W3", anomaly_samples())
    first.post("/deliveries/QC-W3/qc", data=QC_FORM)

    restarted = TestClient(create_app(data))  # brand-new app, same data dir
    detail = restarted.get("/deliveries/QC-W3").text
    assert "已隔离" in detail and "#120" in detail and "#400" in detail
    assert restarted.get("/deliveries/QC-W3/download").status_code == 409


def test_web_index_shows_quarantine_badge(client):
    publish_via_web(client, "QC-W4", anomaly_samples())
    client.post("/deliveries/QC-W4/qc", data=QC_FORM)
    text = client.get("/").text
    assert "已隔离" in text


def test_qc_panel_only_on_published_pages(client):
    client.post(
        "/deliveries",
        data={"transfer_id": "QC-W5", "frame_count": "2", "size_bytes": "300",
              "sha256": "0" * 64},
    )
    assert "发起质量判读" not in client.get("/deliveries/QC-W5").text


def test_api_qc_flow(client):
    publish_via_web(client, "QC-A1", anomaly_samples())
    resp = client.post("/api/deliveries/QC-A1/qc", json={})
    assert resp.status_code == 400  # confirmation required
    resp = client.post("/api/deliveries/QC-A1/qc", json={"confirm": True})
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "quarantined"
    assert body["silence_index"] == 120 and body["clip_index"] == 400
    assert body["sample_count"] == 500

    qc = client.get("/api/deliveries/QC-A1/qc").json()
    assert qc["qc"]["status"] == "quarantined"
    assert qc["qc"]["download_allowed"] is False
    assert len(qc["runs"]) == 1 and qc["runs"][0]["run_no"] == 1

    api = client.get("/api/deliveries/QC-A1").json()
    assert api["qc"]["status"] == "quarantined"
    assert client.get("/api/deliveries/QC-A1/download").status_code == 409


def test_api_qc_unknown_transfer_is_404(client):
    assert client.get("/api/deliveries/NOPE/qc").status_code == 404
    assert client.post("/api/deliveries/NOPE/qc", json={"confirm": True}).status_code == 404
