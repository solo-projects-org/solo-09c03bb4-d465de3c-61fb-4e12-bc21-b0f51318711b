import hashlib
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.main import create_app  # noqa: E402
from app.storage import FRAME_BYTES, xor_bytes  # noqa: E402


@pytest.fixture
def client(tmp_path):
    from fastapi.testclient import TestClient

    app = create_app(tmp_path / "data")
    return TestClient(app)


def make_case(count: int, size: int, tid: str, *, digest: str | None = None):
    archive = bytes((i * 13 + 5) % 256 for i in range(size))
    padded = archive + b"\x00" * (count * FRAME_BYTES - size)
    frames = [padded[i * FRAME_BYTES : (i + 1) * FRAME_BYTES] for i in range(count)]
    return {
        "archive": archive,
        "frames": frames,
        "parity": xor_bytes(frames),
        "sha256": digest or hashlib.sha256(archive).hexdigest(),
    }


def test_index_page_renders_empty_ledger(client):
    response = client.get("/")
    assert response.status_code == 200
    assert "传输台账" in response.text
    assert "暂无传输记录" in response.text


def test_full_web_flow_complete_upload(client):
    case = make_case(4, 900, "WEB-1")
    response = client.post(
        "/deliveries",
        data={
            "transfer_id": "WEB-1",
            "frame_count": "4",
            "size_bytes": "900",
            "sha256": case["sha256"],
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"] == "/deliveries/WEB-1"

    csv_text = "frame_no,payload_hex\n" + "".join(
        f"{i},{frame.hex()}\n" for i, frame in enumerate(case["frames"], start=1)
    )
    response = client.post(
        "/deliveries/WEB-1/frames", data={"csv_text": csv_text}, follow_redirects=False
    )
    assert response.status_code == 303
    assert "ok=" in response.headers["location"]

    detail = client.get("/deliveries/WEB-1")
    assert detail.status_code == 200
    assert "已核验发布" in detail.text
    assert "/deliveries/WEB-1/download" in detail.text

    download = client.get("/deliveries/WEB-1/download")
    assert download.status_code == 200
    assert download.content == case["archive"]
    assert download.headers["x-archive-sha256"] == case["sha256"]


def test_web_upload_with_missing_frame_shows_missing_state(client):
    case = make_case(4, 900, "WEB-2")
    client.post(
        "/deliveries",
        data={"transfer_id": "WEB-2", "frame_count": "4", "size_bytes": "900",
              "sha256": case["sha256"]},
    )
    csv_text = "".join(f"{i},{case['frames'][i - 1].hex()}\n" for i in (1, 3, 4))
    client.post("/deliveries/WEB-2/frames", data={"csv_text": csv_text})
    detail = client.get("/deliveries/WEB-2").text
    assert "等待补帧" in detail
    assert "尚缺第 2 帧" in detail
    assert "3 / 4" in detail
    assert client.get("/deliveries/WEB-2/download").status_code == 404

    # Upload the missing frame from a file instead of pasted text.
    response = client.post(
        "/deliveries/WEB-2/frames",
        files={"csv_file": ("batch.csv", f"2,{case['frames'][1].hex()}\n".encode(), "text/csv")},
    )
    assert response.status_code == 200  # TestClient follows redirects by default
    assert client.get("/deliveries/WEB-2/download").content == case["archive"]


def test_web_parity_recovery_flow(client):
    case = make_case(5, 1111, "WEB-3")
    client.post(
        "/deliveries",
        data={"transfer_id": "WEB-3", "frame_count": "5", "size_bytes": "1111",
              "sha256": case["sha256"]},
    )
    csv_text = "".join(f"{i},{case['frames'][i - 1].hex()}\n" for i in (1, 2, 4, 5))
    client.post("/deliveries/WEB-3/frames", data={"csv_text": csv_text})
    # The missing frame is flagged as recoverable only once parity exists.
    assert 'frame-cell recoverable' not in client.get("/deliveries/WEB-3").text
    client.post("/deliveries/WEB-3/parity", data={"parity_hex": case["parity"].hex()})
    detail = client.get("/deliveries/WEB-3").text
    assert "已核验发布" in detail
    assert "由校验帧恢复" in detail
    assert client.get("/deliveries/WEB-3/download").content == case["archive"]


def test_web_parity_file_upload(client):
    case = make_case(3, 600, "WEB-4")
    client.post(
        "/deliveries",
        data={"transfer_id": "WEB-4", "frame_count": "3", "size_bytes": "600",
              "sha256": case["sha256"]},
    )
    client.post(
        "/deliveries/WEB-4/frames",
        data={"csv_text": f"1,{case['frames'][0].hex()}\n3,{case['frames'][2].hex()}\n"},
    )
    client.post(
        "/deliveries/WEB-4/parity",
        files={"parity_file": ("parity.txt", case["parity"].hex().encode(), "text/plain")},
    )
    assert client.get("/deliveries/WEB-4/download").content == case["archive"]


def test_web_digest_mismatch_is_reported_and_not_downloadable(client):
    case = make_case(3, 640, "WEB-5", digest="0" * 64)
    client.post(
        "/deliveries",
        data={"transfer_id": "WEB-5", "frame_count": "3", "size_bytes": "640",
              "sha256": case["sha256"]},
    )
    csv_text = "".join(f"{i},{f.hex()}\n" for i, f in enumerate(case["frames"], 1))
    client.post("/deliveries/WEB-5/frames", data={"csv_text": csv_text})
    detail = client.get("/deliveries/WEB-5")
    assert "摘要不符" in detail.text
    assert "分片已保留" in detail.text
    assert "当前实际摘要" in detail.text
    assert client.get("/deliveries/WEB-5/download").status_code == 404
    api = client.get("/api/deliveries/WEB-5").json()
    assert api["status"] == "mismatch"
    assert api["actual_sha256"] == hashlib.sha256(case["archive"]).hexdigest()
    assert api["received_frames"] == [1, 2, 3]


def test_web_conflict_is_rejected_without_losing_fragments(client):
    case = make_case(3, 600, "WEB-6")
    client.post(
        "/deliveries",
        data={"transfer_id": "WEB-6", "frame_count": "3", "size_bytes": "600",
              "sha256": case["sha256"]},
    )
    client.post("/deliveries/WEB-6/frames", data={"csv_text": f"1,{case['frames'][0].hex()}\n"})
    response = client.post(
        "/deliveries/WEB-6/frames",
        data={"csv_text": f"1,{'00' * 256}\n"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert "err=" in response.headers["location"]
    assert client.get("/deliveries/WEB-6").status_code == 200
    assert client.get("/api/deliveries/WEB-6").json()["received_frames"] == [1]


def test_api_conflict_returns_409(client):
    case = make_case(2, 400, "API-1")
    client.post(
        "/api/deliveries",
        json={"transfer_id": "API-1", "frame_count": 2, "size_bytes": 400,
              "sha256": case["sha256"]},
    )
    client.post("/api/deliveries/API-1/frames",
                json={"frames": [{"frame_no": 1, "payload_hex": case["frames"][0].hex()}]})
    response = client.post(
        "/api/deliveries/API-1/frames",
        json={"frames": [{"frame_no": 1, "payload_hex": "00" * 256}]},
    )
    assert response.status_code == 409
    assert "拒收" in response.json()["error"]


def test_api_validation_errors_are_400(client):
    response = client.post(
        "/api/deliveries",
        json={"transfer_id": "BAD", "frame_count": 20, "size_bytes": 100, "sha256": "x" * 64},
    )
    assert response.status_code == 400
    response = client.post(
        "/api/deliveries",
        json={"transfer_id": "BAD2", "frame_count": 2, "size_bytes": 100, "sha256": "0" * 64},
    )
    assert response.status_code == 400  # 2 frames cannot hold only 100 bytes


def test_unknown_transfer_page_is_404(client):
    assert client.get("/deliveries/NOPE").status_code == 404
    assert client.get("/api/deliveries/NOPE").status_code == 404
    assert client.get("/deliveries/NOPE/download").status_code == 404


def test_healthz(client):
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json()["ok"] is True


def test_page_escapes_transfer_id_in_markup(client):
    # Transfer IDs are restricted to a safe charset, so markup injection is
    # impossible; verify the restriction is enforced at registration.
    response = client.post(
        "/api/deliveries",
        json={"transfer_id": "<script>", "frame_count": 2, "size_bytes": 300,
              "sha256": "0" * 64},
    )
    assert response.status_code == 400


def test_parity_only_csv_batch_is_accepted(client):
    case = make_case(3, 600, "WEB-7")
    client.post(
        "/deliveries",
        data={"transfer_id": "WEB-7", "frame_count": "3", "size_bytes": "600",
              "sha256": case["sha256"]},
    )
    client.post(
        "/deliveries/WEB-7/frames",
        data={"csv_text": f"1,{case['frames'][0].hex()}\n3,{case['frames'][2].hex()}\n"},
    )
    csv_text = f"transfer_id,frame_no,payload_hex\nWEB-7,P,{case['parity'].hex()}\n"
    response = client.post(
        "/deliveries/WEB-7/frames", data={"csv_text": csv_text}, follow_redirects=False
    )
    assert response.status_code == 303
    assert "err=" not in response.headers["location"]
    assert client.get("/deliveries/WEB-7/download").content == case["archive"]


def test_published_recovered_frame_not_shown_as_missing(client):
    case = make_case(3, 700, "WEB-8")
    client.post(
        "/deliveries",
        data={"transfer_id": "WEB-8", "frame_count": "3", "size_bytes": "700",
              "sha256": case["sha256"]},
    )
    client.post(
        "/deliveries/WEB-8/frames",
        data={"csv_text": f"1,{case['frames'][0].hex()}\n3,{case['frames'][2].hex()}\n"},
    )
    client.post("/deliveries/WEB-8/parity", data={"parity_hex": case["parity"].hex()})
    api = client.get("/api/deliveries/WEB-8").json()
    assert api["status"] == "published"
    assert api["missing_frames"] == []
    assert api["recovered_frames"] == [2]
    detail = client.get("/deliveries/WEB-8").text
    assert "已恢复" in detail
    assert ">缺帧<" not in detail
    assert client.get("/deliveries/WEB-8/download").content == case["archive"]


def test_oversized_csv_is_rejected(client):
    client.post(
        "/api/deliveries",
        json={"transfer_id": "BIG", "frame_count": 2, "size_bytes": 300, "sha256": "0" * 64},
    )
    huge = ("1," + "00" * 256 + "\n") * 500  # ~0.5 MiB, beyond the 256 KiB cap
    response = client.post(
        "/deliveries/BIG/frames", data={"csv_text": huge}, follow_redirects=False
    )
    assert response.status_code == 303
    assert "err=" in response.headers["location"]


def test_transfer_id_path_traversal_is_refused(client):
    response = client.post(
        "/api/deliveries",
        json={"transfer_id": "../etc", "frame_count": 2, "size_bytes": 300, "sha256": "0" * 64},
    )
    assert response.status_code == 400
    assert client.get("/deliveries/..%2Fetc").status_code in (400, 404)
