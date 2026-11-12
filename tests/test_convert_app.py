from __future__ import annotations

import io
import zipfile
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from app import app

SAMPLE = Path(__file__).resolve().parents[1] / "sample-convert"


@pytest.fixture()
def client():
    app.config.update(TESTING=True)
    with app.test_client() as client:
        yield client


def _form(client, *, drop=(), **overrides):
    files = []
    for path in sorted(SAMPLE.glob("*.parquet")):
        if path.name in drop:
            continue
        files.append(
            (
                io.BytesIO(path.read_bytes()),
                path.name,
                "application/octet-stream",
            )
        )
    data = {
        "manifest": (
            io.BytesIO((SAMPLE / "manifest.csv").read_bytes()),
            "manifest.csv",
        ),
        "files": files,
        "ticket_field_id": "10",
        "channel_field_id": "11",
        "batch_size": "4",
    }
    data.update(overrides)
    return client.post(
        "/convert", data=data, content_type="multipart/form-data"
    )


def test_index_links_to_conversion(client):
    html = client.get("/").get_data(as_text=True)
    assert "等值删除 → 位置删除转换" in html
    assert "/convert/sample" in html


def test_convert_upload_returns_zip(client):
    response = _form(client)
    assert response.status_code == 200
    assert response.mimetype == "application/zip"
    archive = zipfile.ZipFile(io.BytesIO(response.data))
    assert sorted(archive.namelist()) == [
        "delete-pos-0002.parquet",
        "manifest.csv",
    ]
    manifest_text = archive.read("manifest.csv").decode()
    assert "delete-pos-0002.parquet,position_delete,12" in manifest_text
    assert "equality_delete" not in manifest_text
    # Existing positional delete entry passes through unchanged.
    assert "delete-pos-existing-0001.parquet,position_delete,8" in manifest_text

    table = pq.read_table(io.BytesIO(archive.read("delete-pos-0002.parquet")))
    assert table.num_rows == 6


def test_convert_sample_page_shows_rename_and_sequence_story(client):
    response = client.get("/convert/sample")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "等值删除转换结果" in html
    assert "channel_id" in html
    assert "sale_channel" in html
    assert "data-cv-0003.parquet" in html
    assert "不会新增删除记录" in html
    # The same-key, same-sequence file is listed as skipped.
    assert "因序列号未严格小于而跳过" in html
    assert "/convert/sample.zip" in html


def test_convert_sample_zip_download(client):
    response = client.get("/convert/sample.zip")
    assert response.status_code == 200
    assert response.mimetype == "application/zip"
    archive = zipfile.ZipFile(io.BytesIO(response.data))
    assert "manifest.csv" in archive.namelist()


def test_missing_replica_returns_named_error_and_no_zip(client):
    response = _form(client, drop={"data-cv-0001.parquet"})
    assert response.status_code == 400
    html = response.get_data(as_text=True)
    assert "转换未完成" in html
    assert "data-cv-0001.parquet" in html
    assert "没有生成任何转换包" in html


def test_unknown_field_id_returns_named_error(client):
    response = _form(client, ticket_field_id="4242")
    assert response.status_code == 400
    html = response.get_data(as_text=True)
    assert "4242" in html
    assert "找不到字段 ID" in html


def test_non_integer_field_id_rejected(client):
    response = _form(client, channel_field_id="abc")
    assert response.status_code == 400
    assert "不是整数" in response.get_data(as_text=True)


def test_missing_manifest_rejected(client):
    response = client.post(
        "/convert",
        data={"ticket_field_id": "10", "channel_field_id": "11"},
        content_type="multipart/form-data",
    )
    assert response.status_code == 400
    assert "请上传" in response.get_data(as_text=True)


def test_duplicate_field_id_in_upload_aborts(client, tmp_path: Path):
    bad_schema_path = tmp_path / "data-cv-0001.parquet"
    import pyarrow as pa

    schema = pa.schema(
        [
            pa.field("ticket_no", pa.int64(),
                     metadata={"PARQUET:field_id": "10"}),
            pa.field("ticket_dup", pa.int64(),
                     metadata={"PARQUET:field_id": "10"}),
            pa.field("sales_channel", pa.int32(),
                     metadata={"PARQUET:field_id": "11"}),
        ]
    )
    pq.write_table(
        pa.table(
            {"ticket_no": [1], "ticket_dup": [1], "sales_channel": [2]},
            schema=schema,
        ),
        bad_schema_path,
    )
    files = []
    for path in sorted(SAMPLE.glob("*.parquet")):
        if path.name == "data-cv-0001.parquet":
            payload = bad_schema_path.read_bytes()
        else:
            payload = path.read_bytes()
        files.append((io.BytesIO(payload), path.name, "application/octet-stream"))
    response = client.post(
        "/convert",
        data={
            "manifest": (
                io.BytesIO((SAMPLE / "manifest.csv").read_bytes()),
                "manifest.csv",
            ),
            "files": files,
            "ticket_field_id": "10",
            "channel_field_id": "11",
        },
        content_type="multipart/form-data",
    )
    assert response.status_code == 400
    assert "无法唯一绑定" in response.get_data(as_text=True)
