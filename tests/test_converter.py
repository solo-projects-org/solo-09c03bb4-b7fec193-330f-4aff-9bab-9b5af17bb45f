from __future__ import annotations

import csv
import io
import sqlite3
import zipfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from checker.converter import (
    build_conversion_zip,
    render_updated_manifest,
    run_conversion,
)
from checker.errors import CheckError
from checker.engine import run_check
from checker.manifest import FileKind, parse_manifest

SAMPLE = Path(__file__).resolve().parents[1] / "sample-convert"
BASE = "s3://ferry-lake/warehouse/settlement/voyage_date=2026-11-05"
TICKET_ID = 10
CHANNEL_ID = 11


def sample_manifest():
    return parse_manifest((SAMPLE / "manifest.csv").read_bytes())


def sample_uploads(manifest) -> dict[str, Path]:
    return {
        entry.uploaded_filename: SAMPLE / entry.uploaded_filename
        for entry in manifest.entries
        if (SAMPLE / entry.uploaded_filename).is_file()
    }


def test_sample_conversion_hits_and_skips(tmp_path: Path):
    manifest = sample_manifest()
    result = run_conversion(
        manifest,
        sample_uploads(manifest),
        TICKET_ID,
        CHANNEL_ID,
        tmp_path,
        batch_size=4,
    )

    assert result.output_filename == "delete-pos-0002.parquet"
    assert result.output_original_path == (
        f"{BASE}/delete-eq/delete-pos-0002.parquet"
    )
    assert result.delete_rows_read == 8
    assert result.distinct_delete_keys == 7
    assert result.duplicate_delete_rows == 1
    assert result.positional_rows_written == 6
    assert result.matched_data_rows == {
        "data-cv-0001.parquet": 4,
        "data-cv-0002.parquet": 2,
    }
    assert [s.entry.uploaded_filename for s in result.skipped_data_files] == [
        "data-cv-0003.parquet"
    ]
    assert result.bound_channels["delete-eq-0002.parquet"] == "channel_id"
    assert result.bound_channels["data-cv-0001.parquet"] == "sales_channel"
    assert result.bound_channels["data-cv-0003.parquet"] == "sale_channel"
    assert result.channel_name_mismatch is True

    table = pq.read_table(tmp_path / result.output_filename)
    assert table.schema.names == ["file_path", "pos"]
    assert pa.types.is_string(table.schema.field("file_path").type)
    assert pa.types.is_int64(table.schema.field("pos").type)
    rows = table.to_pylist()
    # data-cv-0001 hits at rows 0,1,2,4 (row 3 = ticket 5004 not deleted);
    # data-cv-0002 hits at rows 3 and 4.
    assert rows == [
        {"file_path": f"{BASE}/data/data-cv-0001.parquet", "pos": 0},
        {"file_path": f"{BASE}/data/data-cv-0001.parquet", "pos": 1},
        {"file_path": f"{BASE}/data/data-cv-0001.parquet", "pos": 2},
        {"file_path": f"{BASE}/data/data-cv-0001.parquet", "pos": 4},
        {"file_path": f"{BASE}/data/data-cv-0002.parquet", "pos": 3},
        {"file_path": f"{BASE}/data/data-cv-0002.parquet", "pos": 4},
    ]


def test_same_sequence_equal_key_adds_no_delete(tmp_path: Path):
    manifest = sample_manifest()
    run_conversion(
        manifest,
        sample_uploads(manifest),
        TICKET_ID,
        CHANNEL_ID,
        tmp_path,
        batch_size=3,
    )
    # data-cv-0003 (sequence 12 == delete sequence 12) must never appear.
    table = pq.read_table(tmp_path / "delete-pos-0002.parquet")
    paths = {row["file_path"] for row in table.to_pylist()}
    assert f"{BASE}/data/data-cv-0003.parquet" not in paths


def test_null_matches_only_null(tmp_path: Path):
    schema = pa.schema(
        [
            pa.field("ticket_no", pa.int64(),
                     metadata={"PARQUET:field_id": "10"}),
            pa.field("sales_channel", pa.int32(),
                     metadata={"PARQUET:field_id": "11"}),
        ]
    )
    data = tmp_path / "data.parquet"
    pq.write_table(
        pa.table(
            {"ticket_no": [1, 1, 2], "sales_channel": [None, 5, None]},
            schema=schema,
        ),
        data,
    )
    eq = tmp_path / "delete-eq.parquet"
    eq_schema = pa.schema(
        [
            pa.field("ticket_no", pa.int64(),
                     metadata={"PARQUET:field_id": "10"}),
            pa.field("channel_id", pa.int32(),
                     metadata={"PARQUET:field_id": "11"}),
        ]
    )
    pq.write_table(
        pa.table(
            {"ticket_no": [1, 2], "channel_id": [None, None]},
            schema=eq_schema,
        ),
        eq,
    )
    manifest_text = (
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        f"{BASE}/data/data.parquet,data.parquet,data,3\n"
        f"{BASE}/delete-eq/delete-eq.parquet,delete-eq.parquet,"
        "equality_delete,9\n"
    )
    manifest = parse_manifest(manifest_text)
    result = run_conversion(
        manifest,
        {"data.parquet": data, "delete-eq.parquet": eq},
        TICKET_ID,
        CHANNEL_ID,
        tmp_path,
        batch_size=2,
    )
    table = pq.read_table(tmp_path / result.output_filename)
    # (1, NULL) -> row 0 only; row 1 has channel 5 and must not match.
    # (2, NULL) -> row 2.
    assert table.to_pylist() == [
        {"file_path": f"{BASE}/data/data.parquet", "pos": 0},
        {"file_path": f"{BASE}/data/data.parquet", "pos": 2},
    ]


def test_channel_rename_still_matches_by_field_id(tmp_path: Path):
    manifest = sample_manifest()
    result = run_conversion(
        manifest,
        sample_uploads(manifest),
        TICKET_ID,
        CHANNEL_ID,
        tmp_path,
        batch_size=2,
    )
    # The equality delete uses channel_id; data uses sales_channel /
    # sale_channel.  Matching still happens and produces rows.
    assert result.positional_rows_written == 6


def test_updated_manifest_replaces_only_equality_entry(tmp_path: Path):
    manifest = sample_manifest()
    result = run_conversion(
        manifest,
        sample_uploads(manifest),
        TICKET_ID,
        CHANNEL_ID,
        tmp_path,
    )
    text = render_updated_manifest(
        manifest,
        result.equality_entry,
        result.output_filename,
        result.output_original_path,
    )
    rows = list(csv.DictReader(io.StringIO(text)))
    kinds = {(row["uploaded_filename"], row["file_type"]) for row in rows}
    assert (
        "delete-pos-0002.parquet",
        FileKind.POSITION_DELETE.value,
    ) in kinds
    assert not any(name == "delete-eq-0002.parquet" for name, _ in kinds)
    converted = next(
        row for row in rows if row["uploaded_filename"] == "delete-pos-0002.parquet"
    )
    # The original delete sequence number is preserved.
    assert converted["data_sequence_number"] == "12"
    # Every other entry keeps its exact values.
    by_name = {row["uploaded_filename"]: row for row in rows}
    original = {e.uploaded_filename: e for e in manifest.entries}
    for name, entry in original.items():
        if name == "delete-eq-0002.parquet":
            continue
        kept = by_name[name]
        assert kept["original_path"] == entry.original_path
        assert kept["file_type"] == entry.kind.value
        assert kept["data_sequence_number"] == str(entry.sequence_number)


def test_zip_bundle_contents_and_checker_round_trip(tmp_path: Path):
    manifest = sample_manifest()
    result = run_conversion(
        manifest,
        sample_uploads(manifest),
        TICKET_ID,
        CHANNEL_ID,
        tmp_path,
        batch_size=5,
    )
    zip_path = build_conversion_zip(
        result, tmp_path / result.output_filename, tmp_path
    )
    with zipfile.ZipFile(zip_path) as archive:
        assert sorted(archive.namelist()) == [
            "delete-pos-0002.parquet",
            "manifest.csv",
        ]
        extracted = tmp_path / "pkg"
        archive.extractall(extracted)

    new_manifest = parse_manifest((extracted / "manifest.csv").read_bytes())
    combined: dict[str, Path] = {}
    for entry in new_manifest.entries:
        replica = extracted / entry.uploaded_filename
        if not replica.is_file():
            replica = SAMPLE / entry.uploaded_filename
        combined[entry.uploaded_filename] = replica

    check = run_check(
        new_manifest, combined, [5001, 5002, 5003, 5005, 6002], "ticket_no"
    )
    by_ticket = {t.ticket: t for t in check.tickets}
    # 5001 deleted in the older data file only.
    occ_5001 = by_ticket[5001].occurrences
    files = {o.file_entry.uploaded_filename: o for o in occ_5001}
    assert files["data-cv-0001.parquet"].deleted
    assert not files["data-cv-0003.parquet"].deleted
    assert by_ticket[5005].deleted
    assert by_ticket[6002].found and not by_ticket[6002].deleted


def test_missing_replica_aborts_without_output(tmp_path: Path):
    manifest = sample_manifest()
    uploads = sample_uploads(manifest)
    del uploads["data-cv-0002.parquet"]
    with pytest.raises(CheckError) as excinfo:
        run_conversion(
            manifest, uploads, TICKET_ID, CHANNEL_ID, tmp_path
        )
    assert "data-cv-0002.parquet" in excinfo.value.files
    assert not list(tmp_path.glob("*.parquet"))
    assert not list(tmp_path.glob("*.zip"))


def test_unknown_field_id_aborts(tmp_path: Path):
    manifest = sample_manifest()
    with pytest.raises(CheckError) as excinfo:
        run_conversion(
            manifest, sample_uploads(manifest), 999, CHANNEL_ID, tmp_path
        )
    assert "999" in excinfo.value.message
    assert excinfo.value.files  # always names a file


def test_duplicate_field_id_in_file_aborts(tmp_path: Path):
    schema = pa.schema(
        [
            pa.field("ticket_no", pa.int64(),
                     metadata={"PARQUET:field_id": "10"}),
            pa.field("ticket_alias", pa.int64(),
                     metadata={"PARQUET:field_id": "10"}),
            pa.field("sales_channel", pa.int32(),
                     metadata={"PARQUET:field_id": "11"}),
        ]
    )
    data = tmp_path / "data.parquet"
    pq.write_table(
        pa.table(
            {
                "ticket_no": [1],
                "ticket_alias": [1],
                "sales_channel": [2],
            },
            schema=schema,
        ),
        data,
    )
    eq = tmp_path / "delete-eq.parquet"
    pq.write_table(
        pa.table(
            {
                "ticket_no": pa.array([1], pa.int64()),
                "channel_id": pa.array([2], pa.int32()),
            },
            schema=pa.schema(
                [
                    pa.field("ticket_no", pa.int64(),
                             metadata={"PARQUET:field_id": "10"}),
                    pa.field("channel_id", pa.int32(),
                             metadata={"PARQUET:field_id": "11"}),
                ]
            ),
        ),
        eq,
    )
    manifest = parse_manifest(
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        f"{BASE}/data/data.parquet,data.parquet,data,1\n"
        f"{BASE}/delete-eq/delete-eq.parquet,delete-eq.parquet,"
        "equality_delete,5\n"
    )
    with pytest.raises(CheckError, match="无法唯一绑定"):
        run_conversion(
            manifest,
            {"data.parquet": data, "delete-eq.parquet": eq},
            TICKET_ID,
            CHANNEL_ID,
            tmp_path,
        )


def test_non_integer_key_column_aborts(tmp_path: Path):
    schema = pa.schema(
        [
            pa.field("ticket_no", pa.string(),
                     metadata={"PARQUET:field_id": "10"}),
            pa.field("sales_channel", pa.int32(),
                     metadata={"PARQUET:field_id": "11"}),
        ]
    )
    data = tmp_path / "data.parquet"
    pq.write_table(
        pa.table({"ticket_no": ["1"], "sales_channel": [2]}, schema=schema),
        data,
    )
    eq = tmp_path / "delete-eq.parquet"
    eq_schema = pa.schema(
        [
            pa.field("ticket_no", pa.int64(),
                     metadata={"PARQUET:field_id": "10"}),
            pa.field("channel_id", pa.int32(),
                     metadata={"PARQUET:field_id": "11"}),
        ]
    )
    pq.write_table(
        pa.table({"ticket_no": [1], "channel_id": [2]}, schema=eq_schema),
        eq,
    )
    manifest = parse_manifest(
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        f"{BASE}/data/data.parquet,data.parquet,data,1\n"
        f"{BASE}/delete-eq/delete-eq.parquet,delete-eq.parquet,"
        "equality_delete,5\n"
    )
    with pytest.raises(CheckError) as excinfo:
        run_conversion(
            manifest,
            {"data.parquet": data, "delete-eq.parquet": eq},
            TICKET_ID,
            CHANNEL_ID,
            tmp_path,
        )
    assert "data.parquet" in excinfo.value.files
    assert "int32" in excinfo.value.message


def test_two_equality_entries_aborts(tmp_path: Path):
    manifest = parse_manifest(
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        f"{BASE}/data/data.parquet,data.parquet,data,1\n"
        f"{BASE}/delete-eq/a.parquet,a.parquet,equality_delete,5\n"
        f"{BASE}/delete-eq/b.parquet,b.parquet,equality_delete,5\n"
    )
    with pytest.raises(CheckError, match="唯一的等值删除"):
        run_conversion(manifest, {}, TICKET_ID, CHANNEL_ID, tmp_path)


def test_corrupt_file_aborts_and_names_file(tmp_path: Path):
    bad = tmp_path / "broken.parquet"
    bad.write_bytes(b"not parquet at all")
    manifest = parse_manifest(
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        f"{BASE}/data/data.parquet,data.parquet,data,1\n"
        f"{BASE}/delete-eq/broken.parquet,broken.parquet,equality_delete,5\n"
    )
    with pytest.raises(CheckError) as excinfo:
        run_conversion(
            manifest,
            {"data.parquet": SAMPLE / "data-cv-0001.parquet",
             "broken.parquet": bad},
            TICKET_ID,
            CHANNEL_ID,
            tmp_path,
        )
    assert "broken.parquet" in excinfo.value.files


def test_ticket_column_name_must_match_across_data_files(tmp_path: Path):
    schema_a = pa.schema(
        [
            pa.field("ticket_no", pa.int64(),
                     metadata={"PARQUET:field_id": "10"}),
            pa.field("sales_channel", pa.int32(),
                     metadata={"PARQUET:field_id": "11"}),
        ]
    )
    schema_b = pa.schema(
        [
            pa.field("ticket_id", pa.int64(),
                     metadata={"PARQUET:field_id": "10"}),
            pa.field("sales_channel", pa.int32(),
                     metadata={"PARQUET:field_id": "11"}),
        ]
    )
    d1 = tmp_path / "d1.parquet"
    d2 = tmp_path / "d2.parquet"
    pq.write_table(
        pa.table({"ticket_no": [1], "sales_channel": [2]}, schema=schema_a), d1
    )
    pq.write_table(
        pa.table({"ticket_id": [1], "sales_channel": [2]}, schema=schema_b), d2
    )
    eq = tmp_path / "delete-eq.parquet"
    pq.write_table(
        pa.table(
            {"ticket_no": [1], "sales_channel": [2]}, schema=schema_a
        ),
        eq,
    )
    manifest = parse_manifest(
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        f"{BASE}/data/d1.parquet,d1.parquet,data,1\n"
        f"{BASE}/data/d2.parquet,d2.parquet,data,1\n"
        f"{BASE}/delete-eq/delete-eq.parquet,delete-eq.parquet,"
        "equality_delete,5\n"
    )
    with pytest.raises(CheckError, match="票号列必须同名"):
        run_conversion(
            manifest,
            {"d1.parquet": d1, "d2.parquet": d2, "delete-eq.parquet": eq},
            TICKET_ID,
            CHANNEL_ID,
            tmp_path,
        )


def test_no_matching_keys_writes_empty_positional_file(tmp_path: Path):
    schema = pa.schema(
        [
            pa.field("ticket_no", pa.int64(),
                     metadata={"PARQUET:field_id": "10"}),
            pa.field("sales_channel", pa.int32(),
                     metadata={"PARQUET:field_id": "11"}),
        ]
    )
    data = tmp_path / "data.parquet"
    pq.write_table(
        pa.table({"ticket_no": [1], "sales_channel": [2]}, schema=schema), data
    )
    eq = tmp_path / "delete-eq.parquet"
    pq.write_table(
        pa.table({"ticket_no": [42], "sales_channel": [9]}, schema=schema), eq
    )
    manifest = parse_manifest(
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        f"{BASE}/data/data.parquet,data.parquet,data,1\n"
        f"{BASE}/delete-eq/delete-eq.parquet,delete-eq.parquet,"
        "equality_delete,5\n"
    )
    result = run_conversion(
        manifest,
        {"data.parquet": data, "delete-eq.parquet": eq},
        TICKET_ID,
        CHANNEL_ID,
        tmp_path,
    )
    assert result.positional_rows_written == 0
    table = pq.read_table(tmp_path / result.output_filename)
    assert table.num_rows == 0
    assert table.schema.names == ["file_path", "pos"]
    assert any("没有任何数据行命中" in w for w in result.warnings)


def test_int32_and_int64_key_types_mix(tmp_path: Path):
    data_schema = pa.schema(
        [
            pa.field("ticket_no", pa.int64(),
                     metadata={"PARQUET:field_id": "10"}),
            pa.field("sales_channel", pa.int32(),
                     metadata={"PARQUET:field_id": "11"}),
        ]
    )
    eq_schema = pa.schema(
        [
            pa.field("ticket_no", pa.int32(),
                     metadata={"PARQUET:field_id": "10"}),
            pa.field("channel_id", pa.int64(),
                     metadata={"PARQUET:field_id": "11"}),
        ]
    )
    data = tmp_path / "data.parquet"
    pq.write_table(
        pa.table(
            {"ticket_no": [7, 8], "sales_channel": pa.array([1, 2], pa.int32())},
            schema=data_schema,
        ),
        data,
    )
    eq = tmp_path / "delete-eq.parquet"
    pq.write_table(
        pa.table(
            {"ticket_no": pa.array([7], pa.int32()),
             "channel_id": pa.array([1], pa.int64())},
            schema=eq_schema,
        ),
        eq,
    )
    manifest = parse_manifest(
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        f"{BASE}/data/data.parquet,data.parquet,data,1\n"
        f"{BASE}/delete-eq/delete-eq.parquet,delete-eq.parquet,"
        "equality_delete,5\n"
    )
    result = run_conversion(
        manifest,
        {"data.parquet": data, "delete-eq.parquet": eq},
        TICKET_ID,
        CHANNEL_ID,
        tmp_path,
    )
    table = pq.read_table(tmp_path / result.output_filename)
    assert table.to_pylist() == [
        {"file_path": f"{BASE}/data/data.parquet", "pos": 0}
    ]


def test_sqlite_spill_database_is_removed(tmp_path: Path):
    manifest = sample_manifest()
    run_conversion(
        manifest,
        sample_uploads(manifest),
        TICKET_ID,
        CHANNEL_ID,
        tmp_path,
        batch_size=2,
    )
    assert not list(tmp_path.glob("*.sqlite3"))


def test_batch_size_validation(tmp_path: Path):
    manifest = sample_manifest()
    with pytest.raises(CheckError, match="批大小"):
        run_conversion(
            manifest,
            sample_uploads(manifest),
            TICKET_ID,
            CHANNEL_ID,
            tmp_path,
            batch_size=0,
        )


def test_same_field_ids_rejected(tmp_path: Path):
    manifest = sample_manifest()
    with pytest.raises(CheckError, match="不能相同"):
        run_conversion(
            manifest,
            sample_uploads(manifest),
            TICKET_ID,
            TICKET_ID,
            tmp_path,
        )


def test_large_file_row_numbers_survive_batching(tmp_path: Path):
    # 1,000 data rows processed 37 at a time: row numbers must stay the
    # original file-wide indices and duplicate delete keys must not create
    # duplicate positional entries across batch boundaries.
    data_schema = pa.schema(
        [
            pa.field("ticket_no", pa.int64(),
                     metadata={"PARQUET:field_id": "10"}),
            pa.field("sales_channel", pa.int32(),
                     metadata={"PARQUET:field_id": "11"}),
        ]
    )
    eq_schema = pa.schema(
        [
            pa.field("ticket_no", pa.int64(),
                     metadata={"PARQUET:field_id": "10"}),
            pa.field("channel_id", pa.int32(),
                     metadata={"PARQUET:field_id": "11"}),
        ]
    )
    tickets = list(range(100_000, 101_000))
    channels = [ticket % 5 for ticket in tickets]
    data = tmp_path / "data.parquet"
    pq.write_table(
        pa.table(
            {"ticket_no": tickets, "sales_channel": pa.array(channels, pa.int32())},
            schema=data_schema,
        ),
        data,
        row_group_size=128,
    )
    # Delete every 100th ticket; repeat the first key three times.
    delete_tickets = tickets[::100]
    eq_rows = [
        {"ticket_no": delete_tickets[0], "channel_id": delete_tickets[0] % 5}
    ] * 3 + [
        {"ticket_no": ticket, "channel_id": ticket % 5}
        for ticket in delete_tickets
    ]
    eq = tmp_path / "delete-eq.parquet"
    pq.write_table(pa.Table.from_pylist(eq_rows, schema=eq_schema), eq)

    manifest = parse_manifest(
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        f"{BASE}/data/data.parquet,data.parquet,data,1\n"
        f"{BASE}/delete-eq/delete-eq.parquet,delete-eq.parquet,"
        "equality_delete,5\n"
    )
    result = run_conversion(
        manifest,
        {"data.parquet": data, "delete-eq.parquet": eq},
        TICKET_ID,
        CHANNEL_ID,
        tmp_path,
        batch_size=37,
    )
    expected_positions = list(range(0, 1000, 100))
    table = pq.read_table(tmp_path / result.output_filename)
    assert [row["pos"] for row in table.to_pylist()] == expected_positions
    assert result.positional_rows_written == len(expected_positions)
    assert result.duplicate_delete_rows == 3
    assert result.distinct_delete_keys == len(expected_positions)


def test_sqlite_is_operator_semantics():
    # Guard the null-aware comparison the engine relies on.
    connection = sqlite3.connect(":memory:")
    connection.execute("CREATE TABLE d(t INTEGER, c INTEGER)")
    connection.executemany(
        "INSERT INTO d VALUES(?, ?)", [(1, None), (None, 2), (1, 2)]
    )
    assert connection.execute(
        "SELECT count(*) FROM d WHERE t IS ? AND c IS ?", (1, None)
    ).fetchone()[0] == 1
    assert connection.execute(
        "SELECT count(*) FROM d WHERE t IS ? AND c IS ?", (None, 2)
    ).fetchone()[0] == 1
    assert connection.execute(
        "SELECT count(*) FROM d WHERE t IS ? AND c IS ?", (1, 2)
    ).fetchone()[0] == 1
    assert connection.execute(
        "SELECT count(*) FROM d WHERE t IS ? AND c IS ?", (None, None)
    ).fetchone()[0] == 0
