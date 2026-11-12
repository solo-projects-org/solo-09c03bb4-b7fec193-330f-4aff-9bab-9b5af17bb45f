"""Generate the equality-delete -> positional-delete conversion sample.

The conversion demo lives in its own directory so the checker sample stays
untouched.  It deliberately covers:

* a sales channel column renamed between data replicas and the equality
  delete file (``sales_channel`` vs ``channel_id`` vs ``sale_channel``),
  which still matches because binding uses Iceberg field ids;
* null key values that only match nulls;
* a data file whose data sequence number equals the delete sequence number:
  those rows must not produce positional delete entries;
* an existing positional delete manifest entry that must pass through
  unchanged;
* duplicate equality delete rows that collapse to one distinct key.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
SAMPLE = ROOT / "sample-convert"

TICKET_FIELD_ID = 10
CHANNEL_FIELD_ID = 11

BASE = "s3://ferry-lake/warehouse/settlement/voyage_date=2026-11-05"
DATA1_PATH = f"{BASE}/data/data-cv-0001.parquet"
DATA2_PATH = f"{BASE}/data/data-cv-0002.parquet"
DATA3_PATH = f"{BASE}/data/data-cv-0003.parquet"
EQ_DELETE_PATH = f"{BASE}/delete-eq/delete-eq-0002.parquet"
POS_DELETE_PATH = f"{BASE}/delete-pos/delete-pos-existing-0001.parquet"

# data-0001/0002 call the channel column sales_channel; data-0003 renames it
# to sale_channel and the equality delete file calls it channel_id.  Field ids
# make the binding unambiguous.
DATA_SCHEMA_A = pa.schema(
    [
        pa.field(
            "ticket_no",
            pa.int64(),
            metadata={"PARQUET:field_id": str(TICKET_FIELD_ID)},
        ),
        pa.field(
            "sales_channel",
            pa.int32(),
            metadata={"PARQUET:field_id": str(CHANNEL_FIELD_ID)},
        ),
        pa.field("passenger_name", pa.string()),
    ]
)

DATA_SCHEMA_B = pa.schema(
    [
        pa.field(
            "ticket_no",
            pa.int64(),
            metadata={"PARQUET:field_id": str(TICKET_FIELD_ID)},
        ),
        pa.field(
            "sale_channel",
            pa.int32(),
            metadata={"PARQUET:field_id": str(CHANNEL_FIELD_ID)},
        ),
        pa.field("passenger_name", pa.string()),
    ]
)

EQ_DELETE_SCHEMA = pa.schema(
    [
        pa.field(
            "ticket_no",
            pa.int64(),
            metadata={"PARQUET:field_id": str(TICKET_FIELD_ID)},
        ),
        pa.field(
            "channel_id",
            pa.int32(),
            metadata={"PARQUET:field_id": str(CHANNEL_FIELD_ID)},
        ),
    ]
)

POS_DELETE_SCHEMA = pa.schema(
    [
        pa.field("file_path", pa.string()),
        pa.field("pos", pa.int64()),
    ]
)


def write_data1() -> None:
    # Two row groups of 4 rows so batch/row-number logic is exercised.
    rows = [
        {"ticket_no": 5001, "sales_channel": 1, "passenger_name": "A1"},
        {"ticket_no": 5002, "sales_channel": 2, "passenger_name": "A2"},
        {"ticket_no": 5003, "sales_channel": None, "passenger_name": "A3"},
        {"ticket_no": 5004, "sales_channel": 1, "passenger_name": "A4"},
        {"ticket_no": 5005, "sales_channel": 1, "passenger_name": "A5"},
        {"ticket_no": 5006, "sales_channel": 3, "passenger_name": "A6"},
        {"ticket_no": 5007, "sales_channel": None, "passenger_name": "A7"},
        {"ticket_no": 5008, "sales_channel": 2, "passenger_name": "A8"},
    ]
    table = pa.Table.from_pylist(rows, schema=DATA_SCHEMA_A)
    pq.write_table(
        table, SAMPLE / "data-cv-0001.parquet", row_group_size=4,
        compression="snappy",
    )


def write_data2() -> None:
    rows = [
        {"ticket_no": 6001, "sales_channel": 1, "passenger_name": "B1"},
        {"ticket_no": 6002, "sales_channel": 2, "passenger_name": "B2"},
        {"ticket_no": 6003, "sales_channel": 2, "passenger_name": "B3"},
        {"ticket_no": 5002, "sales_channel": 9, "passenger_name": "B4"},
        {"ticket_no": 5003, "sales_channel": None, "passenger_name": "B5"},
    ]
    table = pa.Table.from_pylist(rows, schema=DATA_SCHEMA_A)
    pq.write_table(
        table, SAMPLE / "data-cv-0002.parquet", row_group_size=3,
        compression="snappy",
    )


def write_data3() -> None:
    # Same voyage day, newer file: its sequence number equals the delete's
    # sequence number, so none of its rows may be added to the output.
    rows = [
        {"ticket_no": 5001, "sale_channel": 1, "passenger_name": "C1"},
        {"ticket_no": 7001, "sale_channel": 2, "passenger_name": "C2"},
    ]
    table = pa.Table.from_pylist(rows, schema=DATA_SCHEMA_B)
    pq.write_table(
        table, SAMPLE / "data-cv-0003.parquet", row_group_size=2,
        compression="snappy",
    )


def write_equality_delete() -> None:
    rows = [
        # Hits data-cv-0001 row 1 (ticket 5002 / channel 2).
        {"ticket_no": 5002, "channel_id": 2},
        # Null channel: only data-cv-0001 row 2 (5003, NULL) matches.
        {"ticket_no": 5003, "channel_id": None},
        # Hits data-cv-0001 row 4 (5005 / channel 1).
        {"ticket_no": 5005, "channel_id": 1},
        # Hits data-cv-0002 row 3 (5002 / channel 9): same ticket, different
        # channel than the first key — both keys must be kept.
        {"ticket_no": 5002, "channel_id": 9},
        # Duplicate of an earlier key; it must not create a second record.
        {"ticket_no": 5005, "channel_id": 1},
        # Ticket 5001 / channel 1 exists in data-cv-0001 and data-cv-0003.
        # data-cv-0003 shares the delete sequence number and is skipped.
        {"ticket_no": 5001, "channel_id": 1},
        # No matching data row anywhere.
        {"ticket_no": 9999, "channel_id": 7},
        # Null ticket only matches a null ticket (no such row in the sample).
        {"ticket_no": None, "channel_id": 1},
    ]
    table = pa.Table.from_pylist(rows, schema=EQ_DELETE_SCHEMA)
    pq.write_table(
        table,
        SAMPLE / "delete-eq-0002.parquet",
        row_group_size=3,
        compression="snappy",
    )


def write_existing_position_delete() -> None:
    table = pa.Table.from_pylist(
        [{"file_path": DATA1_PATH, "pos": 7}],
        schema=POS_DELETE_SCHEMA,
    )
    pq.write_table(
        table,
        SAMPLE / "delete-pos-existing-0001.parquet",
        compression="snappy",
    )


def write_manifest() -> None:
    lines = [
        "original_path,uploaded_filename,file_type,data_sequence_number",
        f"{DATA1_PATH},data-cv-0001.parquet,data,7",
        f"{DATA2_PATH},data-cv-0002.parquet,data,7",
        f"{DATA3_PATH},data-cv-0003.parquet,data,12",
        f"{POS_DELETE_PATH},delete-pos-existing-0001.parquet,position_delete,8",
        f"{EQ_DELETE_PATH},delete-eq-0002.parquet,equality_delete,12",
    ]
    (SAMPLE / "manifest.csv").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )


def main() -> None:
    SAMPLE.mkdir(parents=True, exist_ok=True)
    write_data1()
    write_data2()
    write_data3()
    write_equality_delete()
    write_existing_position_delete()
    write_manifest()
    for name in (
        "data-cv-0001.parquet",
        "data-cv-0002.parquet",
        "data-cv-0003.parquet",
        "delete-eq-0002.parquet",
        "delete-pos-existing-0001.parquet",
        "manifest.csv",
    ):
        print(f"wrote {SAMPLE / name}")


if __name__ == "__main__":
    main()
