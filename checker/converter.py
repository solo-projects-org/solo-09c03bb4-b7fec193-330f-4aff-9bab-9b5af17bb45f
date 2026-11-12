"""Convert an Iceberg v2 equality delete into a positional delete.

The checker page only understands positional deletes.  When the operator
cancels tickets by (ticket number, sales channel) pair, the lake writes an
*equality* delete file instead.  This module turns the single equality
delete replica into an equivalent positional delete:

* key columns are located by **Iceberg field id**, never by name.  Each id
  must resolve to exactly one top-level ``int32``/``int64`` column in every
  data file and in the equality delete file;
* the channel column may be named differently in different files, while the
  ticket column keeps the same name across the data replicas;
* a delete key matches a data row only when both key values are equal, with
  SQL null semantics (a null equals only a null);
* a row is emitted only when the delete file's data sequence number is
  strictly greater than the data file's;
* rows are read and written in fixed-size batches and the delete keys are
  spilled to a temporary SQLite database, so neither the full data set nor
  the full delete key set is ever held in memory;
* any missing replica, ambiguous key column or read/write failure aborts the
  whole conversion with a ``CheckError`` naming the file.

The equality delete manifest entry is replaced by a positional delete entry
that keeps the original data sequence number.  Every other manifest entry is
left untouched.
"""

from __future__ import annotations

import csv
import io
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping

import pyarrow as pa
import pyarrow.parquet as pq

from .errors import CheckError
from .manifest import FileKind, Manifest, ManifestEntry, parse_manifest

FIELD_ID_METADATA_KEY = b"PARQUET:field_id"
DELETE_PATH_COLUMN = "file_path"
DELETE_POS_COLUMN = "pos"

_POSITIONAL_DELETE_SCHEMA = pa.schema(
    [
        pa.field(DELETE_PATH_COLUMN, pa.string(), nullable=False),
        pa.field(DELETE_POS_COLUMN, pa.int64(), nullable=False),
    ]
)


@dataclass
class BoundKey:
    """One key column bound inside one Parquet file."""

    field_id: int
    column_name: str
    column_type: pa.DataType


@dataclass
class SkippedDataFile:
    entry: ManifestEntry
    reason: str


@dataclass
class ConversionResult:
    manifest: Manifest
    ticket_field_id: int
    channel_field_id: int
    equality_entry: ManifestEntry
    output_filename: str
    output_original_path: str
    output_manifest_name: str
    batch_size: int
    delete_rows_read: int
    distinct_delete_keys: int
    duplicate_delete_rows: int
    positional_rows_written: int
    matched_data_rows: dict[str, int] = field(default_factory=dict)
    skipped_data_files: list[SkippedDataFile] = field(default_factory=list)
    channel_name_mismatch: bool = False
    bound_channels: dict[str, str] = field(default_factory=dict)
    unused_uploads: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def has_hits(self) -> bool:
        return self.positional_rows_written > 0


# ---------------------------------------------------------------------------
# Key column binding
# ---------------------------------------------------------------------------


def _field_id_of(field_: pa.Field, *, file_label: str) -> int | None:
    metadata = field_.metadata or {}
    raw = metadata.get(FIELD_ID_METADATA_KEY)
    if raw is None:
        return None
    try:
        return int(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
    except (TypeError, ValueError):
        raise CheckError(
            f"文件 “{file_label}” 的列 “{field_.name}” 的 Parquet field id "
            f"“{raw!r}” 不是整数，无法按字段 ID 绑定键列。",
            files=[file_label],
        ) from None


def _bind_key_column(
    schema: pa.Schema,
    field_id: int,
    *,
    file_label: str,
    role: str,
) -> BoundKey:
    """Resolve ``field_id`` to a unique top-level int32/int64 column."""
    matches: list[pa.Field] = []
    for index in range(len(schema)):
        column = schema.field(index)  # top-level fields only
        if _field_id_of(column, file_label=file_label) == field_id:
            matches.append(column)
    if not matches:
        raise CheckError(
            f"{role} “{file_label}” 的顶层列中找不到字段 ID 为 {field_id} "
            "的列。请确认该列在 Parquet 元数据中带有正确的 field id。",
            files=[file_label],
        )
    if len(matches) > 1:
        names = "、".join(column.name for column in matches)
        raise CheckError(
            f"{role} “{file_label}” 中字段 ID {field_id} 同时绑定了多个顶层列"
            f"（{names}），键列无法唯一绑定。",
            files=[file_label],
        )
    column = matches[0]
    if not (pa.types.is_int32(column.type) or pa.types.is_int64(column.type)):
        raise CheckError(
            f"{role} “{file_label}” 中字段 ID {field_id} 绑定的列 "
            f"“{column.name}” 类型为 {column.type}，只接受顶层 int32 或 "
            "int64 列。",
            files=[file_label],
        )
    return BoundKey(
        field_id=field_id, column_name=column.name, column_type=column.type
    )


def _open_parquet(path: Path, label: str, *, role: str) -> pq.ParquetFile:
    try:
        return pq.ParquetFile(str(path))
    except Exception as exc:
        raise CheckError(
            f"无法读取{role} “{label}”：{exc}",
            files=[label],
        ) from None


def _bind_file(
    path: Path,
    label: str,
    *,
    role: str,
    ticket_field_id: int,
    channel_field_id: int,
) -> tuple[pq.ParquetFile, BoundKey, BoundKey]:
    parquet = _open_parquet(path, label, role=role)
    try:
        schema = parquet.schema_arrow
    except Exception as exc:
        raise CheckError(
            f"无法读取{role} “{label}” 的 schema：{exc}",
            files=[label],
        ) from None
    ticket = _bind_key_column(
        schema, ticket_field_id, file_label=label, role=role
    )
    channel = _bind_key_column(
        schema, channel_field_id, file_label=label, role=role
    )
    if ticket.column_name == channel.column_name:
        raise CheckError(
            f"{role} “{label}” 中票号字段 ID {ticket_field_id} 与渠道字段 ID "
            f"{channel_field_id} 绑定到了同一列 “{ticket.column_name}”，"
            "两个键必须是不同的列。",
            files=[label],
        )
    return parquet, ticket, channel


# ---------------------------------------------------------------------------
# Manifest / output helpers
# ---------------------------------------------------------------------------


def _converted_names(equality_entry: ManifestEntry) -> tuple[str, str]:
    stem = Path(equality_entry.uploaded_filename).stem
    if stem.startswith("delete-eq"):
        new_stem = "delete-pos" + stem[len("delete-eq"):]
    else:
        new_stem = f"delete-pos-{stem}"
    output_filename = f"{new_stem}.parquet"
    # Keep URI-style original paths verbatim: pathlib would collapse
    # "s3://" into "s3:/" on POSIX.
    original = equality_entry.original_path
    if "/" in original:
        parent = original.rsplit("/", 1)[0]
        output_original_path = f"{parent}/{output_filename}"
    else:
        output_original_path = output_filename
    return output_filename, output_original_path


def render_updated_manifest(
    manifest: Manifest,
    equality_entry: ManifestEntry,
    output_filename: str,
    output_original_path: str,
) -> str:
    """CSV manifest with the equality entry replaced by a positional one."""
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(
        [
            "original_path",
            "uploaded_filename",
            "file_type",
            "data_sequence_number",
        ]
    )
    for entry in manifest.entries:
        if entry is equality_entry:
            writer.writerow(
                [
                    output_original_path,
                    output_filename,
                    FileKind.POSITION_DELETE.value,
                    entry.sequence_number,
                ]
            )
        else:
            writer.writerow(
                [
                    entry.original_path,
                    entry.uploaded_filename,
                    entry.kind.value,
                    entry.sequence_number,
                ]
            )
    return buffer.getvalue()


# ---------------------------------------------------------------------------
# Streaming conversion
# ---------------------------------------------------------------------------


def _load_equality_keys(
    parquet: pq.ParquetFile,
    ticket: BoundKey,
    channel: BoundKey,
    *,
    label: str,
    connection: sqlite3.Connection,
    batch_size: int,
) -> tuple[int, int]:
    """Stream equality delete rows into the on-disk key table."""
    rows_read = 0
    try:
        batches = parquet.iter_batches(
            batch_size=batch_size, columns=[ticket.column_name, channel.column_name]
        )
        for batch in batches:
            tickets = batch.column(ticket.column_name).to_pylist()
            channels = batch.column(channel.column_name).to_pylist()
            rows = list(zip(tickets, channels))
            rows_read += len(rows)
            connection.executemany(
                """
                INSERT INTO delete_keys(ticket_no, channel)
                SELECT ?, ?
                WHERE NOT EXISTS (
                    SELECT 1 FROM delete_keys d
                    WHERE d.ticket_no IS ? AND d.channel IS ?
                )
                """,
                [(t, c, t, c) for t, c in rows],
            )
            connection.commit()
    except CheckError:
        raise
    except Exception as exc:
        raise CheckError(
            f"读取等值删除文件 “{label}” 的键列失败：{exc}",
            files=[label],
        ) from None
    distinct = connection.execute(
        "SELECT count(*) FROM delete_keys"
    ).fetchone()[0]
    return rows_read, int(distinct)


def _convert_one_data_file(
    *,
    entry: ManifestEntry,
    path: Path,
    ticket: BoundKey,
    channel: BoundKey,
    connection: sqlite3.Connection,
    writer: pq.ParquetWriter,
    batch_size: int,
    counters: dict[str, int],
) -> int:
    label = entry.uploaded_filename
    parquet = _open_parquet(path, label, role="数据文件")
    hits = 0
    try:
        row_number = 0
        batches = parquet.iter_batches(
            batch_size=batch_size, columns=[ticket.column_name, channel.column_name]
        )
        for batch in batches:
            tickets = batch.column(ticket.column_name).to_pylist()
            channels = batch.column(channel.column_name).to_pylist()
            rows = list(zip(tickets, channels))
            connection.execute("DELETE FROM data_batch")
            connection.executemany(
                "INSERT INTO data_batch(row_no, ticket_no, channel) "
                "VALUES (?, ?, ?)",
                [
                    (row_number + offset, value[0], value[1])
                    for offset, value in enumerate(rows)
                ],
            )
            connection.commit()
            matched = connection.execute(
                """
                SELECT b.row_no
                FROM data_batch b
                JOIN delete_keys d
                  ON d.ticket_no IS b.ticket_no
                 AND d.channel IS b.channel
                ORDER BY b.row_no
                """
            ).fetchall()
            if matched:
                positions = [int(row_no) for (row_no,) in matched]
                table = pa.table(
                    {
                        DELETE_PATH_COLUMN: [entry.original_path]
                        * len(positions),
                        DELETE_POS_COLUMN: pa.array(
                            positions, type=pa.int64()
                        ),
                    },
                    schema=_POSITIONAL_DELETE_SCHEMA,
                )
                writer.write_table(table)
                hits += len(positions)
                counters["written"] += len(positions)
            row_number += len(rows)
    except CheckError:
        raise
    except Exception as exc:
        raise CheckError(
            f"读取或转换数据文件 “{label}”（原始路径 {entry.original_path}）"
            f"失败：{exc}",
            files=[label],
        ) from None
    return hits


def run_conversion(
    manifest: Manifest,
    uploaded_paths: Mapping[str, Path],
    ticket_field_id: int,
    channel_field_id: int,
    work_dir: Path,
    *,
    batch_size: int = 10_000,
) -> ConversionResult:
    """Convert the manifest's single equality delete to a positional delete.

    ``work_dir`` receives the new positional Parquet and a temporary SQLite
    spill database.  The updated manifest text is produced separately via
    :func:`render_updated_manifest`.
    """
    if ticket_field_id == channel_field_id:
        raise CheckError(
            "票号字段 ID 与渠道字段 ID 不能相同。"
        )
    if batch_size <= 0:
        raise CheckError("批大小必须是正整数。")
    if not manifest.data_files:
        raise CheckError("清单中没有 data 类型的数据文件，无法转换。")

    equality_entries = manifest.equality_delete_files
    if not equality_entries:
        raise CheckError(
            "清单中没有 equality_delete 条目，没有可转换的等值删除文件。"
        )
    if len(equality_entries) > 1:
        raise CheckError(
            "本入口只接受唯一的等值删除 Parquet；清单中出现了 "
            f"{len(equality_entries)} 个 equality_delete 条目。",
            files=[entry.uploaded_filename for entry in equality_entries],
        )
    equality_entry = equality_entries[0]

    # All data replicas and the single equality delete replica are required.
    required = [equality_entry] + manifest.data_files
    missing = [
        entry.uploaded_filename
        for entry in required
        if not Path(uploaded_paths.get(entry.uploaded_filename, "")).is_file()
    ]
    if missing:
        raise CheckError(
            "以下必需文件的上传副本缺失，无法转换。",
            files=missing,
        )

    referenced = {entry.uploaded_filename for entry in manifest.entries}
    unused_uploads = sorted(
        name for name in uploaded_paths if name not in referenced
    )

    # Bind key columns by field id in every file first: any ambiguity aborts
    # before anything is written.
    eq_parquet, eq_ticket, eq_channel = _bind_file(
        uploaded_paths[equality_entry.uploaded_filename],
        equality_entry.uploaded_filename,
        role="等值删除文件",
        ticket_field_id=ticket_field_id,
        channel_field_id=channel_field_id,
    )

    data_bindings: list[
        tuple[ManifestEntry, pq.ParquetFile, BoundKey, BoundKey]
    ] = []
    ticket_names: set[str] = set()
    channel_names: dict[str, str] = {}
    for entry in manifest.data_files:
        label = entry.uploaded_filename
        parquet, ticket, channel = _bind_file(
            uploaded_paths[label],
            label,
            role="数据文件",
            ticket_field_id=ticket_field_id,
            channel_field_id=channel_field_id,
        )
        ticket_names.add(ticket.column_name)
        channel_names[label] = channel.column_name
        data_bindings.append((entry, parquet, ticket, channel))

    if len(ticket_names) > 1:
        raise CheckError(
            "各数据副本中票号字段 ID "
            f"{ticket_field_id} 绑定到的列名不一致（"
            + "、".join(sorted(ticket_names))
            + "）。数据副本的票号列必须同名。"
        )

    output_filename, output_original_path = _converted_names(equality_entry)
    output_path = work_dir / output_filename
    if output_path.exists():
        raise CheckError(
            f"转换输出文件名 “{output_filename}” 与上传文件冲突。",
            files=[output_filename],
        )

    db_path = work_dir / "conversion-spill.sqlite3"
    connection = sqlite3.connect(str(db_path))
    # Spill storage: the DB is deleted with the request's temp directory.
    connection.execute("PRAGMA journal_mode = MEMORY")
    connection.execute("PRAGMA synchronous = OFF")
    connection.execute("PRAGMA temp_store = MEMORY")
    writer: pq.ParquetWriter | None = None
    try:
        connection.execute(
            "CREATE TABLE delete_keys(ticket_no INTEGER, channel INTEGER)"
        )
        connection.execute(
            "CREATE INDEX delete_keys_lookup "
            "ON delete_keys(ticket_no, channel)"
        )
        connection.execute(
            "CREATE TABLE data_batch("
            "row_no INTEGER PRIMARY KEY, ticket_no INTEGER, channel INTEGER)"
        )
        connection.commit()

        delete_rows_read, distinct_keys = _load_equality_keys(
            eq_parquet,
            eq_ticket,
            eq_channel,
            label=equality_entry.uploaded_filename,
            connection=connection,
            batch_size=batch_size,
        )

        writer = pq.ParquetWriter(
            str(output_path),
            _POSITIONAL_DELETE_SCHEMA,
            compression="snappy",
        )
        counters = {"written": 0}
        matched_rows: dict[str, int] = {}
        skipped: list[SkippedDataFile] = []

        for entry, _parquet, ticket, channel in data_bindings:
            if entry.sequence_number >= equality_entry.sequence_number:
                skipped.append(
                    SkippedDataFile(
                        entry=entry,
                        reason=(
                            "数据序列号 "
                            f"{entry.sequence_number} 不小于等值删除序列号 "
                            f"{equality_entry.sequence_number}，"
                            "仅当删除序列号严格大于数据序列号时才写出位置删除"
                        ),
                    )
                )
                continue
            hits = _convert_one_data_file(
                entry=entry,
                path=uploaded_paths[entry.uploaded_filename],
                ticket=ticket,
                channel=channel,
                connection=connection,
                writer=writer,
                batch_size=batch_size,
                counters=counters,
            )
            matched_rows[entry.uploaded_filename] = hits
    except CheckError:
        if writer is not None:
            try:
                writer.close()
            except Exception:
                pass
        connection.close()
        if output_path.exists():
            output_path.unlink()
        if db_path.exists():
            db_path.unlink()
        raise
    except Exception as exc:
        if writer is not None:
            try:
                writer.close()
            except Exception:
                pass
        connection.close()
        if output_path.exists():
            output_path.unlink()
        if db_path.exists():
            db_path.unlink()
        raise CheckError(
            f"转换过程中发生写入失败：{exc}",
            files=[output_filename],
        ) from None
    else:
        writer.close()
        connection.close()
        if db_path.exists():
            db_path.unlink()

    warnings: list[str] = []
    if unused_uploads:
        warnings.append(
            "以下上传文件未出现在清单中，已忽略："
            + "、".join(unused_uploads)
        )
    if counters["written"] == 0:
        warnings.append(
            "没有任何数据行命中等值删除键，输出的位置删除 Parquet 不包含"
            "删除记录（文件仍按约定生成，可与清单一并交付）。"
        )

    return ConversionResult(
        manifest=manifest,
        ticket_field_id=ticket_field_id,
        channel_field_id=channel_field_id,
        equality_entry=equality_entry,
        output_filename=output_filename,
        output_original_path=output_original_path,
        output_manifest_name="manifest.csv",
        batch_size=batch_size,
        delete_rows_read=delete_rows_read,
        distinct_delete_keys=distinct_keys,
        duplicate_delete_rows=delete_rows_read - distinct_keys,
        positional_rows_written=counters["written"],
        matched_data_rows=matched_rows,
        skipped_data_files=skipped,
        channel_name_mismatch=len(set(channel_names.values())) > 1
        or (
            eq_channel.column_name
            not in set(channel_names.values())
        ),
        bound_channels={
            equality_entry.uploaded_filename: eq_channel.column_name,
            **channel_names,
        },
        unused_uploads=unused_uploads,
        warnings=warnings,
    )


def build_conversion_zip(
    result: ConversionResult,
    output_parquet: Path,
    work_dir: Path,
) -> Path:
    """Bundle the new positional Parquet and the updated CSV manifest."""
    import zipfile

    manifest_text = render_updated_manifest(
        result.manifest,
        result.equality_entry,
        result.output_filename,
        result.output_original_path,
    )
    zip_path = work_dir / "converted-position-delete.zip"
    try:
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.write(output_parquet, arcname=result.output_filename)
            archive.writestr(result.output_manifest_name, manifest_text)
    except Exception as exc:
        raise CheckError(
            f"打包转换结果失败：{exc}",
            files=[result.output_filename],
        ) from None
    return zip_path
