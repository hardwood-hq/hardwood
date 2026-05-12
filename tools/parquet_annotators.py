#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

"""
Post-processes a Parquet file to stamp logical type annotations into the Thrift
footer. PyArrow cannot emit certain logical types natively, so these helpers
read the compact-Thrift footer, patch the target SchemaElement, and rewrite the
file. The rest of the file (data pages, row groups, statistics, etc.) is
preserved byte-for-byte.

Supported annotations:
- BSON       — `annotate_column_as_bson`
- FLOAT16    — `annotate_element_at_path_as_float16`
- INTERVAL   — `annotate_column_as_interval`
- JSON       — `annotate_element_at_path_as_json`
- VARIANT    — `annotate_group_as_variant`

Modern-only fixture helpers (strip legacy annotations so only `logicalType` remains):
- `strip_converted_type` — for LIST / MAP outer groups.

Only the subset of parquet.thrift that PyArrow emits, plus the LogicalType
union, is modelled in the embedded IDL.
"""

import io
import shutil
import struct

import thriftpy2
from thriftpy2.protocol.compact import TCompactProtocolFactory
from thriftpy2.transport import TMemoryBuffer

_PARQUET_IDL = r"""
enum FieldRepetitionType { REQUIRED = 0, OPTIONAL = 1, REPEATED = 2 }
enum Type { BOOLEAN=0, INT32=1, INT64=2, INT96=3, FLOAT=4, DOUBLE=5, BYTE_ARRAY=6, FIXED_LEN_BYTE_ARRAY=7 }
enum ConvertedType {
  UTF8=0, MAP=1, MAP_KEY_VALUE=2, LIST=3, ENUM=4, DECIMAL=5, DATE=6,
  TIME_MILLIS=7, TIME_MICROS=8, TIMESTAMP_MILLIS=9, TIMESTAMP_MICROS=10,
  UINT_8=11, UINT_16=12, UINT_32=13, UINT_64=14,
  INT_8=15, INT_16=16, INT_32=17, INT_64=18,
  JSON=19, BSON=20, INTERVAL=21
}
struct StringType {}
struct UUIDType {}
struct MapType {}
struct ListType {}
struct EnumType {}
struct DateType {}
struct IntervalType {}
struct Float16Type {}
struct NullType {}
struct DecimalType { 1: required i32 scale; 2: required i32 precision; }
struct MilliSeconds {}
struct MicroSeconds {}
struct NanoSeconds {}
union TimeUnit { 1: MilliSeconds MILLIS; 2: MicroSeconds MICROS; 3: NanoSeconds NANOS; }
struct TimestampType { 1: required bool isAdjustedToUTC; 2: required TimeUnit unit; }
struct TimeType { 1: required bool isAdjustedToUTC; 2: required TimeUnit unit; }
struct IntType { 1: required byte bitWidth; 2: required bool isSigned; }
struct JsonType {}
struct BsonType {}
struct VariantType { 1: optional byte specification_version; }
struct GeometryType {}
struct GeographyType {}
union LogicalType {
  1:  StringType STRING
  2:  MapType MAP
  3:  ListType LIST
  4:  EnumType ENUM
  5:  DecimalType DECIMAL
  6:  DateType DATE
  7:  TimeType TIME
  8:  TimestampType TIMESTAMP
  9:  IntervalType INTERVAL
  10: IntType INTEGER
  11: NullType UNKNOWN
  12: JsonType JSON
  13: BsonType BSON
  14: UUIDType UUID
  15: Float16Type FLOAT16
  16: VariantType VARIANT
  17: GeometryType GEOMETRY
  18: GeographyType GEOGRAPHY
}
struct SchemaElement {
  1: optional Type type;
  2: optional i32 type_length;
  3: optional FieldRepetitionType repetition_type;
  4: required string name;
  5: optional i32 num_children;
  6: optional ConvertedType converted_type;
  7: optional i32 scale;
  8: optional i32 precision;
  9: optional i32 field_id;
  10: optional LogicalType logicalType;
}
struct KeyValue { 1: required string key; 2: optional string value; }
enum Encoding {
  PLAIN=0, PLAIN_DICTIONARY=2, RLE=3, BIT_PACKED=4, DELTA_BINARY_PACKED=5,
  DELTA_LENGTH_BYTE_ARRAY=6, DELTA_BYTE_ARRAY=7, RLE_DICTIONARY=8, BYTE_STREAM_SPLIT=9
}
enum CompressionCodec {
  UNCOMPRESSED=0, SNAPPY=1, GZIP=2, LZO=3, BROTLI=4, LZ4=5, ZSTD=6, LZ4_RAW=7
}
enum PageType { DATA_PAGE=0, INDEX_PAGE=1, DICTIONARY_PAGE=2, DATA_PAGE_V2=3 }
enum BoundaryOrder { UNORDERED=0, ASCENDING=1, DESCENDING=2 }
struct Statistics {
  1: optional binary max;
  2: optional binary min;
  3: optional i64 null_count;
  4: optional i64 distinct_count;
  5: optional binary max_value;
  6: optional binary min_value;
  7: optional bool is_max_value_exact;
  8: optional bool is_min_value_exact;
}
struct SizeStatistics {
  1: optional i64 unencoded_byte_array_data_bytes;
  2: optional list<i64> repetition_level_histogram;
  3: optional list<i64> definition_level_histogram;
}
struct EncodingStats { 1: required PageType page_type; 2: required Encoding encoding; 3: required i32 count; }
struct PageEncodingStats { 1: required PageType page_type; 2: required Encoding encoding; 3: required i32 count; }
struct SplitBlockAlgorithm {}
union BloomFilterAlgorithm { 1: SplitBlockAlgorithm BLOCK; }
struct XxHash {}
union BloomFilterHash { 1: XxHash XXHASH; }
struct Uncompressed {}
union BloomFilterCompression { 1: Uncompressed UNCOMPRESSED; }
struct BloomFilterHeader {
  1: required i32 numBytes;
  2: required BloomFilterAlgorithm algorithm;
  3: required BloomFilterHash hash;
  4: required BloomFilterCompression compression;
}
struct BoundingBox {
  1: required double xmin; 2: required double xmax;
  3: required double ymin; 4: required double ymax;
  5: optional double zmin; 6: optional double zmax;
  7: optional double mmin; 8: optional double mmax;
}
struct GeospatialStatistics {
  1: optional BoundingBox bbox;
  2: optional list<i32> geospatial_types;
}
struct ColumnCryptoMetaData {}
struct ColumnMetaData {
  1: required Type type;
  2: required list<Encoding> encodings;
  3: required list<string> path_in_schema;
  4: required CompressionCodec codec;
  5: required i64 num_values;
  6: required i64 total_uncompressed_size;
  7: required i64 total_compressed_size;
  8: optional list<KeyValue> key_value_metadata;
  9: required i64 data_page_offset;
  10: optional i64 index_page_offset;
  11: optional i64 dictionary_page_offset;
  12: optional Statistics statistics;
  13: optional list<EncodingStats> encoding_stats;
  14: optional i64 bloom_filter_offset;
  15: optional i32 bloom_filter_length;
  16: optional SizeStatistics size_statistics;
  17: optional GeospatialStatistics geospatial_statistics;
}
struct ColumnChunk {
  1: optional string file_path;
  2: required i64 file_offset;
  3: optional ColumnMetaData meta_data;
  4: optional i64 offset_index_offset;
  5: optional i32 offset_index_length;
  6: optional i64 column_index_offset;
  7: optional i32 column_index_length;
  8: optional ColumnCryptoMetaData crypto_metadata;
  9: optional binary encrypted_column_metadata;
}
struct SortingColumn { 1: required i32 column_idx; 2: required bool descending; 3: required bool nulls_first; }
struct RowGroup {
  1: required list<ColumnChunk> columns;
  2: required i64 total_byte_size;
  3: required i64 num_rows;
  4: optional list<SortingColumn> sorting_columns;
  5: optional i64 file_offset;
  6: optional i64 total_compressed_size;
  7: optional i16 ordinal;
}
struct TypeDefinedOrder {}
union ColumnOrder { 1: TypeDefinedOrder TYPE_ORDER }
struct EncryptionAlgorithm {}
struct FileMetaData {
  1: required i32 version;
  2: required list<SchemaElement> schema;
  3: required i64 num_rows;
  4: required list<RowGroup> row_groups;
  5: optional list<KeyValue> key_value_metadata;
  6: optional string created_by;
  7: optional list<ColumnOrder> column_orders;
  8: optional EncryptionAlgorithm encryption_algorithm;
  9: optional binary footer_signing_key_metadata;
}
"""

_parquet = thriftpy2.load_fp(io.StringIO(_PARQUET_IDL), module_name="_parquet_patch_thrift")

def _read_parquet_footer(path: str):
    """Read and parse a Parquet file's Thrift footer.

    Returns a `(data_before_footer, file_metadata)` tuple where
    `data_before_footer` is the raw bytes preceding the footer and
    `file_metadata` is the parsed `FileMetaData`.
    """
    with open(path, 'rb') as f:
        raw = f.read()

    if raw[-4:] != b'PAR1':
        raise ValueError(f"{path} is not a Parquet file (missing PAR1 magic)")
    footer_len = struct.unpack('<I', raw[-8:-4])[0]
    footer_start = len(raw) - 8 - footer_len
    footer_bytes = raw[footer_start:-8]

    proto = TCompactProtocolFactory().get_protocol(TMemoryBuffer(footer_bytes))
    file_metadata = _parquet.FileMetaData()
    file_metadata.read(proto)

    return raw[:footer_start], file_metadata


def _write_parquet_footer(path: str, data_before_footer: bytes, file_metadata) -> None:
    """Re-serialize `file_metadata` and write the Parquet file back to `path`."""
    out = TMemoryBuffer()
    file_metadata.write(TCompactProtocolFactory().get_protocol(out))
    new_footer = out.getvalue()

    with open(path, 'wb') as f:
        f.write(data_before_footer)
        f.write(new_footer)
        f.write(struct.pack('<I', len(new_footer)))
        f.write(b'PAR1')


def annotate_column_as_bson(path: str, column_name: str) -> None:
    """Rewrite `path` so that the named column carries the BSON logical type."""
    data_before_footer, file_metadata = _read_parquet_footer(path)

    matched = False
    for el in file_metadata.schema:
        if el.name == column_name:
            el.converted_type = _parquet.ConvertedType.BSON
            el.logicalType = _parquet.LogicalType(BSON=_parquet.BsonType())
            matched = True
            break
    if not matched:
        raise ValueError(f"Column {column_name!r} not found in {path}")

    _write_parquet_footer(path, data_before_footer, file_metadata)


def annotate_group_as_variant(path: str, group_name: str, spec_version: int = 1) -> None:
    """Rewrite `path` so that the named *group* SchemaElement carries the
    Variant logical type annotation (`LogicalType.VARIANT(specification_version=spec_version)`).

    The group must already exist in the schema with the expected children
    (`metadata`, `value`, and optionally `typed_value`); this helper only
    stamps the annotation — it does not restructure the schema.
    """
    data_before_footer, file_metadata = _read_parquet_footer(path)

    matched = False
    for el in file_metadata.schema:
        if el.name == group_name and el.num_children is not None:
            el.logicalType = _parquet.LogicalType(
                VARIANT=_parquet.VariantType(specification_version=spec_version))
            matched = True
            break
    if not matched:
        raise ValueError(f"Group {group_name!r} not found in {path}")

    _write_parquet_footer(path, data_before_footer, file_metadata)


def _find_schema_element_by_path(file_metadata, name_path):
    """Find a SchemaElement in `file_metadata.schema` by hierarchical name path.

    The schema is a depth-first flat list; each non-leaf carries `num_children`.
    `name_path` is a list like `['intervals', 'list', 'element']`. Returns the
    matching SchemaElement, raising if no path matches uniquely.
    """
    elements = file_metadata.schema

    def _walk(start, depth):
        if depth == len(name_path):
            return start, None  # not used
        target = name_path[depth]
        i = start + 1  # children begin right after the parent
        children_remaining = elements[start].num_children or 0
        while children_remaining > 0:
            child = elements[i]
            if child.name == target:
                if depth == len(name_path) - 1:
                    return i, child
                return _walk(i, depth + 1)
            # skip subtree under `child`
            i = _skip_subtree(i)
            children_remaining -= 1
        raise ValueError(f"Path segment {target!r} not found at depth {depth}")

    def _skip_subtree(idx):
        nc = elements[idx].num_children or 0
        idx += 1
        for _ in range(nc):
            idx = _skip_subtree(idx)
        return idx

    _, found = _walk(0, 0)
    if found is None:
        raise ValueError(f"Path {name_path!r} did not resolve to a leaf")
    return found


def annotate_element_at_path_as_interval(path: str, name_path) -> None:
    """Annotate the SchemaElement at `name_path` (excluding the root) as INTERVAL."""
    data_before_footer, file_metadata = _read_parquet_footer(path)
    el = _find_schema_element_by_path(file_metadata, list(name_path))
    el.logicalType = _parquet.LogicalType(INTERVAL=_parquet.IntervalType())
    _write_parquet_footer(path, data_before_footer, file_metadata)


def annotate_element_at_path_as_time(path: str, name_path, *,
                                     unit: str = "MILLIS", is_adjusted_to_utc: bool = True) -> None:
    """Annotate the SchemaElement at `name_path` as TIME with the given unit."""
    data_before_footer, file_metadata = _read_parquet_footer(path)
    el = _find_schema_element_by_path(file_metadata, list(name_path))
    time_unit = {
        "MILLIS": _parquet.TimeUnit(MILLIS=_parquet.MilliSeconds()),
        "MICROS": _parquet.TimeUnit(MICROS=_parquet.MicroSeconds()),
        "NANOS": _parquet.TimeUnit(NANOS=_parquet.NanoSeconds()),
    }[unit]
    el.logicalType = _parquet.LogicalType(
        TIME=_parquet.TimeType(isAdjustedToUTC=is_adjusted_to_utc, unit=time_unit))
    if unit == "MILLIS":
        el.converted_type = _parquet.ConvertedType.TIME_MILLIS
    elif unit == "MICROS":
        el.converted_type = _parquet.ConvertedType.TIME_MICROS
    _write_parquet_footer(path, data_before_footer, file_metadata)


def annotate_element_at_path_as_json(path: str, name_path) -> None:
    """Annotate the SchemaElement at `name_path` as JSON (BYTE_ARRAY payload)."""
    data_before_footer, file_metadata = _read_parquet_footer(path)
    el = _find_schema_element_by_path(file_metadata, list(name_path))
    el.logicalType = _parquet.LogicalType(JSON=_parquet.JsonType())
    el.converted_type = _parquet.ConvertedType.JSON
    _write_parquet_footer(path, data_before_footer, file_metadata)


def annotate_element_at_path_as_float16(path: str, name_path) -> None:
    """Annotate the SchemaElement at `name_path` as FLOAT16 (FLBA(2) payload).

    PyArrow has no native FLOAT16 emitter; the underlying column must be
    written as `pa.binary(2)` and post-annotated here.
    """
    data_before_footer, file_metadata = _read_parquet_footer(path)
    el = _find_schema_element_by_path(file_metadata, list(name_path))
    el.logicalType = _parquet.LogicalType(FLOAT16=_parquet.Float16Type())
    _write_parquet_footer(path, data_before_footer, file_metadata)


def annotate_element_at_path_as_decimal(path: str, name_path, *,
                                        precision: int, scale: int) -> None:
    """Annotate the SchemaElement at `name_path` as DECIMAL(precision, scale)."""
    data_before_footer, file_metadata = _read_parquet_footer(path)
    el = _find_schema_element_by_path(file_metadata, list(name_path))
    el.logicalType = _parquet.LogicalType(
        DECIMAL=_parquet.DecimalType(scale=scale, precision=precision))
    el.converted_type = _parquet.ConvertedType.DECIMAL
    el.scale = scale
    el.precision = precision
    _write_parquet_footer(path, data_before_footer, file_metadata)


def annotate_column_as_interval(path: str, column_name: str, *, legacy_only: bool = False) -> None:
    """Rewrite `path` so that the named FIXED_LEN_BYTE_ARRAY(12) column carries the INTERVAL annotation.

    By default writes the modern `LogicalType.IntervalType` union (field 9). When
    `legacy_only=True`, writes only the legacy `converted_type=INTERVAL` (value 21)
    and clears any modern `logicalType` — simulating files from older writers
    (parquet-mr, Spark, Hive) that predate the LogicalType union.
    """
    data_before_footer, file_metadata = _read_parquet_footer(path)

    matched = False
    for el in file_metadata.schema:
        if el.name == column_name:
            if legacy_only:
                el.logicalType = None
                el.converted_type = 21  # ConvertedType.INTERVAL
            else:
                el.logicalType = _parquet.LogicalType(INTERVAL=_parquet.IntervalType())
            matched = True
            break
    if not matched:
        raise ValueError(f"Column {column_name!r} not found in {path}")

    _write_parquet_footer(path, data_before_footer, file_metadata)


def _find_outer_group(file_metadata, field_name: str):
    """Return the SchemaElement for the top-level group named `field_name`."""
    for el in file_metadata.schema:
        if el.name == field_name and el.num_children is not None:
            return el
    raise ValueError(f"Top-level group '{field_name}' not found in schema")


def strip_converted_type(src: str, dst: str, field_name: str) -> None:
    """Copy `src` to `dst`, removing `converted_type` from the outer group for `field_name`.

    The result carries only `logicalType` (modern-only annotation), which is the
    case a reader must handle when the writer omits the legacy annotation.

    PyArrow 24.0.0 (pinned in `requirements.txt`) does not emit
    `MAP_KEY_VALUE` on the inner `key_value` group, so for MAP fixtures only
    the outer group's `converted_type` needs clearing for the result to be
    fully modern-only end-to-end.
    """
    shutil.copy2(src, dst)
    data, md = _read_parquet_footer(dst)
    el = _find_outer_group(md, field_name)
    el.converted_type = None
    _write_parquet_footer(dst, data, md)
