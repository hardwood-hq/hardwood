#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

"""
Post-processes a Parquet file to tag a chosen *group* SchemaElement with the
Variant logical type in the Thrift footer. PyArrow cannot emit
`VARIANT(specification_version=1)` natively (no `pa.variant()` yet as of
23.0.1), so this helper reads the compact-Thrift footer, stamps
`LogicalType.VARIANT(VariantType(specification_version=1))` onto the named
group, and rewrites the file. The rest of the file (data pages, row groups,
statistics, etc.) is preserved byte-for-byte.

Only the subset of parquet.thrift that PyArrow emits, plus the LogicalType
union definition, is modelled here; this matches `parquet_bson_annotation.py`.
"""

import io
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
struct PageEncodingStats { 1: required PageType page_type; 2: required Encoding encoding; 3: required i32 count; }
struct BsonType2 {}
struct SizeStatistics {
  1: optional i64 unencoded_byte_array_data_bytes;
  2: optional list<i64> repetition_level_histogram;
  3: optional list<i64> definition_level_histogram;
}
struct GeospatialStatistics { 1: optional list<double> bbox; 2: optional list<i32> geospatial_types; }
struct EncodingStats { 1: required PageType page_type; 2: required Encoding encoding; 3: required i32 count; }
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

_parquet = thriftpy2.load_fp(io.StringIO(_PARQUET_IDL), module_name="_parquet_variant_patch_thrift")


def annotate_group_as_variant(path: str, group_name: str, spec_version: int = 1) -> None:
    """Rewrite `path` so that the named *group* SchemaElement carries the
    Variant logical type annotation (`LogicalType.VARIANT(specification_version=spec_version)`).

    The group must already exist in the schema with the expected children
    (`metadata`, `value`, and optionally `typed_value`); this helper only
    stamps the annotation — it does not restructure the schema.
    """
    with open(path, 'rb') as f:
        raw = f.read()

    if raw[-4:] != b'PAR1':
        raise ValueError(f"{path} is not a Parquet file (missing PAR1 magic)")
    footer_len = struct.unpack('<I', raw[-8:-4])[0]
    footer_start = len(raw) - 8 - footer_len
    footer_bytes = raw[footer_start:-8]
    data_before_footer = raw[:footer_start]

    proto = TCompactProtocolFactory().get_protocol(TMemoryBuffer(footer_bytes))
    md = _parquet.FileMetaData()
    md.read(proto)

    matched = False
    for el in md.schema:
        if el.name == group_name and el.num_children is not None:
            el.logicalType = _parquet.LogicalType(
                VARIANT=_parquet.VariantType(specification_version=spec_version))
            matched = True
            break
    if not matched:
        raise ValueError(f"Group {group_name!r} not found in {path}")

    out = TMemoryBuffer()
    md.write(TCompactProtocolFactory().get_protocol(out))
    new_footer = out.getvalue()

    with open(path, 'wb') as f:
        f.write(data_before_footer)
        f.write(new_footer)
        f.write(struct.pack('<I', len(new_footer)))
        f.write(b'PAR1')
