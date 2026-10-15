/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

/// A Thrift struct a reader in this package can attribute a failure to, with the
/// names `parquet.thrift` gives its fields.
///
/// A struct appears here only if some reader attributes failures to it, and must then
/// list every field id the format gives it — including ids no reader reads, which
/// arrive by way of `skipField`. A list that stops early does not merely lose a name:
/// [#describe] falls through to its second form and asserts the format has no such
/// field, which for a field the format does define is worse than saying nothing.
/// `ThriftStructOracleTest` walks [#values] and checks every entry against
/// parquet-format's own metadata.
///
/// For a union the names are its member names, which are what the ids identify; the
/// members' own fields belong to the member struct, not to the union.
enum ThriftStruct {

    BLOOM_FILTER_ALGORITHM("BloomFilterAlgorithm",
            "BLOCK"),
    BLOOM_FILTER_COMPRESSION("BloomFilterCompression",
            "UNCOMPRESSED"),
    BLOOM_FILTER_HASH("BloomFilterHash",
            "XXHASH"),
    BLOOM_FILTER_HEADER("BloomFilterHeader",
            "numBytes",
            "algorithm",
            "hash",
            "compression"),
    BOUNDING_BOX("BoundingBox",
            "xmin",
            "xmax",
            "ymin",
            "ymax",
            "zmin",
            "zmax",
            "mmin",
            "mmax"),
    COLUMN_CHUNK("ColumnChunk",
            "file_path",
            "file_offset",
            "meta_data",
            "offset_index_offset",
            "offset_index_length",
            "column_index_offset",
            "column_index_length",
            "crypto_metadata",
            "encrypted_column_metadata"),
    COLUMN_INDEX("ColumnIndex",
            "null_pages",
            "min_values",
            "max_values",
            "boundary_order",
            "null_counts",
            "repetition_level_histograms",
            "definition_level_histograms",
            // parquet.thrift does not define id 8, but ColumnIndexReader reads it,
            // so a failure over it is on a field this reader does recognise.
            "nan_counts"),
    COLUMN_META_DATA("ColumnMetaData",
            "type",
            "encodings",
            "path_in_schema",
            "codec",
            "num_values",
            "total_uncompressed_size",
            "total_compressed_size",
            "key_value_metadata",
            "data_page_offset",
            "index_page_offset",
            "dictionary_page_offset",
            "statistics",
            "encoding_stats",
            "bloom_filter_offset",
            "bloom_filter_length",
            "size_statistics",
            "geospatial_statistics"),
    COLUMN_ORDER("ColumnOrder",
            "TYPE_ORDER",
            "IEEE_754_TOTAL_ORDER",
            "INT96_TIMESTAMP_ORDER"),
    DATA_PAGE_HEADER("DataPageHeader",
            "num_values",
            "encoding",
            "definition_level_encoding",
            "repetition_level_encoding",
            "statistics"),
    DATA_PAGE_HEADER_V2("DataPageHeaderV2",
            "num_values",
            "num_nulls",
            "num_rows",
            "encoding",
            "definition_levels_byte_length",
            "repetition_levels_byte_length",
            "is_compressed",
            "statistics"),
    DECIMAL_TYPE("DecimalType",
            "scale",
            "precision"),
    DICTIONARY_PAGE_HEADER("DictionaryPageHeader",
            "num_values",
            "encoding",
            "is_sorted"),
    FILE_META_DATA("FileMetaData",
            "version",
            "schema",
            "num_rows",
            "row_groups",
            "key_value_metadata",
            "created_by",
            "column_orders",
            "encryption_algorithm",
            "footer_signing_key_metadata"),
    GEOGRAPHY_TYPE("GeographyType",
            "crs",
            "algorithm"),
    GEOMETRY_TYPE("GeometryType",
            "crs"),
    GEOSPATIAL_STATISTICS("GeospatialStatistics",
            "bbox",
            "geospatial_types"),
    INT_TYPE("IntType",
            "bitWidth",
            "isSigned"),
    KEY_VALUE("KeyValue",
            "key",
            "value"),
    LOGICAL_TYPE("LogicalType",
            "STRING",
            "MAP",
            "LIST",
            "ENUM",
            "DECIMAL",
            "DATE",
            "TIME",
            "TIMESTAMP",
            // Reserved for INTERVAL, never defined.
            null,
            "INTEGER",
            "UNKNOWN",
            "JSON",
            "BSON",
            "UUID",
            "FLOAT16",
            "VARIANT",
            "GEOMETRY",
            "GEOGRAPHY",
            "FILE"),
    OFFSET_INDEX("OffsetIndex",
            "page_locations",
            "unencoded_byte_array_data_bytes"),
    PAGE_ENCODING_STATS("PageEncodingStats",
            "page_type",
            "encoding",
            "count"),
    PAGE_HEADER("PageHeader",
            "type",
            "uncompressed_page_size",
            "compressed_page_size",
            "crc",
            "data_page_header",
            "index_page_header",
            "dictionary_page_header",
            "data_page_header_v2"),
    PAGE_LOCATION("PageLocation",
            "offset",
            "compressed_page_size",
            "first_row_index"),
    ROW_GROUP("RowGroup",
            "columns",
            "total_byte_size",
            "num_rows",
            "sorting_columns",
            "file_offset",
            "total_compressed_size",
            "ordinal"),
    SCHEMA_ELEMENT("SchemaElement",
            "type",
            "type_length",
            "repetition_type",
            "name",
            "num_children",
            "converted_type",
            "scale",
            "precision",
            "field_id",
            "logicalType"),
    SIZE_STATISTICS("SizeStatistics",
            "unencoded_byte_array_data_bytes",
            "repetition_level_histogram",
            "definition_level_histogram"),
    STATISTICS("Statistics",
            "max",
            "min",
            "null_count",
            "distinct_count",
            "max_value",
            "min_value",
            "is_max_value_exact",
            "is_min_value_exact",
            // Read by StatisticsReader, on the same footing as ColumnIndex.nan_counts.
            "nan_count"),
    TIME_TYPE("TimeType",
            "isAdjustedToUTC",
            "unit"),
    TIME_UNIT("TimeUnit",
            "MILLIS",
            "MICROS",
            "NANOS"),
    TIMESTAMP_TYPE("TimestampType",
            "isAdjustedToUTC",
            "unit"),
    VARIANT_TYPE("VariantType",
            "specification_version");

    /// Held because [ThriftCompactReader#popFieldIdContext] resolves an ordinal back to a
    /// constant once per struct on the page-header path, and `values()` clones its array
    /// on every call.
    private static final ThriftStruct[] VALUES = values();

    private final String structName;
    private final String[] fieldNames;

    /// @param structName the struct's name in `parquet.thrift`, which is also the name
    ///                   of the class parquet-format generates for it
    /// @param fieldNames its field names in id order, ids running from 1
    ThriftStruct(String structName, String... fieldNames) {
        this.structName = structName;
        this.fieldNames = fieldNames;
    }

    /// The struct's name in `parquet.thrift`.
    String structName() {
        return structName;
    }

    /// `Struct.field` for a field the struct defines, or `Struct field N` when it
    /// does not.
    ///
    /// The second form is worth saying rather than hiding: a field id the struct has no
    /// room for means the bytes did not decode to anything the format allows there,
    /// which is a stronger statement than an unexpected type on a field that does exist.
    String describe(int fieldId) {
        String name = fieldName(fieldId);
        return name != null ? structName + "." + name : structName + " field " + fieldId;
    }

    /// What the format calls field `fieldId`, or `null` if it defines no such field.
    String fieldName(int fieldId) {
        return fieldId >= 1 && fieldId <= fieldNames.length ? fieldNames[fieldId - 1] : null;
    }

    /// The constant at `ordinal`, without the defensive copy [#values] makes.
    static ThriftStruct at(int ordinal) {
        return VALUES[ordinal];
    }
}
