/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import dev.hardwood.internal.thrift.ThriftCompactConstants.FieldType.Codes;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.GeospatialStatistics;
import dev.hardwood.metadata.PageEncodingStats;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.SizeStatistics;
import dev.hardwood.metadata.Statistics;

/// Reader for ColumnMetaData from Thrift Compact Protocol.
public class ColumnMetaDataReader {

    /// Stand-in for a `ColumnMetaData` that carries no `path_in_schema` at all.
    private static final FieldPath EMPTY_PATH = new FieldPath(List.of());

    /// Ids of the fields the format requires of a `ColumnMetaData`.
    private static final int[] REQUIRED_FIELDS = { 1, 2, 3, 4, 5, 6, 7, 9 };

    public static ColumnMetaData read(ThriftCompactReader reader) {
        int saved = reader.pushFieldIdContext(ThriftStruct.COLUMN_META_DATA);
        try {
            return readInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static ColumnMetaData readInternal(ThriftCompactReader reader) {
        PhysicalType type = null;
        List<Encoding> encodings = Collections.emptyList();
        FieldPath pathInSchema = EMPTY_PATH;
        CompressionCodec codec = null;
        long numValues = 0;
        long totalUncompressedSize = 0;
        long totalCompressedSize = 0;
        Map<String, String> keyValueMetadata = Collections.emptyMap();
        long dataPageOffset = 0;
        Long dictionaryPageOffset = null;
        Statistics statistics = null;
        GeospatialStatistics geospatialStatistics = null;
        Long bloomFilterOffset = null;
        Integer bloomFilterLength = null;
        List<PageEncodingStats> encodingStats = List.of();
        SizeStatistics sizeStatistics = null;
        long seen = 0;

        while (true) {
            int header = reader.readFieldHeader();
            if (header == ThriftCompactReader.STOP_FIELD) {
                break;
            }

            int fieldId = ThriftCompactReader.fieldId(header);
            switch (fieldId) {
                // Fields 1-7 and 9 are required, so a wrong wire type fails here rather than
                // being reported as a field that never arrived.
                case 1: // type (required Type)
                    reader.requireField(header, Codes.I32);
                    type = ThriftEnumLookup.physicalType(reader.readI32());
                    seen |= 1L << fieldId;
                    break;
                case 2: // encodings (required list<Encoding>)
                    reader.requireField(header, Codes.LIST);
                    encodings = readEncodings(reader);
                    seen |= 1L << fieldId;
                    break;
                case 3: // path_in_schema (required list<string>)
                    reader.requireField(header, Codes.LIST);
                    pathInSchema = reader.pathCache().next(reader);
                    seen |= 1L << fieldId;
                    break;
                case 4: // codec (required CompressionCodec)
                    reader.requireField(header, Codes.I32);
                    codec = ThriftEnumLookup.compressionCodec(reader.readI32());
                    seen |= 1L << fieldId;
                    break;
                case 5: // num_values (required i64)
                    reader.requireField(header, Codes.I64);
                    numValues = reader.readNonNegativeI64();
                    seen |= 1L << fieldId;
                    break;
                case 6: // total_uncompressed_size (required i64)
                    reader.requireField(header, Codes.I64);
                    totalUncompressedSize = reader.readNonNegativeI64();
                    seen |= 1L << fieldId;
                    break;
                case 7: // total_compressed_size (required i64)
                    reader.requireField(header, Codes.I64);
                    totalCompressedSize = reader.readNonNegativeI64();
                    seen |= 1L << fieldId;
                    break;
                case 8: // key_value_metadata (optional list<KeyValue>)
                    if (reader.acceptField(header, Codes.LIST)) {
                        keyValueMetadata = KeyValueMetadataReader.read(reader);
                    }
                    break;
                case 9: // data_page_offset (required i64)
                    reader.requireField(header, Codes.I64);
                    dataPageOffset = reader.readNonNegativeI64();
                    seen |= 1L << fieldId;
                    break;
                case 10: // index_page_offset (optional) - skipped for now
                    reader.skipField(ThriftCompactReader.fieldType(header));
                    break;
                case 11: // dictionary_page_offset (optional)
                    if (reader.acceptField(header, Codes.I64)) {
                        dictionaryPageOffset = reader.readNonNegativeI64();
                    }
                    break;
                case 12: // statistics (optional)
                    if (reader.acceptField(header, Codes.STRUCT)) {
                        statistics = StatisticsReader.read(reader);
                    }
                    break;
                case 13: // encoding_stats (optional list<PageEncodingStats>)
                    if (reader.acceptField(header, Codes.LIST)) {
                        encodingStats = PageEncodingStatsReader.read(reader);
                    }
                    break;
                case 14: // bloom_filter_offset (optional i64)
                    if (reader.acceptField(header, Codes.I64)) {
                        bloomFilterOffset = reader.readNonNegativeI64();
                    }
                    break;
                case 15: // bloom_filter_length (optional i32)
                    if (reader.acceptField(header, Codes.I32)) {
                        bloomFilterLength = reader.readNonNegativeI32();
                    }
                    break;
                case 16: // size_statistics (optional)
                    if (reader.acceptField(header, Codes.STRUCT)) {
                        sizeStatistics = SizeStatisticsReader.read(reader);
                    }
                    break;
                case 17: // geospatial statistics (optional)
                    if (reader.acceptField(header, Codes.STRUCT)) {
                        geospatialStatistics = GeospatialStatisticsReader.read(reader);
                    }
                    break;
                default:
                    reader.skipField(ThriftCompactReader.fieldType(header));
                    break;
            }
        }

        ThriftCompactReader.requireFields(ThriftStruct.COLUMN_META_DATA, seen, REQUIRED_FIELDS);

        return new ColumnMetaData(type, encodings, pathInSchema, codec,
                numValues, totalUncompressedSize, totalCompressedSize, keyValueMetadata, dataPageOffset,
                dictionaryPageOffset, statistics, geospatialStatistics, bloomFilterOffset, bloomFilterLength,
                encodingStats, sizeStatistics);
    }

    /// `Encoding` is a Thrift enum, so its list elements are `i32` on the wire and are mapped
    /// to the enum one by one.
    private static List<Encoding> readEncodings(ThriftCompactReader reader) {
        long listHeader =
                reader.requireListHeader(Codes.I32);
        Encoding[] encodings = new Encoding[ThriftCompactReader.listSize(listHeader)];
        for (int i = 0; i < encodings.length; i++) {
            encodings[i] = ThriftEnumLookup.encoding(reader.readI32());
        }
        return List.of(encodings);
    }
}
