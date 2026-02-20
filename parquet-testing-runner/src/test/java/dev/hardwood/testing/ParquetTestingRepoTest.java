/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.testing;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;

/**
 * Test reading files from the apache/parquet-testing repository.
 * This test helps identify which files we can currently parse.
 */
@Disabled
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ParquetTestingRepoTest {

    static Stream<String> parquetTestFiles() {
        List<String> files = new ArrayList<>();

        // Bad data files (intentionally malformed - we may not be able to read these)
        files.add("bad_data/ARROW-GH-41317.parquet");
        files.add("bad_data/ARROW-GH-41321.parquet");
        files.add("bad_data/ARROW-GH-43605.parquet");
        files.add("bad_data/ARROW-GH-45185.parquet");
        files.add("bad_data/ARROW-RS-GH-6229-DICTHEADER.parquet");
        files.add("bad_data/ARROW-RS-GH-6229-LEVELS.parquet");
        files.add("bad_data/PARQUET-1481.parquet");

        // Data files (valid test data)
        files.add("data/alltypes_dictionary.parquet");
        files.add("data/alltypes_plain.parquet");
        files.add("data/alltypes_plain.snappy.parquet");
        files.add("data/alltypes_tiny_pages.parquet");
        files.add("data/alltypes_tiny_pages_plain.parquet");
        files.add("data/binary.parquet");
        files.add("data/binary_truncated_min_max.parquet");
        files.add("data/byte_array_decimal.parquet");
        files.add("data/byte_stream_split.zstd.parquet");
        files.add("data/byte_stream_split_extended.gzip.parquet");
        files.add("data/column_chunk_key_value_metadata.parquet");
        files.add("data/concatenated_gzip_members.parquet");
        files.add("data/data_index_bloom_encoding_stats.parquet");
        files.add("data/data_index_bloom_encoding_with_length.parquet");
        files.add("data/datapage_v1-corrupt-checksum.parquet");
        files.add("data/datapage_v1-snappy-compressed-checksum.parquet");
        files.add("data/datapage_v1-uncompressed-checksum.parquet");
        files.add("data/datapage_v2.snappy.parquet");
        files.add("data/datapage_v2_empty_datapage.snappy.parquet");
        files.add("data/delta_binary_packed.parquet");
        files.add("data/delta_byte_array.parquet");
        files.add("data/delta_encoding_optional_column.parquet");
        files.add("data/delta_encoding_required_column.parquet");
        files.add("data/delta_length_byte_array.parquet");
        files.add("data/dict-page-offset-zero.parquet");
        files.add("data/fixed_length_byte_array.parquet");
        files.add("data/fixed_length_decimal.parquet");
        files.add("data/fixed_length_decimal_legacy.parquet");
        files.add("data/float16_nonzeros_and_nans.parquet");
        files.add("data/float16_zeros_and_nans.parquet");
        files.add("data/geospatial/crs-arbitrary-value.parquet");
        files.add("data/geospatial/crs-default.parquet");
        files.add("data/geospatial/crs-geography.parquet");
        files.add("data/geospatial/crs-projjson.parquet");
        files.add("data/geospatial/crs-srid.parquet");
        files.add("data/geospatial/geospatial-with-nan.parquet");
        files.add("data/geospatial/geospatial.parquet");
        files.add("data/hadoop_lz4_compressed.parquet");
        files.add("data/hadoop_lz4_compressed_larger.parquet");
        files.add("data/incorrect_map_schema.parquet");
        files.add("data/int32_decimal.parquet");
        files.add("data/int32_with_null_pages.parquet");
        files.add("data/int64_decimal.parquet");
        files.add("data/int96_from_spark.parquet");
        files.add("data/large_string_map.brotli.parquet");
        files.add("data/list_columns.parquet");
        files.add("data/lz4_raw_compressed.parquet");
        files.add("data/lz4_raw_compressed_larger.parquet");
        files.add("data/map_no_value.parquet");
        files.add("data/nan_in_stats.parquet");
        files.add("data/nation.dict-malformed.parquet");
        files.add("data/nested_lists.snappy.parquet");
        files.add("data/nested_maps.snappy.parquet");
        files.add("data/nested_structs.rust.parquet");
        files.add("data/non_hadoop_lz4_compressed.parquet");
        files.add("data/nonnullable.impala.parquet");
        files.add("data/null_list.parquet");
        files.add("data/nullable.impala.parquet");
        files.add("data/nulls.snappy.parquet");
        files.add("data/old_list_structure.parquet");
        files.add("data/overflow_i16_page_cnt.parquet");
        files.add("data/page_v2_empty_compressed.parquet");
        files.add("data/plain-dict-uncompressed-checksum.parquet");
        files.add("data/repeated_no_annotation.parquet");
        files.add("data/repeated_primitive_no_list.parquet");
        files.add("data/rle-dict-snappy-checksum.parquet");
        files.add("data/rle-dict-uncompressed-corrupt-checksum.parquet");
        files.add("data/rle_boolean_encoding.parquet");
        files.add("data/single_nan.parquet");
        files.add("data/sort_columns.parquet");
        files.add("data/unknown-logical-type.parquet");

        // Shredded variant files (nested structure tests)
        for (int i = 1; i <= 138; i++) {
            if (i == 3) {
                continue; // Skip missing case-003
            }
            String filename = String.format("shredded_variant/case-%03d", i);
            if (i == 43 || i == 84 || i == 125) {
                filename += "-INVALID";
            }
            files.add(filename + ".parquet");
        }

        return files.stream();
    }

    @BeforeAll
    void setUp() throws IOException {
        ParquetTestingRepoCloner.ensureCloned();
    }

    @ParameterizedTest
    @MethodSource("parquetTestFiles")
    void testReadParquetFile(String relativePath) throws Exception {
        Path filePath = Paths.get("target", "parquet-testing", relativePath);

        System.out.println("\n=== Testing: " + relativePath + " ===");

        try (ParquetFileReader reader = ParquetFileReader.open(filePath)) {
            // Read and display file metadata
            FileMetaData metadata = reader.getFileMetaData();
            System.out.println("Version: " + metadata.version());
            System.out.println("Num rows: " + metadata.numRows());
            System.out.println("Row groups: " + metadata.rowGroups().size());
            System.out.println("Columns: " + reader.getFileSchema().getColumnCount());

            // Read ALL columns using batch API to verify we can parse everything
            int totalValuesRead = 0;
            for (int colIdx = 0; colIdx < reader.getFileSchema().getColumnCount(); colIdx++) {
                var column = reader.getFileSchema().getColumn(colIdx);

                if (colIdx == 0) {
                    System.out.println("Column " + colIdx + ": " + column.name() + " (" + column.type() + ")");
                }

                try (ColumnReader columnReader = reader.createColumnReader(colIdx)) {
                    int columnValues = 0;
                    while (columnReader.nextBatch()) {
                        columnValues += columnReader.getRecordCount();
                    }
                    totalValuesRead += columnValues;
                }
            }

            System.out.println("  Total records read from all columns: " + totalValuesRead);
            System.out.println("✓ SUCCESS: Can read " + relativePath);
        }
        catch (Exception e) {
            System.out.println("✗ FAILED: " + relativePath);
            System.out.println("  Error: " + e.getClass().getSimpleName() + ": " + e.getMessage());

            // Rethrow to mark test as failed
            throw e;
        }
    }
}
