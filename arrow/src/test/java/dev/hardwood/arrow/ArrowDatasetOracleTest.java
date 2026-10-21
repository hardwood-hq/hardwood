/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.arrow;

import java.io.IOException;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;

import static dev.hardwood.arrow.ArrowTestSupport.assertSameTable;
import static dev.hardwood.arrow.ArrowTestSupport.assertSameTableIgnoringNames;
import static dev.hardwood.arrow.ArrowTestSupport.coreFixture;
import static dev.hardwood.arrow.ArrowTestSupport.fixture;
import static dev.hardwood.arrow.ArrowTestSupport.readAll;
import static org.assertj.core.api.Assertions.assertThat;

/// Arrow C++'s Parquet reader, reached through `arrow-dataset`, reads the same file and hands
/// its batches over in memory through the C Data Interface. Its table is the specification
/// the module implements, schema included, so the comparison is the strict one wherever the
/// file carries no `ARROW:schema` entry for Arrow C++ to restore names from.
class ArrowDatasetOracleTest {

    private final BufferAllocator allocator = new RootAllocator();

    @AfterEach
    void allocatorIsEmpty() {
        assertThat(allocator.getAllocatedMemory()).isZero();
        allocator.close();
    }

    /// Files without an `ARROW:schema` entry: the comparison is strict, names and metadata
    /// included.
    static Stream<Path> files() throws IOException {
        return Stream.of(
                fixture("scalars"),
                fixture("scalars_dict"),
                fixture("empty"),
                fixture("field_ids"),
                fixture("nested"),
                coreFixture("int96_timestamp_test"),
                coreFixture("dict_int96"),
                coreFixture("dict_plain_fallback"),
                coreFixture("dict_cross_chunk"),
                coreFixture("list_of_lists_modern_test"),
                coreFixture("old_list_structure_test"),
                coreFixture("unannotated_repeated_primitive_test"),
                coreFixture("unannotated_repeated_group_test"),
                coreFixture("unannotated_repeated_group_empty_test"),
                coreFixture("unannotated_repeated_nested_test"),
                coreFixture("unannotated_repeated_annotated_list_test"),
                coreFixture("unannotated_repeated_group_annotated_list_test"),
                coreFixture("unannotated_repeated_group_empty_annotated_list_test"),
                coreFixture("repeated_variant_group_plain_test"));
    }

    /// Core's nested fixtures, written by PyArrow with an `ARROW:schema` entry that Arrow C++
    /// honours and the module does not: the child names it restores (`item` for a list
    /// element) differ from the Parquet ones, so names and metadata are left out. Left out
    /// altogether are the legacy two-level list of lists, which Arrow C++ refuses to read, and
    /// `typed_accessors_issue_445`, whose `INTERVAL` columns carry parquet-java's form of the
    /// annotation, which Arrow C++ reads as the `NULL` type and Hardwood as `INTERVAL`.
    static Stream<Path> filesWithArrowSchema() {
        return Stream.of("address_book_test", "deep_nested_struct_test", "map_types_test", "map_struct_value_test",
                "map_with_list_value_test", "list_of_maps_test", "list_of_optional_struct_test", "list_struct_test",
                "nested_list_test", "triple_nested_list_test", "simple_map_test", "map_of_maps_test",
                "enum_nested_test", "nested_flba_test", "uuid_list_test", "unsigned_int_nested_test",
                "nested_int96_timestamp_test", "list_decimal_flba_test", "list_float16_test", "map_typed_keys_test",
                "list_basic_test", "primitive_lists_test", "nested_struct_test", "optional_struct_optional_leaf_test",
                "struct_then_primitive_test", "shared_leaf_name_test", "wide_struct_test", "nested_list_struct_test",
                "list_of_list_of_struct_test", "dict_nested_repeats", "local_timestamp_test",
                "nested_logical_accessors", "variant_test", "variant_shredded_test", "variant_shredded_in_struct_test",
                "variant_in_repeated_test", "variant_attributes_example", "convert_variant_null_test")
                .map(ArrowTestSupport::coreFixture);
    }

    @ParameterizedTest
    @MethodSource("files")
    void matchesArrowCpp(Path file) throws Exception {
        compare(file, true);
    }

    @ParameterizedTest
    @MethodSource("filesWithArrowSchema")
    void matchesArrowCppUpToNames(Path file) throws Exception {
        compare(file, false);
    }

    private void compare(Path file, boolean strict) throws Exception {
        try (DatasetFactory factory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(),
                FileFormat.PARQUET, file.toUri().toString());
             Dataset dataset = factory.finish();
             Scanner scanner = dataset.newScan(new ScanOptions(1024));
             ArrowReader arrowCpp = scanner.scanBatches();
             VectorSchemaRoot expected = readAll(arrowCpp, allocator);
             ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(file));
             ArrowBatchReader reader = ArrowReaders.batchReader(fileReader, allocator);
             VectorSchemaRoot actual = readAll(reader, allocator)) {
            if (strict) {
                assertSameTable(expected, actual);
            }
            else {
                assertSameTableIgnoringNames(expected, actual);
            }
        }
    }
}
