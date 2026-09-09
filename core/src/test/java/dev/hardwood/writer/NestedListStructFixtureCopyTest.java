/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqStruct;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// `nested_list_struct_test.parquet` carries an `OPTIONAL` struct directly enclosing a `LIST`
/// (`chapters.list.element`, which is itself nested inside the outer `chapters` list) — the
/// shape #1026 found the writer refusing while the reader accepted it. Copying the file through
/// [NestedColumnCopier] and reading both back proves the writer now produces this shape, not
/// just that it stops throwing.
class NestedListStructFixtureCopyTest {

    @Test
    void copiesThroughColumnReaderAndColumnBatchAndReadsBackEqual() throws Exception {
        Path source = Paths.get("src/test/resources/nested_list_struct_test.parquet");

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(source))) {
            FileSchema schema = reader.getFileSchema();
            try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
                NestedColumnCopier.copy(reader, schema, writer);
            }
        }

        try (ParquetFileReader originalReader = ParquetFileReader.open(InputFile.of(source));
                RowReader original = originalReader.rowReader();
                ParquetFileReader copyReader = ParquetFileReader.open(InputFile.of(out.buffer()));
                RowReader copy = copyReader.rowReader()) {
            while (original.hasNext()) {
                assertThat(copy.hasNext()).isTrue();
                original.next();
                copy.next();
                for (int f = 0; f < original.getFieldCount(); f++) {
                    assertValuesEqual(original.getValue(f), copy.getValue(f));
                }
            }
            assertThat(copy.hasNext()).isFalse();
        }
    }

    /// Recursively compares two decoded field values, descending into `PqStruct` and `PqList`
    /// by field/element rather than relying on identity or reference equality, since the two
    /// sides are accessor views over independent readers.
    private static void assertValuesEqual(Object a, Object b) {
        if (a == null || b == null) {
            assertThat(a).isEqualTo(b);
        }
        else if (a instanceof PqStruct sa && b instanceof PqStruct sb) {
            assertThat(sa.getFieldCount()).isEqualTo(sb.getFieldCount());
            for (int i = 0; i < sa.getFieldCount(); i++) {
                assertThat(sa.getFieldName(i)).isEqualTo(sb.getFieldName(i));
                assertValuesEqual(sa.getValue(i), sb.getValue(i));
            }
        }
        else if (a instanceof PqList la && b instanceof PqList lb) {
            assertThat(la.size()).isEqualTo(lb.size());
            for (int i = 0; i < la.size(); i++) {
                assertValuesEqual(la.get(i), lb.get(i));
            }
        }
        else if (a instanceof byte[] ba && b instanceof byte[] bb) {
            assertThat(ba).isEqualTo(bb);
        }
        else {
            assertThat(a).isEqualTo(b);
        }
    }
}
