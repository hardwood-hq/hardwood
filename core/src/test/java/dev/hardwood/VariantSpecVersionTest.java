/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.file.Path;
import java.util.List;

import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.SchemaNode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A `VARIANT` group annotated with a specification version the reader does not implement opens
/// and reports its annotation, and a read that touches one of its columns is refused before any
/// value is decoded.
///
/// `variant_spec_version_2_test.parquet` is `variant_test.parquet` (`id` 1 to 4 beside the
/// Variant group `var`) with `var` annotated `VARIANT(2)`. In
/// `variant_spec_version_2_shape_test.parquet` (`id` 1 and 2), `var` is annotated `VARIANT(2)`
/// over children that are not the version-1 shape: a group `header {a}`, then `payload`.
class VariantSpecVersionTest {

    private static final Path VERSION_1 = Path.of("src/test/resources/variant_test.parquet");
    private static final Path VERSION_2 = Path.of("src/test/resources/variant_spec_version_2_test.parquet");
    private static final Path VERSION_2_SHAPE = Path.of("src/test/resources/variant_spec_version_2_shape_test.parquet");
    private static final String MESSAGE = "[variant_spec_version_2_test.parquet] Group 'var' is annotated"
            + " VARIANT(2); version 2 of the Variant specification is not supported, the supported version is 1";

    @Test
    void fileOpensAndReportsTheAnnotation() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(VERSION_2))) {
            assertThat(fileReader.getFileSchema().getField("var"))
                    .isInstanceOfSatisfying(SchemaNode.GroupNode.class, group -> {
                        assertThat(group.isVariant()).isTrue();
                        assertThat(group.logicalType()).isEqualTo(LogicalType.variant(2));
                    });
        }
    }

    @Test
    void readsNotTouchingTheVariantSucceed() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(VERSION_2));
                RowReader rows = fileReader.buildRowReader()
                        .projection(ColumnProjection.columns("id"))
                        .filter(FilterPredicate.eq("id", 3))
                        .build();
                ColumnReader id = fileReader.columnReader("id")) {
            assertThat(rows.hasNext()).isTrue();
            rows.next();
            assertThat(rows.getInt("id")).isEqualTo(3);
            assertThat(rows.hasNext()).isFalse();

            assertThat(id.nextBatch()).isTrue();
            assertThat(id.getValueCount()).isEqualTo(4);
        }
    }

    @Test
    void readsTouchingTheVariantAreUnsupported() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(VERSION_2))) {
            List<ThrowingCallable> reads = List.of(
                    fileReader::rowReader,
                    () -> fileReader.buildRowReader()
                            .projection(ColumnProjection.columns("id"))
                            .filter(FilterPredicate.isNull("var.metadata"))
                            .build(),
                    () -> fileReader.columnReader("var.value"),
                    () -> fileReader.columnReaders(ColumnProjection.columns("id", "var.metadata")));
            for (ThrowingCallable read : reads) {
                assertThatThrownBy(read)
                        .isInstanceOf(UnsupportedOperationException.class)
                        .hasMessage(MESSAGE);
            }
            // The refused reads leave the file reader usable.
            try (RowReader rows = fileReader.buildRowReader().projection(ColumnProjection.columns("id")).build()) {
                assertThat(rows.hasNext()).isTrue();
            }
        }
    }

    /// The `VARIANT(2)` file is the second of the read, after one whose `var` is `VARIANT(1)`.
    @Test
    void laterFileTouchingTheVariantIsUnsupported() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.openAll(
                List.of(InputFile.of(VERSION_1), InputFile.of(VERSION_2)));
                RowReader rows = fileReader.rowReader()) {
            for (int i = 0; i < 4; i++) {
                assertThat(rows.hasNext()).isTrue();
                rows.next();
                assertThat(rows.getVariant("var")).isNotNull();
            }
            assertThatThrownBy(rows::hasNext)
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessage(MESSAGE);
        }
    }

    /// The version-1 shape is not required of a group of another version: the file opens, the
    /// file's other columns read, and a read of the group is refused like any other.
    @Test
    void groupOfAnotherShapeIsRefusedOnlyWhenRead() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(VERSION_2_SHAPE))) {
            assertThat(fileReader.getFileSchema().getField("var"))
                    .isInstanceOfSatisfying(SchemaNode.GroupNode.class,
                            group -> assertThat(group.logicalType()).isEqualTo(LogicalType.variant(2)));

            try (RowReader rows = fileReader.buildRowReader().projection(ColumnProjection.columns("id")).build()) {
                assertThat(rows.hasNext()).isTrue();
                rows.next();
                assertThat(rows.getInt("id")).isEqualTo(1);
                assertThat(rows.hasNext()).isTrue();
                rows.next();
                assertThat(rows.getInt("id")).isEqualTo(2);
                assertThat(rows.hasNext()).isFalse();
            }

            List<ThrowingCallable> reads = List.of(
                    fileReader::rowReader,
                    () -> fileReader.columnReader("var.payload"),
                    () -> fileReader.columnReader("var.header.a"));
            for (ThrowingCallable read : reads) {
                assertThatThrownBy(read)
                        .isInstanceOf(UnsupportedOperationException.class)
                        .hasMessage("[variant_spec_version_2_shape_test.parquet] Group 'var' is annotated"
                                + " VARIANT(2); version 2 of the Variant specification is not supported, the"
                                + " supported version is 1");
            }
        }
    }
}
