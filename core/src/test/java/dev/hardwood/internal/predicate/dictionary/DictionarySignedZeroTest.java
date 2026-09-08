/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate.dictionary;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// Signed-zero handling in `FLOAT` / `DOUBLE` dictionary push-down against
/// `dict_signed_zero.parquet` (one row group, 4096 rows, values `{-0.0, 1.5, 2.5, 3.5}` in both
/// the `f` and `d` columns — `+0.0` never occurs).
///
/// A dictionary holds exact stored values, so `Float.compare` / `Double.compare` — the total order
/// every matcher applies — separates `-0.0` from `+0.0`. A `+0.0` probe is therefore pruned, and
/// this does not depend on the column's [dev.hardwood.metadata.ColumnOrder]: the `±0` ambiguity the
/// Parquet spec describes applies to statistics min/max, not to stored values.
class DictionarySignedZeroTest {

    private static final Path FIXTURE = Paths.get("src/test/resources/dict_signed_zero.parquet");

    private static final int FLOAT_COLUMN = 0;
    private static final int DOUBLE_COLUMN = 1;

    private static ParquetFileReader reader;
    private static HardwoodContextImpl context;
    private static RowGroupDictionaryFilterSource dictionaries;

    @BeforeAll
    static void open() throws Exception {
        InputFile inputFile = InputFile.of(FIXTURE);
        reader = ParquetFileReader.open(inputFile);
        RowGroup rowGroup = reader.getFileMetaData().rowGroups().getFirst();
        FileSchema schema = FileSchema.fromSchemaElements(reader.getFileMetaData().schema());
        context = HardwoodContextImpl.create();
        dictionaries = new RowGroupDictionaryFilterSource(inputFile, rowGroup, 0, schema, context);
    }

    @AfterAll
    static void close() throws Exception {
        reader.close();
        context.close();
    }

    @Test
    void positiveZeroIsProvenAbsent() throws IOException {
        // The dictionary holds -0.0 and no +0.0. Rows holding -0.0 would not match a +0.0
        // predicate either, so proving +0.0 absent is exact, not merely permissible.
        assertThat(DictionaryFilterSupport.valueAbsent(dictionaries.forColumn(FLOAT_COLUMN), 0.0f)).isTrue();
        assertThat(DictionaryFilterSupport.valueAbsent(dictionaries.forColumn(DOUBLE_COLUMN), 0.0)).isTrue();
    }

    @Test
    void negativeZeroIsPresent() throws IOException {
        assertThat(DictionaryFilterSupport.valueAbsent(dictionaries.forColumn(FLOAT_COLUMN), -0.0f)).isFalse();
        assertThat(DictionaryFilterSupport.valueAbsent(dictionaries.forColumn(DOUBLE_COLUMN), -0.0)).isFalse();
    }

    @Test
    void nonZeroValuesAreDecidedTheSameWay() throws IOException {
        assertThat(DictionaryFilterSupport.valueAbsent(dictionaries.forColumn(FLOAT_COLUMN), 2.5f)).isFalse();
        assertThat(DictionaryFilterSupport.valueAbsent(dictionaries.forColumn(FLOAT_COLUMN), 4.5f)).isTrue();
        assertThat(DictionaryFilterSupport.valueAbsent(dictionaries.forColumn(DOUBLE_COLUMN), 2.5)).isFalse();
        assertThat(DictionaryFilterSupport.valueAbsent(dictionaries.forColumn(DOUBLE_COLUMN), 4.5)).isTrue();
    }
}
