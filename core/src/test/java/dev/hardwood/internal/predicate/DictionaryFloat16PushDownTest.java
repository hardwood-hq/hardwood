/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.predicate.dictionary.RowGroupDictionaryFilterSource;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// Dictionary push-down on a `FLOAT16` column, against `dict_float16_pushdown.parquet` (one row
/// group, 4096 rows; `half` cycling 1.0, 2.0, 4.0, 8.0).
///
/// `FLOAT16` is `FIXED_LEN_BYTE_ARRAY(2)` annotated `Float16Type`, so it reaches the
/// `ByteArrayDictionary` arm like [DictionaryFixedLenByteArrayPushDownTest], but resolves to
/// [ResolvedPredicate.Float16Predicate] rather than a binary predicate and so takes its own path
/// through [RowGroupFilterEvaluator]. Every probe sits inside the `[1.0, 8.0]` range statistics
/// advertise, so statistics alone keep the row group and only the dictionary can prove absence.
class DictionaryFloat16PushDownTest {

    private static final Path FIXTURE = Paths.get("src/test/resources/dict_float16_pushdown.parquet");

    private static ParquetFileReader reader;
    private static InputFile inputFile;
    private static RowGroup rowGroup;
    private static FileSchema schema;
    private static HardwoodContextImpl context;

    @BeforeAll
    static void open() throws Exception {
        inputFile = InputFile.of(FIXTURE);
        reader = ParquetFileReader.open(inputFile);
        rowGroup = reader.getFileMetaData().rowGroups().getFirst();
        schema = FileSchema.fromSchemaElements(reader.getFileMetaData().schema());
        context = HardwoodContextImpl.create();
    }

    @AfterAll
    static void close() throws Exception {
        reader.close();
        context.close();
    }

    @Test
    void theFixtureColumnIsAnAnnotatedTwoByteFixedLenByteArray() {
        // Without the Float16 annotation the probes below would resolve to a binary predicate and
        // exercise the FLBA arm instead, leaving the float16 path untested while still passing.
        assertThat(schema.getColumn(0).type()).isEqualTo(PhysicalType.FIXED_LEN_BYTE_ARRAY);
        assertThat(schema.getColumn(0).logicalType()).isInstanceOf(LogicalType.Float16Type.class);
    }

    @Test
    void absentHalfIsDroppedOnlyByTheDictionary() throws IOException {
        FilterPredicate absent = FilterPredicate.eq("half", 3.0f);

        assertThat(statisticsDrop(absent)).as("3.0 lies within [1.0, 8.0]").isFalse();
        assertThat(dictionaryDrop(absent)).isTrue();
        assertThat(dictionaryDrop(FilterPredicate.eq("half", 6.0f))).isTrue();
        assertThat(dictionaryDrop(FilterPredicate.eq("half", 4.0f))).isFalse();
    }

    @Test
    void aProbeBinary16CannotRepresentMatchesNothing() throws IOException {
        // 2.0005 is not a binary16 value. Narrowing the probe would round it to a neighbouring
        // half — 2.0 among them — and wrongly report it present; widening the entries instead
        // compares it against the stored values exactly, and none of them equal it.
        assertThat(dictionaryDrop(FilterPredicate.eq("half", 2.0005f))).isTrue();
    }

    private static RowGroupDictionaryFilterSource dictionaries() {
        return new RowGroupDictionaryFilterSource(inputFile, rowGroup, 0, schema, context);
    }

    private static boolean dictionaryDrop(FilterPredicate filter) throws IOException {
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(filter, schema);
        return RowGroupFilterEvaluator.decideRowGroup(resolved, rowGroup, null, dictionaries())
                == FilterDecision.CANNOT_MATCH;
    }

    private static boolean statisticsDrop(FilterPredicate filter) throws IOException {
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(filter, schema);
        return RowGroupFilterEvaluator.decideRowGroup(resolved, rowGroup, null, null) == FilterDecision.CANNOT_MATCH;
    }
}
