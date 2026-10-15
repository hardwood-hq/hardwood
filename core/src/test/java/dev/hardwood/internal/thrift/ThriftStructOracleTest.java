/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.junit.jupiter.api.Test;

import shaded.parquet.org.apache.thrift.TFieldIdEnum;

import static org.assertj.core.api.Assertions.assertThat;

/// Checks every field name in [ThriftStruct] against parquet-format's own
/// generated metadata.
///
/// The names are transcribed from parquet.thrift by hand, and a wrong one is
/// the failure mode that matters: it produces a confidently wrong field name,
/// which reads exactly as authoritative as a right one and sends whoever is
/// diagnosing a corrupt file to the wrong part of it. Nothing about a message
/// being well formed catches that — only comparing it to a source of truth
/// does.
///
/// parquet-format's generated classes carry the id and name of every field, so
/// every name is checked rather than the ones a corrupt fixture happens to
/// reach. The structs come from [ThriftStruct#values] rather than a list kept
/// here, so a struct the readers can name is checked by virtue of being nameable:
/// there is no second list to fall out of step with the first. The artifact shades
/// its Thrift runtime, which is why the interface is imported from
/// `shaded.parquet.org.apache.thrift`.
class ThriftStructOracleTest {

    /// parquet-format generates one class per struct, under this package and named
    /// exactly as `parquet.thrift` names the struct — which is what
    /// [ThriftStruct#structName] holds.
    private static final String GENERATED_PACKAGE = "org.apache.parquet.format.";

    /// Field ids the format could plausibly use. Parquet's are all small; the
    /// range is swept so that an entry naming an id the format does not define
    /// is caught as readily as one naming it wrongly.
    private static final int MAX_FIELD_ID = 64;

    /// Ids the readers parse that the pinned parquet-format predates.
    ///
    /// Each is read by this library today, from a struct definition newer than the
    /// artifact the oracle comes from, so the generated metadata cannot confirm it and
    /// the reader is the authority instead. Listed rather than skipped so that adding
    /// one is a deliberate act.
    private static final Set<String> AHEAD_OF_THE_ORACLE = Set.of(
            "ColumnOrder field 2",  // IEEE754TotalOrder, parsed by ColumnOrderReader
            "ColumnOrder field 3",  // Int96TimestampOrder, parsed by ColumnOrderReader
            "ColumnIndex field 8",  // nan_counts, parsed by ColumnIndexReader
            "LogicalType field 19", // FILE, parsed by LogicalTypeReader
            "Statistics field 9");  // nan_count, parsed by StatisticsReader

    @Test
    void everyNameMatchesParquetFormatAndNoneIsInvented() throws Exception {
        List<String> wrong = new ArrayList<>();

        for (ThriftStruct struct : ThriftStruct.values()) {
            Class<?> generated = Class.forName(GENERATED_PACKAGE + struct.structName());
            for (int id = 0; id <= MAX_FIELD_ID; id++) {
                if (AHEAD_OF_THE_ORACLE.contains(struct.structName() + " field " + id)) {
                    continue;
                }
                String expected = fieldNameIn(generated, id);
                String actual = struct.fieldName(id);
                if (!Objects.equals(expected, actual)) {
                    wrong.add(struct.structName() + " field " + id + ": parquet-format says "
                            + expected + ", ThriftStruct says " + actual);
                }
            }
        }

        assertThat(wrong).isEmpty();
    }

    /// What parquet-format calls field `id` of `generated`, or `null` if it
    /// defines no such field.
    private static String fieldNameIn(Class<?> generated, int id) {
        for (Object field : fieldsOf(generated)) {
            TFieldIdEnum typed = (TFieldIdEnum) field;
            if (typed.getThriftFieldId() == id) {
                return typed.getFieldName();
            }
        }
        return null;
    }

    /// The constants of the `_Fields` enum parquet-format nests in every generated
    /// struct, which is where the ids and names live.
    private static Object[] fieldsOf(Class<?> generated) {
        for (Class<?> nested : generated.getDeclaredClasses()) {
            if ("_Fields".equals(nested.getSimpleName())) {
                return nested.getEnumConstants();
            }
        }
        throw new IllegalStateException("no _Fields on " + generated);
    }
}
