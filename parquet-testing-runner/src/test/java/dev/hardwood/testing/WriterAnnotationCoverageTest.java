/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.testing;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.parquet.column.Encoding;
import org.apache.parquet.example.data.Group;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.OutputFile;
import dev.hardwood.internal.writer.LogicalTypeValueRange;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.testing.Coverage.StorageForm;
import dev.hardwood.testing.CoverageDomain.Annotation;
import dev.hardwood.writer.ColumnBatch;
import dev.hardwood.writer.ColumnEncoding;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;

/// The annotation half of the write-path interop gate, driven by the domain rather than by a
/// table beside it.
///
/// [WriterLogicalTypeInteropTest] pairs each annotation with the parquet-java annotation it must
/// read back as, over a hand-written list of points. This covers the same annotations at the
/// points that list leaves out — every width and signedness of an `INT`, every unit of a `TIME`
/// or `TIMESTAMP` against both settings of its UTC flag, and a `DECIMAL` at each carrier's
/// precision boundary — and at the two things a list of annotations does not describe at all:
///
/// - **Both storage forms.** An annotation's comparator governs the chunk's bounds whether the
///   values sit behind a dictionary or not, and the two reach those bounds through different
///   accumulators.
/// - **The ends of the declared range.** An annotation narrows what its physical type may hold,
///   and [LogicalTypeValueRange] computes by how much. The ends are where the comparator is
///   fragile — an unsigned maximum is spelled as a negative, a binary `DECIMAL` bound needs sign
///   extension, a `BYTE_ARRAY` maximum past the truncation length must truncate *and* increment.
///
/// What it deliberately does not cover is the refusal of a value *outside* that range. A refused
/// value produces no bytes, so parquet-java has nothing to say about it and this module adds
/// nothing by asserting it; core's `WriterAnnotationRangeTest` owns that half, holds the writer
/// to the message it reports, and pins the bounds as constants of its own rather than deriving
/// them from the same class the writer applies.
///
/// Its cases come from [CoverageDomain#annotations], which is also what
/// [WriteCoverageVerdictTest] requires, so an annotation added to the writer produces a case here
/// and a requirement there in the same commit.
class WriterAnnotationCoverageTest {

    private static final String COLUMN = CoverageDomain.COLUMN;

    /// Rows in a storage-form file: enough that four distinct values make a dictionary the
    /// smaller of the two encodings.
    private static final int STORAGE_ROWS = 100;

    private static final int STORAGE_DISTINCT = 4;

    /// The length of the `BYTE_ARRAY` maximum, past the default statistics truncation length of
    /// 64 bytes so that the bound written for it is a truncated one.
    private static final int LONG_BINARY_LENGTH = 100;

    static Stream<Annotation> annotations() {
        return CoverageDomain.annotations().stream().filter(CoverageDomain::readByParquetJava);
    }

    // ==================== Tests ====================

    /// Each annotation written in every storage form it has, read back through parquet-java, with
    /// the page encoding held to the form the case is for — without which a dictionary that
    /// quietly resolved to `PLAIN` would pass as having covered both.
    @ParameterizedTest(name = "{0}")
    @MethodSource("annotations")
    void storageForms(Annotation annotation, @TempDir Path dir) throws IOException {
        for (StorageForm form : CoverageDomain.storageForms(annotation)) {
            Path file = dir.resolve(form + ".parquet");
            List<Object> values = storageValues(annotation);
            write(annotation, file, configFor(form), values, holdsNoValue(annotation));

            assertValues(annotation, file, values);
            if (!holdsNoValue(annotation)) {
                assertPageEncoding(file, form == StorageForm.DICTIONARY
                        ? Encoding.RLE_DICTIONARY
                        : Encoding.PLAIN);
            }
        }
    }

    /// Each annotation written at the ends of the range it declares and at a point inside it,
    /// read back through parquet-java value by value.
    ///
    /// An annotation that narrows nothing takes the extremes of the order it puts on its physical
    /// type — which for an unsigned integer is not the type's own, the maximum being the all-ones
    /// pattern the signed storage spells `-1`. That value and a `BYTE_ARRAY` maximum past the
    /// truncation length are the two that have to survive the trip intact.
    @ParameterizedTest(name = "{0}")
    @MethodSource("annotations")
    void boundaryValues(Annotation annotation, @TempDir Path dir) throws IOException {
        Path file = dir.resolve("boundaries.parquet");
        List<Boundary> written = present(annotation);
        List<Object> values = new ArrayList<>();
        for (Boundary boundary : written) {
            values.add(value(annotation, boundary));
        }

        write(annotation, file, WriterConfig.defaults(), values, holdsNoValue(annotation));
        assertValues(annotation, file, values);
    }

    // ==================== The values ====================

    /// Where in an annotation's range a written value sits.
    ///
    /// These name what a case writes rather than a coverage cell: the values reach the file and
    /// are read back out of it value by value, which is a stronger statement than a cell recording
    /// that the case ran.
    private enum Boundary {

        /// The smallest value the column may hold.
        MIN,

        /// The largest value the column may hold.
        MAX,

        /// A value strictly between the two.
        INTERIOR,

        /// The nulls an `UNKNOWN` column carries, that annotation admitting no value at all.
        NULLS_ONLY
    }

    /// The boundary classes whose values a file actually holds: the ends and the interior, or —
    /// for `UNKNOWN`, which admits no value at all — the nulls it carries instead.
    private static List<Boundary> present(Annotation annotation) {
        if (holdsNoValue(annotation)) {
            return List.of(Boundary.NULLS_ONLY);
        }
        return List.of(Boundary.MIN, Boundary.INTERIOR, Boundary.MAX);
    }

    /// The value `annotation` takes at one boundary class, in the representation its carrier's
    /// setter accepts: a boxed `long` for an integral carrier, the bytes for a binary one.
    private static Object value(Annotation annotation, Boundary boundary) {
        return isIntegral(annotation) ? integralValue(annotation, boundary)
                : binaryValue(annotation, boundary);
    }

    /// Where an integral column's boundaries sit: the ends the annotation declares, or the
    /// extremes of the order it puts on its physical type where it declares none.
    ///
    /// An `INT(32)` or `INT(64)` narrows no bit pattern of its storage, so it is unbounded — but
    /// its unsigned form still changes which bit patterns are the extremes. Every pattern is a
    /// value, and the largest is the all-ones one, which the signed storage spells `-1`. Taking
    /// the signed extremes for such a column would write values named for a boundary they do not
    /// sit on, and would leave the pattern the unsigned maximum actually is out of the file.
    private static long integralValue(Annotation annotation, Boundary boundary) {
        LogicalTypeValueRange range = CoverageDomain.rangeOf(annotation);
        boolean narrow = annotation.carrier() == PhysicalType.INT32;
        long min;
        long max;
        if (range.isBounded()) {
            min = range.min();
            max = range.max();
        }
        else if (isUnsigned(annotation)) {
            min = 0;
            max = -1;
        }
        else {
            min = narrow ? Integer.MIN_VALUE : Long.MIN_VALUE;
            max = narrow ? Integer.MAX_VALUE : Long.MAX_VALUE;
        }
        return switch (boundary) {
            case MIN -> min;
            case MAX -> max;
            case INTERIOR, NULLS_ONLY -> min < 0 && max > 0 ? 0 : min + 1;
        };
    }

    /// Where a binary column's boundaries sit.
    ///
    /// A binary `DECIMAL` is the one annotation that narrows such a column, and it narrows it by
    /// magnitude: its values are big-endian two's complement unscaled values, so the ends are the
    /// declared bound and its negation, sign-extended to the column's length where it has one. Every other annotation over a binary carrier narrows
    /// nothing, and takes the extremes of the unsigned order the format compares those columns in
    /// — for a `BYTE_ARRAY` the empty value and a long run of `0xff`, whose bound is past the
    /// truncation length and so has to be truncated upwards to still bound the column.
    private static byte[] binaryValue(Annotation annotation, Boundary boundary) {
        LogicalTypeValueRange range = CoverageDomain.rangeOf(annotation);
        if (range.isBounded()) {
            BigInteger bound = range.unscaledBound();
            BigInteger value = switch (boundary) {
                case MIN -> bound.negate();
                case MAX -> bound;
                case INTERIOR, NULLS_ONLY -> BigInteger.ZERO;
            };
            return unscaled(value, annotation.typeLength());
        }
        if (annotation.typeLength() != null) {
            return switch (boundary) {
                case MIN -> filled(annotation.typeLength(), (byte) 0x00);
                case MAX -> filled(annotation.typeLength(), (byte) 0xff);
                default -> filled(annotation.typeLength(), (byte) 0x2a);
            };
        }
        return switch (boundary) {
            case MIN -> new byte[0];
            case MAX -> filled(LONG_BINARY_LENGTH, (byte) 0xff);
            default -> "interior".getBytes(StandardCharsets.UTF_8);
        };
    }

    /// `value` as the big-endian two's complement bytes a `DECIMAL` column stores, sign-extended
    /// to the column's declared length where it has one.
    private static byte[] unscaled(BigInteger value, Integer typeLength) {
        byte[] minimal = value.toByteArray();
        if (typeLength == null) {
            return minimal;
        }
        byte[] extended = filled(typeLength, (byte) (value.signum() < 0 ? 0xff : 0x00));
        System.arraycopy(minimal, 0, extended, typeLength - minimal.length, minimal.length);
        return extended;
    }

    private static byte[] filled(int length, byte fill) {
        byte[] bytes = new byte[length];
        Arrays.fill(bytes, fill);
        return bytes;
    }

    /// The values a storage-form file holds: few enough distinct ones that a dictionary is the
    /// smaller encoding, and all well inside the declared range so that the file is about how the
    /// values are stored rather than about which they are.
    private static List<Object> storageValues(Annotation annotation) {
        List<Object> values = new ArrayList<>(STORAGE_ROWS);
        for (int row = 0; row < STORAGE_ROWS; row++) {
            values.add(ordinalValue(annotation, row % STORAGE_DISTINCT));
        }
        return values;
    }

    private static Object ordinalValue(Annotation annotation, int ordinal) {
        if (isIntegral(annotation)) {
            return integralValue(annotation, Boundary.INTERIOR) + ordinal;
        }
        LogicalTypeValueRange range = CoverageDomain.rangeOf(annotation);
        if (range.isBounded()) {
            return unscaled(BigInteger.valueOf(ordinal), annotation.typeLength());
        }
        if (annotation.typeLength() != null) {
            byte[] value = filled(annotation.typeLength(), (byte) 0x2a);
            value[annotation.typeLength() - 1] = (byte) ordinal;
            return value;
        }
        return ("v" + ordinal).getBytes(StandardCharsets.UTF_8);
    }

    /// Whether the annotation puts an unsigned order on its column.
    private static boolean isUnsigned(Annotation annotation) {
        return annotation.logicalType() instanceof LogicalType.IntType intType && !intType.isSigned();
    }

    private static boolean isIntegral(Annotation annotation) {
        return annotation.carrier() == PhysicalType.INT32 || annotation.carrier() == PhysicalType.INT64;
    }

    private static boolean holdsNoValue(Annotation annotation) {
        return CoverageDomain.rangeOf(annotation).holdsNoValue();
    }

    // ==================== Writing and reading ====================

    private static WriterConfig configFor(StorageForm form) {
        return form == StorageForm.DICTIONARY
                ? WriterConfig.defaults()
                : WriterConfig.builder().encoding(ColumnEncoding.PLAIN).build();
    }

    /// Writes a one-column file holding `values` through the columnar API.
    private void write(Annotation annotation, Path file, WriterConfig config, List<Object> values,
            boolean allNull) throws IOException {

        try (ParquetFileWriter writer = ParquetFileWriter.create(
                OutputFile.of(file), CoverageDomain.schemaOf(annotation), config)) {
            writer.columnWriter().writeBatch(batch -> set(batch, annotation, values, allNull));
        }
    }

    private static void set(ColumnBatch batch, Annotation annotation, List<Object> values,
            boolean allNull) {

        boolean[] nulls = new boolean[values.size()];
        Arrays.fill(nulls, allNull);
        switch (annotation.carrier()) {
            case INT32 -> {
                int[] ints = new int[values.size()];
                for (int i = 0; i < ints.length; i++) {
                    ints[i] = Math.toIntExact((long) values.get(i));
                }
                batch.ints(COLUMN, ints, nulls);
            }
            case INT64 -> {
                long[] longs = new long[values.size()];
                for (int i = 0; i < longs.length; i++) {
                    longs[i] = (long) values.get(i);
                }
                batch.longs(COLUMN, longs, nulls);
            }
            case BYTE_ARRAY -> batch.bytes(COLUMN, binaries(values), nulls);
            case FIXED_LEN_BYTE_ARRAY -> batch.fixed(COLUMN, binaries(values), nulls);
            default -> throw new IllegalStateException("No carrier setter for " + annotation.carrier());
        }
    }

    private static byte[][] binaries(List<Object> values) {
        byte[][] binaries = new byte[values.size()][];
        for (int i = 0; i < binaries.length; i++) {
            binaries[i] = (byte[]) values.get(i);
        }
        return binaries;
    }

    /// Every value parquet-java reads back is the one that was written, and a column that holds
    /// no value carries a null in every row instead.
    private void assertValues(Annotation annotation, Path file, List<Object> values)
            throws IOException {

        List<Group> rows = ParquetJavaReader.readGroups(file);
        assertThat(rows).as("row count").hasSize(values.size());
        for (int r = 0; r < rows.size(); r++) {
            Group row = rows.get(r);
            int field = row.getType().getFieldIndex(COLUMN);
            if (holdsNoValue(annotation)) {
                assertThat(row.getFieldRepetitionCount(field)).as("row %d is null", r).isZero();
                continue;
            }
            assertThat(read(annotation, row, field)).as("row %d value", r).isEqualTo(values.get(r));
        }
    }

    private static Object read(Annotation annotation, Group row, int field) {
        return switch (annotation.carrier()) {
            case INT32 -> (long) row.getInteger(field, 0);
            case INT64 -> row.getLong(field, 0);
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> row.getBinary(field, 0).getBytes();
            default -> throw new IllegalStateException("No carrier reader for " + annotation.carrier());
        };
    }

    private void assertPageEncoding(Path file, Encoding expected) throws IOException {
        for (Set<Encoding> chunk : ParquetJavaReader.readPages(file).chunkValueEncodings()) {
            assertThat(chunk).as("page encodings").containsExactly(expected);
        }
    }
}
