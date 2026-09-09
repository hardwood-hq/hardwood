/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.InputFile;
import dev.hardwood.Validity;
import dev.hardwood.internal.writer.ByteBufferOutputFile;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.LogicalType.EdgeInterpolationAlgorithm;
import dev.hardwood.metadata.LogicalType.TimeUnit;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// Round-trip tests for the stage-13a logical type annotations: a column declared with an
/// annotation is written to a real file and read back carrying it, and the sort orders the
/// annotations imply are honoured by the column statistics.
class WriterLogicalTypeRoundTripTest {

    /// Every annotation the writer supports on a primitive column, paired with the physical
    /// type and (where fixed) byte length it annotates.
    static Stream<Annotated> annotations() {
        return Stream.of(
                new Annotated(PhysicalType.BYTE_ARRAY, null, LogicalType.string()),
                new Annotated(PhysicalType.BYTE_ARRAY, null, LogicalType.enumType()),
                new Annotated(PhysicalType.BYTE_ARRAY, null, LogicalType.json()),
                new Annotated(PhysicalType.BYTE_ARRAY, null, LogicalType.bson()),
                new Annotated(PhysicalType.BYTE_ARRAY, null, LogicalType.decimal(20, 2)),
                new Annotated(PhysicalType.BYTE_ARRAY, null, LogicalType.geometry("EPSG:4326")),
                new Annotated(PhysicalType.BYTE_ARRAY, null,
                        LogicalType.geography("EPSG:4326", EdgeInterpolationAlgorithm.KARNEY)),
                new Annotated(PhysicalType.INT32, null, LogicalType.date()),
                new Annotated(PhysicalType.INT32, null, LogicalType.intType(8, true)),
                new Annotated(PhysicalType.INT32, null, LogicalType.intType(16, false)),
                new Annotated(PhysicalType.INT32, null, LogicalType.intType(32, false)),
                new Annotated(PhysicalType.INT32, null, LogicalType.decimal(9, 2)),
                new Annotated(PhysicalType.INT32, null, LogicalType.time(true, TimeUnit.MILLIS)),
                new Annotated(PhysicalType.INT64, null, LogicalType.intType(64, false)),
                new Annotated(PhysicalType.INT64, null, LogicalType.decimal(18, 4)),
                new Annotated(PhysicalType.INT64, null, LogicalType.time(false, TimeUnit.MICROS)),
                new Annotated(PhysicalType.INT64, null, LogicalType.time(true, TimeUnit.NANOS)),
                new Annotated(PhysicalType.INT64, null, LogicalType.timestamp(true, TimeUnit.MILLIS)),
                new Annotated(PhysicalType.INT64, null, LogicalType.timestamp(false, TimeUnit.MICROS)),
                new Annotated(PhysicalType.INT64, null, LogicalType.timestamp(true, TimeUnit.NANOS)),
                new Annotated(PhysicalType.FIXED_LEN_BYTE_ARRAY, 16, LogicalType.uuid()),
                new Annotated(PhysicalType.FIXED_LEN_BYTE_ARRAY, 2, LogicalType.float16()),
                new Annotated(PhysicalType.FIXED_LEN_BYTE_ARRAY, 12, LogicalType.interval()),
                new Annotated(PhysicalType.FIXED_LEN_BYTE_ARRAY, 8, LogicalType.decimal(18, 3)));
    }

    record Annotated(PhysicalType type, Integer typeLength, LogicalType logicalType) {
        @Override
        public String toString() {
            return logicalType + " on " + type + (typeLength == null ? "" : "(" + typeLength + ")");
        }
    }

    @ParameterizedTest
    @MethodSource("annotations")
    void annotationSurvivesTheFile(Annotated annotated) throws Exception {
        ByteBufferOutputFile out = writeOneRow(annotated);

        try (ParquetFileReader reader = openReader(out)) {
            assertThat(reader.getFileSchema().getColumn("v").logicalType()).isEqualTo(annotated.logicalType());
        }
    }

    @Test
    void nestedAndNullableColumnsCarryTheirAnnotation() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .struct("person", RepetitionType.OPTIONAL, person -> person
                        .addColumn("name", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
                                LogicalType.string())
                        .addColumn("born", PhysicalType.INT32, RepetitionType.REQUIRED,
                                LogicalType.date()))
                .list("tags", RepetitionType.OPTIONAL, element -> element.primitive(
                        PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, LogicalType.string()))
                .build();

        Validity present = Validity.ofNulls(new boolean[] { false, false });
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch
                    .struct("person", present)
                    .bytes("person.name", new byte[][] { bytes("ada"), bytes("alan") }, present)
                    .ints("person.born", new int[] { 1, 2 })
                    .list("tags", new int[] { 0, 1, 2 }, present)
                    .bytes("tags.list.element", new byte[][] { bytes("x"), bytes("y") }, present));
        }

        try (ParquetFileReader reader = openReader(out)) {
            FileSchema readBack = reader.getFileSchema();
            assertThat(readBack.getColumn("person.name").logicalType()).isEqualTo(LogicalType.string());
            assertThat(readBack.getColumn("person.born").logicalType()).isEqualTo(LogicalType.date());
            assertThat(readBack.getColumn("tags.list.element").logicalType())
                    .isEqualTo(LogicalType.string());
        }
    }

    /// An annotation whose sort order matches the collector's keeps its bounds: a `DATE` is an
    /// `INT32` compared as a signed integer, exactly as an unannotated one is.
    @Test
    void signedOrderAnnotationsKeepTheirBounds() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT32, RepetitionType.REQUIRED, LogicalType.date())
                .build();

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, new int[] { 5, -3, 9 }));
        }

        try (ParquetFileReader reader = openReader(out)) {
            Statistics statistics = columnMeta(reader, 0).statistics();
            assertThat(toInt(statistics.minValue())).isEqualTo(-3);
            assertThat(toInt(statistics.maxValue())).isEqualTo(9);
        }
    }

    /// parquet-format leaves the ordering of these annotations undefined, so no `min` / `max`
    /// can be written for them — a reader has no way to interpret a bound. The null count still
    /// supports pushdown.
    @ParameterizedTest
    @MethodSource("unorderedAnnotations")
    void columnsWithoutAWellDefinedOrderWriteNoBounds(Annotated annotated) throws Exception {
        ByteBufferOutputFile out = writeOneRow(annotated);

        try (ParquetFileReader reader = openReader(out)) {
            Statistics statistics = columnMeta(reader, 0).statistics();
            assertThat(statistics.minValue()).isNull();
            assertThat(statistics.maxValue()).isNull();
            assertThat(statistics.nullCount()).isZero();
        }
    }

    static Stream<Annotated> unorderedAnnotations() {
        return Stream.of(
                new Annotated(PhysicalType.FIXED_LEN_BYTE_ARRAY, 12, LogicalType.interval()),
                new Annotated(PhysicalType.BYTE_ARRAY, null, LogicalType.geometry("EPSG:4326")),
                new Annotated(PhysicalType.BYTE_ARRAY, null,
                        LogicalType.geography("EPSG:4326", EdgeInterpolationAlgorithm.KARNEY)));
    }

    /// `UNKNOWN` describes a column holding only nulls, so its ordering is undefined and it
    /// writes a null count alone.
    @Test
    void unknownColumnWritesOnlyItsNullCount() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT32, RepetitionType.OPTIONAL, LogicalType.nullType())
                .build();

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, new int[] { 0, 0, 0 },
                    Validity.ofNulls(new boolean[] { true, true, true })));
        }

        try (ParquetFileReader reader = openReader(out)) {
            assertThat(reader.getFileSchema().getColumn("v").logicalType()).isEqualTo(LogicalType.nullType());
            Statistics statistics = columnMeta(reader, 0).statistics();
            assertThat(statistics.minValue()).isNull();
            assertThat(statistics.maxValue()).isNull();
            assertThat(statistics.nullCount()).isEqualTo(3L);
        }
    }

    /// An integer-backed `DECIMAL` is compared as a signed integer, which is what the collector
    /// already does, so those columns keep their bounds.
    @Test
    void integerBackedDecimalKeepsItsBounds() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT32, RepetitionType.REQUIRED, LogicalType.decimal(9, 2))
                .build();

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, new int[] { 1234, -99, 5000 }));
        }

        try (ParquetFileReader reader = openReader(out)) {
            Statistics statistics = columnMeta(reader, 0).statistics();
            assertThat(toInt(statistics.minValue())).isEqualTo(-99);
            assertThat(toInt(statistics.maxValue())).isEqualTo(5000);
        }
    }

    /// Writes a single row into the annotated column, enough to exercise the schema
    /// serialization and the chunk statistics.
    private static ByteBufferOutputFile writeOneRow(Annotated annotated) throws Exception {
        FileSchema.Builder builder = FileSchema.builder("schema");
        if (annotated.typeLength() == null) {
            builder.addColumn("v", annotated.type(), RepetitionType.REQUIRED, annotated.logicalType());
        }
        else {
            builder.addColumn("v", annotated.type(), RepetitionType.REQUIRED, annotated.typeLength(),
                    annotated.logicalType());
        }

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, builder.build())) {
            writer.columnWriter().writeBatch(batch -> writeValue(batch, annotated));
        }
        return out;
    }

    private static void writeValue(ColumnBatch batch, Annotated annotated) {
        switch (annotated.type()) {
            case BOOLEAN -> batch.booleans(0, new boolean[] { true });
            case INT32 -> batch.ints(0, new int[] { 42 });
            case INT64 -> batch.longs(0, new long[] { 42L });
            case FLOAT -> batch.floats(0, new float[] { 42F });
            case DOUBLE -> batch.doubles(0, new double[] { 42D });
            case BYTE_ARRAY -> batch.bytes(0, new byte[][] { bytes("value") });
            case FIXED_LEN_BYTE_ARRAY -> batch.fixed(0, new byte[][] { new byte[annotated.typeLength()] });
            default -> throw new IllegalArgumentException("Unsupported physical type " + annotated.type());
        }
    }

    private static int toInt(byte[] value) {
        return ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static ParquetFileReader openReader(ByteBufferOutputFile out) throws Exception {
        return ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())));
    }

    private static ColumnMetaData columnMeta(ParquetFileReader reader, int columnIndex) {
        return reader.getFileMetaData().rowGroups().get(0).columns().get(columnIndex).metaData();
    }
}
