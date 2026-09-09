/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.internal.thrift.ThriftCompactConstants.FieldType;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.LogicalType.EdgeInterpolationAlgorithm;
import dev.hardwood.metadata.LogicalType.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Unit tests for [LogicalTypeWriter], pinning it as the exact inverse of
/// [LogicalTypeReader]: every union member written here must read back equal.
class LogicalTypeWriterTest {

    static Stream<LogicalType> roundTripped() {
        return Stream.of(
                new LogicalType.StringType(),
                new LogicalType.MapType(),
                new LogicalType.ListType(),
                new LogicalType.EnumType(),
                new LogicalType.DateType(),
                new LogicalType.JsonType(),
                new LogicalType.BsonType(),
                new LogicalType.NullType(),
                new LogicalType.UuidType(),
                new LogicalType.Float16Type(),
                new LogicalType.DecimalType(0, 1),
                new LogicalType.DecimalType(2, 9),
                new LogicalType.DecimalType(38, 38),
                new LogicalType.VariantType(1),
                new LogicalType.VariantType(2),
                new LogicalType.GeometryType("EPSG:4326"),
                new LogicalType.GeographyType("EPSG:4326", EdgeInterpolationAlgorithm.KARNEY));
    }

    static Stream<LogicalType> timeAndTimestampVariants() {
        return Stream.of(TimeUnit.values())
                .flatMap(unit -> Stream.of(true, false)
                        .flatMap(utc -> Stream.of(
                                new LogicalType.TimeType(utc, unit),
                                new LogicalType.TimestampType(utc, unit))));
    }

    static Stream<LogicalType> intVariants() {
        return Stream.of(8, 16, 32, 64)
                .flatMap(width -> Stream.of(
                        new LogicalType.IntType(width, true),
                        new LogicalType.IntType(width, false)));
    }

    /// Every algorithm the format defines. `UNKNOWN` is excluded: it is the reader's placeholder
    /// for one added later, and is rejected rather than written — see
    /// [#unrecognizedEdgeInterpolationIsRejected].
    static Stream<LogicalType> edgeInterpolations() {
        return Stream.of(EdgeInterpolationAlgorithm.values())
                .filter(algorithm -> algorithm != EdgeInterpolationAlgorithm.UNKNOWN)
                .map(algorithm -> new LogicalType.GeographyType("OGC:CRS84", algorithm));
    }

    @ParameterizedTest
    @MethodSource({ "roundTripped", "timeAndTimestampVariants", "intVariants", "edgeInterpolations" })
    void readsBackEqual(LogicalType logicalType) throws Exception {
        assertThat(roundTrip(logicalType)).isEqualTo(logicalType);
    }

    /// The reader substitutes the spec's default CRS for an absent one, so a geospatial
    /// annotation without a CRS reads back as the default rather than as null.
    @Test
    void absentCrsReadsBackAsTheDefault() throws Exception {
        assertThat(roundTrip(new LogicalType.GeometryType(null)))
                .isEqualTo(new LogicalType.GeometryType("OGC:CRS84"));
        assertThat(roundTrip(new LogicalType.GeographyType(null, EdgeInterpolationAlgorithm.VINCENTY)))
                .isEqualTo(new LogicalType.GeographyType("OGC:CRS84", EdgeInterpolationAlgorithm.VINCENTY));
    }

    /// An algorithm the reader could not name decodes to `UNKNOWN`, which names no union member.
    /// Writing it would have to invent an algorithm the values were not interpolated with, so a
    /// schema read from a newer file fails on the way back out instead.
    @Test
    void unrecognizedEdgeInterpolationIsRejected() {
        LogicalType geography = new LogicalType.GeographyType("OGC:CRS84", EdgeInterpolationAlgorithm.UNKNOWN);

        assertThatThrownBy(() -> LogicalTypeWriter.write(new ThriftCompactWriter(), geography))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("No Thrift value for edge interpolation algorithm: UNKNOWN");
    }

    /// parquet.thrift reserves union field 9 for INTERVAL without defining the member struct,
    /// so an interval column is annotated by its legacy `converted_type` alone.
    @Test
    void intervalHasNoUnionMember() {
        assertThatThrownBy(() -> LogicalTypeWriter.write(new ThriftCompactWriter(), new LogicalType.IntervalType()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("INTERVAL has no LogicalType union member and is written as the legacy "
                         + "converted_type only");
    }

    /// The union must terminate exactly at its own STOP marker: a member struct that leaked a
    /// field-id context or an unbalanced STOP would desync everything written after it.
    @Test
    void unionEndsAtItsOwnStop() throws Exception {
        ThriftCompactWriter writer = new ThriftCompactWriter();
        writer.pushFieldIdContext();
        writer.writeFieldBegin(1, FieldType.STRUCT);
        LogicalTypeWriter.write(writer, new LogicalType.TimestampType(false, TimeUnit.NANOS));
        writer.writeFieldBegin(2, FieldType.I32);
        writer.writeI32(7);

        ThriftCompactReader reader = new ThriftCompactReader(ByteBuffer.wrap(writer.toByteArray()));
        assertThat(ThriftCompactReader.fieldId(reader.readFieldHeader())).isEqualTo((short) 1);
        assertThat(LogicalTypeReader.read(reader))
                .isEqualTo(new LogicalType.TimestampType(false, TimeUnit.NANOS));

        assertThat(ThriftCompactReader.fieldId(reader.readFieldHeader())).isEqualTo((short) 2);
        assertThat(reader.readI32()).isEqualTo(7);
    }

    private static LogicalType roundTrip(LogicalType logicalType) throws Exception {
        ThriftCompactWriter writer = new ThriftCompactWriter();
        LogicalTypeWriter.write(writer, logicalType);
        return LogicalTypeReader.read(new ThriftCompactReader(ByteBuffer.wrap(writer.toByteArray())));
    }
}
