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
                LogicalType.string(),
                LogicalType.map(),
                LogicalType.list(),
                LogicalType.enumType(),
                LogicalType.date(),
                LogicalType.json(),
                LogicalType.bson(),
                LogicalType.nullType(),
                LogicalType.uuid(),
                LogicalType.float16(),
                LogicalType.decimal(1, 0),
                LogicalType.decimal(9, 2),
                LogicalType.decimal(38, 38),
                LogicalType.variant(1),
                LogicalType.variant(2),
                LogicalType.geometry("EPSG:4326"),
                LogicalType.geography("EPSG:4326", EdgeInterpolationAlgorithm.KARNEY));
    }

    static Stream<LogicalType> parameterlessMembers() {
        return roundTripped().filter(logicalType -> logicalType.getClass().getRecordComponents().length == 0);
    }

    static Stream<LogicalType> timeAndTimestampVariants() {
        return Stream.of(TimeUnit.values())
                .flatMap(unit -> Stream.of(true, false)
                        .flatMap(utc -> Stream.of(
                                LogicalType.time(utc, unit),
                                LogicalType.timestamp(utc, unit))));
    }

    static Stream<LogicalType> intVariants() {
        return Stream.of(8, 16, 32, 64)
                .flatMap(width -> Stream.of(
                        LogicalType.intType(width, true),
                        LogicalType.intType(width, false)));
    }

    /// Every algorithm the format defines. `UNKNOWN` is excluded: it is the reader's placeholder
    /// for one added later, and is rejected rather than written — see
    /// [#unrecognizedEdgeInterpolationIsRejected].
    static Stream<LogicalType> edgeInterpolations() {
        return Stream.of(EdgeInterpolationAlgorithm.values())
                .filter(algorithm -> algorithm != EdgeInterpolationAlgorithm.UNKNOWN)
                .map(algorithm -> LogicalType.geography("OGC:CRS84", algorithm));
    }

    @ParameterizedTest
    @MethodSource({ "roundTripped", "timeAndTimestampVariants", "intVariants", "edgeInterpolations" })
    void readsBackEqual(LogicalType logicalType) throws Exception {
        assertThat(roundTrip(logicalType)).isEqualTo(logicalType);
    }

    /// The members carrying no parameters have one value, and a footer names it once per column
    /// that is annotated with it. The reader hands back the shared instance rather than
    /// allocating one per column.
    @ParameterizedTest
    @MethodSource("parameterlessMembers")
    void parameterlessMembersReadBackAsTheSharedInstance(LogicalType logicalType) throws Exception {
        assertThat(roundTrip(logicalType)).isSameAs(logicalType);
    }

    /// The CRS is optional on the wire, and the reader substitutes the spec's default for an
    /// absent one. Only the record constructor expresses that absence — the factories
    /// substitute the default themselves — so it is what builds the annotation here.
    @Test
    void absentCrsReadsBackAsTheDefault() throws Exception {
        assertThat(roundTrip(new LogicalType.GeometryType(null)))
                .isEqualTo(LogicalType.geometry("OGC:CRS84"));
        assertThat(roundTrip(new LogicalType.GeographyType(null, EdgeInterpolationAlgorithm.VINCENTY)))
                .isEqualTo(LogicalType.geography("OGC:CRS84", EdgeInterpolationAlgorithm.VINCENTY));
    }

    /// An algorithm the reader could not name decodes to `UNKNOWN`, which names no union member.
    /// Writing it would have to invent an algorithm the values were not interpolated with, so a
    /// schema read from a newer file fails on the way back out instead.
    @Test
    void unrecognizedEdgeInterpolationIsRejected() {
        LogicalType geography = LogicalType.geography("OGC:CRS84", EdgeInterpolationAlgorithm.UNKNOWN);

        assertThatThrownBy(() -> LogicalTypeWriter.write(new ThriftCompactWriter(), geography))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("No Thrift value for edge interpolation algorithm: UNKNOWN");
    }

    /// parquet.thrift reserves union field 9 for INTERVAL without defining the member struct,
    /// so an interval column is annotated by its legacy `converted_type` alone.
    @Test
    void intervalHasNoUnionMember() {
        assertThatThrownBy(() -> LogicalTypeWriter.write(new ThriftCompactWriter(), LogicalType.interval()))
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
        LogicalTypeWriter.write(writer, LogicalType.timestamp(false, TimeUnit.NANOS));
        writer.writeFieldBegin(2, FieldType.I32);
        writer.writeI32(7);

        ThriftCompactReader reader = new ThriftCompactReader(ByteBuffer.wrap(writer.toByteArray()));
        assertThat(ThriftCompactReader.fieldId(reader.readFieldHeader())).isEqualTo((short) 1);
        assertThat(LogicalTypeReader.read(reader))
                .isEqualTo(LogicalType.timestamp(false, TimeUnit.NANOS));

        assertThat(ThriftCompactReader.fieldId(reader.readFieldHeader())).isEqualTo((short) 2);
        assertThat(reader.readI32()).isEqualTo(7);
    }

    private static LogicalType roundTrip(LogicalType logicalType) throws Exception {
        ThriftCompactWriter writer = new ThriftCompactWriter();
        LogicalTypeWriter.write(writer, logicalType);
        return LogicalTypeReader.read(new ThriftCompactReader(ByteBuffer.wrap(writer.toByteArray())));
    }
}
