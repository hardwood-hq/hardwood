/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.stream.Stream;

import org.apache.parquet.format.BsonType;
import org.apache.parquet.format.DateType;
import org.apache.parquet.format.DecimalType;
import org.apache.parquet.format.EdgeInterpolationAlgorithm;
import org.apache.parquet.format.EnumType;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.Float16Type;
import org.apache.parquet.format.GeographyType;
import org.apache.parquet.format.GeometryType;
import org.apache.parquet.format.IntType;
import org.apache.parquet.format.JsonType;
import org.apache.parquet.format.ListType;
import org.apache.parquet.format.LogicalType;
import org.apache.parquet.format.MapType;
import org.apache.parquet.format.MicroSeconds;
import org.apache.parquet.format.MilliSeconds;
import org.apache.parquet.format.NanoSeconds;
import org.apache.parquet.format.NullType;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.StringType;
import org.apache.parquet.format.TimeType;
import org.apache.parquet.format.TimeUnit;
import org.apache.parquet.format.TimestampType;
import org.apache.parquet.format.Type;
import org.apache.parquet.format.UUIDType;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.VariantType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.internal.schema.AnnotationKind;
import shaded.parquet.org.apache.thrift.protocol.TCompactProtocol;
import shaded.parquet.org.apache.thrift.transport.TIOStreamTransport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/// Checks Hardwood's hand-written readers and writers of the `LogicalType` union against
/// parquet-java's, an independent implementation of the same spec.
///
/// A fixture built by this project's own datagen only proves the reader agrees with the writer
/// that produced it: both can encode a field the same wrong way and the round trip still passes.
/// These cases encode with `org.apache.parquet.format`, so a field whose wire type Hardwood has
/// wrong fails here even when every in-house fixture agrees with itself.
///
/// The reference types share their simple names with Hardwood's metadata records, so the
/// Hardwood side is qualified: `org.apache.parquet.format` is imported, since it dominates the
/// encoding side.
class LogicalTypeReferenceCodecTest {

    /// `GeographyType.algorithm` is `optional EdgeInterpolationAlgorithm`, a Thrift **enum**, so
    /// the reference writes it as an `i32` — not as a struct holding a union whose set variant
    /// names the algorithm. Reading it as the latter skips the field and reports every geography
    /// column as the `SPHERICAL` default (#909).
    @Test
    void geographyAlgorithmMatchesTheReferenceEncoding() throws IOException {
        dev.hardwood.metadata.LogicalType parsed =
                readLogicalType(referenceFooter("EPSG:4326", EdgeInterpolationAlgorithm.THOMAS));

        assertThat(parsed).isInstanceOf(dev.hardwood.metadata.LogicalType.GeographyType.class);
        dev.hardwood.metadata.LogicalType.GeographyType geography =
                (dev.hardwood.metadata.LogicalType.GeographyType) parsed;
        assertThat(geography.crs()).isEqualTo("EPSG:4326");
        assertThat(geography.edgeInterpolation())
                .isEqualTo(dev.hardwood.metadata.LogicalType.EdgeInterpolationAlgorithm.THOMAS);
    }

    /// Every algorithm the reference knows decodes to the constant of the same name, which pins
    /// the numbering and not just the wire type — the mapping was off by one as well.
    @Test
    void everyReferenceAlgorithmDecodesToItsOwnConstant() throws IOException {
        for (EdgeInterpolationAlgorithm algorithm : EdgeInterpolationAlgorithm.values()) {
            dev.hardwood.metadata.LogicalType parsed =
                    readLogicalType(referenceFooter("OGC:CRS84", algorithm));
            assertThat(((dev.hardwood.metadata.LogicalType.GeographyType) parsed).edgeInterpolation().name())
                    .as("algorithm %s (thrift value %d)", algorithm.name(), algorithm.getValue())
                    .isEqualTo(algorithm.name());
        }
    }

    /// Every annotation Hardwood models with a `LogicalType` union member, with each parameter
    /// value the wire distinguishes: every unit of `TIME` and `TIMESTAMP` under both UTC flags,
    /// every `INT` width in both signs, and every edge interpolation algorithm. The samples are
    /// chosen per [AnnotationKind] by an exhaustive switch, so a new member does not compile until
    /// it has some.
    static Stream<dev.hardwood.metadata.LogicalType> unionMembers() {
        return Stream.of(AnnotationKind.values()).flatMap(LogicalTypeReferenceCodecTest::samples);
    }

    /// The field id and parameters of every union member, as the reference encodes them, decode
    /// to the Hardwood record of the same member. Hardwood's reader and writer both take the field
    /// id from [AnnotationKind], so their round trip alone would pass a wrong id.
    @ParameterizedTest
    @MethodSource("unionMembers")
    void theReferenceEncodingDecodesToTheSameAnnotation(dev.hardwood.metadata.LogicalType annotation)
            throws Exception {
        byte[] encoded = encodeWithReference(reference(annotation));

        assertThat(LogicalTypeReader.read(new ThriftCompactReader(ByteBuffer.wrap(encoded))))
                .isEqualTo(annotation);
    }

    /// The reference decodes what Hardwood's writer encodes to the same member with the same
    /// parameters.
    @ParameterizedTest
    @MethodSource("unionMembers")
    void hardwoodsEncodingDecodesToTheSameReferenceMember(dev.hardwood.metadata.LogicalType annotation)
            throws Exception {
        ThriftCompactWriter writer = new ThriftCompactWriter();
        LogicalTypeWriter.write(writer, annotation);

        LogicalType decoded = decodeWithReference(writer.toByteArray());
        LogicalType expected = reference(annotation);
        assertThat(decoded.getSetField()).isEqualTo(expected.getSetField());
        assertThat(decoded).isEqualTo(expected);
    }

    /// `parquet.thrift` reserves union field 9 for `INTERVAL` without defining a member there, so
    /// Hardwood has none to write (`LogicalTypeWriterTest` pins the writer's refusal).
    @Test
    void intervalHasNoUnionMember() {
        assertThat(LogicalType._Fields.findByThriftId(9)).isNull();
        assertThat(AnnotationKind.INTERVAL.hasUnionMember()).isFalse();
    }

    private static Stream<dev.hardwood.metadata.LogicalType> samples(AnnotationKind kind) {
        return switch (kind) {
            case STRING -> Stream.of(dev.hardwood.metadata.LogicalType.string());
            case MAP -> Stream.of(dev.hardwood.metadata.LogicalType.map());
            case LIST -> Stream.of(dev.hardwood.metadata.LogicalType.list());
            case ENUM -> Stream.of(dev.hardwood.metadata.LogicalType.enumType());
            case DECIMAL -> Stream.of(dev.hardwood.metadata.LogicalType.decimal(9, 2),
                    dev.hardwood.metadata.LogicalType.decimal(38, 0));
            case DATE -> Stream.of(dev.hardwood.metadata.LogicalType.date());
            case TIME -> Stream.of(dev.hardwood.metadata.LogicalType.TimeUnit.values())
                    .flatMap(unit -> Stream.of(dev.hardwood.metadata.LogicalType.time(true, unit),
                            dev.hardwood.metadata.LogicalType.time(false, unit)));
            case TIMESTAMP -> Stream.of(dev.hardwood.metadata.LogicalType.TimeUnit.values())
                    .flatMap(unit -> Stream.of(dev.hardwood.metadata.LogicalType.timestamp(true, unit),
                            dev.hardwood.metadata.LogicalType.timestamp(false, unit)));
            // No union member: see intervalHasNoUnionMember.
            case INTERVAL -> Stream.empty();
            case INT -> Stream.of(8, 16, 32, 64)
                    .flatMap(width -> Stream.of(dev.hardwood.metadata.LogicalType.intType(width, true),
                            dev.hardwood.metadata.LogicalType.intType(width, false)));
            case NULL -> Stream.of(dev.hardwood.metadata.LogicalType.nullType());
            case JSON -> Stream.of(dev.hardwood.metadata.LogicalType.json());
            case BSON -> Stream.of(dev.hardwood.metadata.LogicalType.bson());
            case UUID -> Stream.of(dev.hardwood.metadata.LogicalType.uuid());
            case FLOAT16 -> Stream.of(dev.hardwood.metadata.LogicalType.float16());
            case VARIANT -> Stream.of(dev.hardwood.metadata.LogicalType.variant(1));
            case GEOMETRY -> Stream.of(dev.hardwood.metadata.LogicalType.geometry("EPSG:4326"));
            // UNKNOWN is the reader's stand-in for an algorithm it cannot name, never written.
            case GEOGRAPHY -> Stream.of(dev.hardwood.metadata.LogicalType.EdgeInterpolationAlgorithm.values())
                    .filter(algorithm -> algorithm != dev.hardwood.metadata.LogicalType.EdgeInterpolationAlgorithm.UNKNOWN)
                    .map(algorithm -> dev.hardwood.metadata.LogicalType.geography("EPSG:4326", algorithm));
        };
    }

    /// The reference union member that stands for `annotation`, built through the reference's
    /// own setter of the member of that name, so its field id is the reference's.
    private static LogicalType reference(dev.hardwood.metadata.LogicalType annotation) {
        return switch (annotation) {
            case dev.hardwood.metadata.LogicalType.StringType ignored -> LogicalType.STRING(new StringType());
            case dev.hardwood.metadata.LogicalType.MapType ignored -> LogicalType.MAP(new MapType());
            case dev.hardwood.metadata.LogicalType.ListType ignored -> LogicalType.LIST(new ListType());
            case dev.hardwood.metadata.LogicalType.EnumType ignored -> LogicalType.ENUM(new EnumType());
            case dev.hardwood.metadata.LogicalType.DecimalType decimal ->
                    LogicalType.DECIMAL(new DecimalType(decimal.scale(), decimal.precision()));
            case dev.hardwood.metadata.LogicalType.DateType ignored -> LogicalType.DATE(new DateType());
            case dev.hardwood.metadata.LogicalType.TimeType time ->
                    LogicalType.TIME(new TimeType(time.isAdjustedToUTC(), reference(time.unit())));
            case dev.hardwood.metadata.LogicalType.TimestampType timestamp ->
                    LogicalType.TIMESTAMP(new TimestampType(timestamp.isAdjustedToUTC(), reference(timestamp.unit())));
            case dev.hardwood.metadata.LogicalType.IntervalType ignored ->
                    throw new IllegalArgumentException("INTERVAL has no union member");
            case dev.hardwood.metadata.LogicalType.IntType integer ->
                    LogicalType.INTEGER(new IntType((byte) integer.bitWidth(), integer.isSigned()));
            case dev.hardwood.metadata.LogicalType.NullType ignored -> LogicalType.UNKNOWN(new NullType());
            case dev.hardwood.metadata.LogicalType.JsonType ignored -> LogicalType.JSON(new JsonType());
            case dev.hardwood.metadata.LogicalType.BsonType ignored -> LogicalType.BSON(new BsonType());
            case dev.hardwood.metadata.LogicalType.UuidType ignored -> LogicalType.UUID(new UUIDType());
            case dev.hardwood.metadata.LogicalType.Float16Type ignored -> LogicalType.FLOAT16(new Float16Type());
            case dev.hardwood.metadata.LogicalType.VariantType variant -> {
                VariantType member = new VariantType();
                member.setSpecification_version((byte) variant.specVersion());
                yield LogicalType.VARIANT(member);
            }
            case dev.hardwood.metadata.LogicalType.GeometryType geometry -> {
                GeometryType member = new GeometryType();
                member.setCrs(geometry.crs());
                yield LogicalType.GEOMETRY(member);
            }
            case dev.hardwood.metadata.LogicalType.GeographyType geography -> {
                GeographyType member = new GeographyType();
                member.setCrs(geography.crs());
                member.setAlgorithm(EdgeInterpolationAlgorithm.valueOf(geography.edgeInterpolation().name()));
                yield LogicalType.GEOGRAPHY(member);
            }
        };
    }

    private static TimeUnit reference(dev.hardwood.metadata.LogicalType.TimeUnit unit) {
        return switch (unit) {
            case MILLIS -> TimeUnit.MILLIS(new MilliSeconds());
            case MICROS -> TimeUnit.MICROS(new MicroSeconds());
            case NANOS -> TimeUnit.NANOS(new NanoSeconds());
        };
    }

    private static byte[] encodeWithReference(LogicalType annotation) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        annotation.write(new TCompactProtocol(new TIOStreamTransport(out)));
        return out.toByteArray();
    }

    private static LogicalType decodeWithReference(byte[] encoded) throws Exception {
        LogicalType decoded = new LogicalType();
        decoded.read(new TCompactProtocol(new TIOStreamTransport(new ByteArrayInputStream(encoded))));
        return decoded;
    }

    /// A footer holding one `GEOGRAPHY` column, serialized by the reference implementation.
    private static byte[] referenceFooter(String crs, EdgeInterpolationAlgorithm algorithm)
            throws IOException {
        GeographyType geography = new GeographyType();
        geography.setCrs(crs);
        geography.setAlgorithm(algorithm);

        SchemaElement root = new SchemaElement("schema");
        root.setNum_children(1);
        SchemaElement column = new SchemaElement("city_geom");
        column.setType(Type.BYTE_ARRAY);
        column.setRepetition_type(FieldRepetitionType.OPTIONAL);
        column.setLogicalType(LogicalType.GEOGRAPHY(geography));

        FileMetaData metaData = new FileMetaData(1, List.of(root, column), 0, List.of());
        metaData.setCreated_by("parquet-format-structures (reference)");

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Util.writeFileMetaData(metaData, out);
        return out.toByteArray();
    }

    /// Parses the footer with Hardwood and returns the geography column's logical type.
    private static dev.hardwood.metadata.LogicalType readLogicalType(byte[] footer) {
        ThriftCompactReader reader = new ThriftCompactReader(
                ByteBuffer.wrap(footer).order(ByteOrder.LITTLE_ENDIAN));
        dev.hardwood.metadata.FileMetaData metaData =
                assertDoesNotThrow(() -> FileMetaDataReader.read(reader));
        return metaData.schema().get(1).logicalType();
    }
}
