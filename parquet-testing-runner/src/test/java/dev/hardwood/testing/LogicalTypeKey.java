/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.testing;

import java.util.Optional;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.BsonLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.EnumLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.Float16LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.GeographyLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.GeometryLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntervalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.JsonLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.ListLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.LogicalTypeAnnotationVisitor;
import org.apache.parquet.schema.LogicalTypeAnnotation.MapLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.UnknownLogicalTypeAnnotation;

import dev.hardwood.metadata.LogicalType;

/// One spelling of a logical-type annotation that both implementations produce, so a
/// requirement expressed in Hardwood's `LogicalType` and an observation read out of a file by
/// parquet-java name the same cell.
///
/// The spelling carries exactly the parameters that make two annotations behave differently:
/// an `INT`'s width and signedness, a `DECIMAL`'s precision and scale, a `TIME` or `TIMESTAMP`'s
/// unit and UTC adjustment. A `GEOMETRY`'s coordinate reference system is not among them — it
/// travels with the annotation but changes nothing about how the values are written — so every
/// CRS spells the same cell.
///
/// Neither side is derived from the other. Where they disagree the observation lands on a cell
/// no requirement names, which leaves the requirement uncovered rather than quietly satisfied.
final class LogicalTypeKey {

    /// The spelling of a column carrying no annotation at all.
    static final String NONE = "NONE";

    private LogicalTypeKey() {
    }

    /// The key of a Hardwood annotation. The switch is exhaustive over the sealed hierarchy, so
    /// a new member does not compile until it is spelled here — which is what keeps the domain
    /// growing with the writer.
    ///
    /// @param logicalType the annotation, or `null` for an unannotated column
    /// @return the canonical key
    static String of(LogicalType logicalType) {
        return switch (logicalType) {
            case null -> NONE;
            case LogicalType.StringType ignored -> "STRING";
            case LogicalType.EnumType ignored -> "ENUM";
            case LogicalType.UuidType ignored -> "UUID";
            case LogicalType.IntType type -> intKey(type.bitWidth(), type.isSigned());
            case LogicalType.DecimalType type -> decimalKey(type.precision(), type.scale());
            case LogicalType.DateType ignored -> "DATE";
            case LogicalType.TimeType type -> timeKey("TIME", type.unit().name(), type.isAdjustedToUTC());
            case LogicalType.TimestampType type ->
                    timeKey("TIMESTAMP", type.unit().name(), type.isAdjustedToUTC());
            case LogicalType.IntervalType ignored -> "INTERVAL";
            case LogicalType.JsonType ignored -> "JSON";
            case LogicalType.BsonType ignored -> "BSON";
            case LogicalType.ListType ignored -> "LIST";
            case LogicalType.MapType ignored -> "MAP";
            case LogicalType.VariantType ignored -> "VARIANT";
            case LogicalType.GeometryType ignored -> "GEOMETRY";
            case LogicalType.GeographyType ignored -> "GEOGRAPHY";
            case LogicalType.Float16Type ignored -> "FLOAT16";
            case LogicalType.FileType ignored -> "FILE";
            case LogicalType.NullType ignored -> "UNKNOWN";
        };
    }

    /// The key of the annotation parquet-java read out of a file.
    ///
    /// An annotation the visitor does not spell — one parquet-java models and Hardwood does not
    /// write — keys as itself, which matches no requirement and is therefore visible rather
    /// than silently folded into a covered cell.
    ///
    /// @param annotation the annotation parquet-java parsed, or `null` for an unannotated column
    /// @return the canonical key
    static String of(LogicalTypeAnnotation annotation) {
        if (annotation == null) {
            return NONE;
        }
        return annotation.accept(VISITOR).orElseGet(() -> "UNMAPPED(" + annotation + ")");
    }

    private static final LogicalTypeAnnotationVisitor<String> VISITOR = new LogicalTypeAnnotationVisitor<>() {

        @Override
        public Optional<String> visit(StringLogicalTypeAnnotation ignored) {
            return Optional.of("STRING");
        }

        @Override
        public Optional<String> visit(EnumLogicalTypeAnnotation ignored) {
            return Optional.of("ENUM");
        }

        @Override
        public Optional<String> visit(UUIDLogicalTypeAnnotation ignored) {
            return Optional.of("UUID");
        }

        @Override
        public Optional<String> visit(IntLogicalTypeAnnotation annotation) {
            return Optional.of(intKey(annotation.getBitWidth(), annotation.isSigned()));
        }

        @Override
        public Optional<String> visit(DecimalLogicalTypeAnnotation annotation) {
            return Optional.of(decimalKey(annotation.getPrecision(), annotation.getScale()));
        }

        @Override
        public Optional<String> visit(DateLogicalTypeAnnotation ignored) {
            return Optional.of("DATE");
        }

        @Override
        public Optional<String> visit(TimeLogicalTypeAnnotation annotation) {
            return Optional.of(
                    timeKey("TIME", annotation.getUnit().name(), annotation.isAdjustedToUTC()));
        }

        @Override
        public Optional<String> visit(TimestampLogicalTypeAnnotation annotation) {
            return Optional.of(
                    timeKey("TIMESTAMP", annotation.getUnit().name(), annotation.isAdjustedToUTC()));
        }

        @Override
        public Optional<String> visit(IntervalLogicalTypeAnnotation ignored) {
            return Optional.of("INTERVAL");
        }

        @Override
        public Optional<String> visit(JsonLogicalTypeAnnotation ignored) {
            return Optional.of("JSON");
        }

        @Override
        public Optional<String> visit(BsonLogicalTypeAnnotation ignored) {
            return Optional.of("BSON");
        }

        @Override
        public Optional<String> visit(ListLogicalTypeAnnotation ignored) {
            return Optional.of("LIST");
        }

        @Override
        public Optional<String> visit(MapLogicalTypeAnnotation ignored) {
            return Optional.of("MAP");
        }

        @Override
        public Optional<String> visit(GeometryLogicalTypeAnnotation ignored) {
            return Optional.of("GEOMETRY");
        }

        @Override
        public Optional<String> visit(GeographyLogicalTypeAnnotation ignored) {
            return Optional.of("GEOGRAPHY");
        }

        @Override
        public Optional<String> visit(Float16LogicalTypeAnnotation ignored) {
            return Optional.of("FLOAT16");
        }

        @Override
        public Optional<String> visit(UnknownLogicalTypeAnnotation ignored) {
            return Optional.of("UNKNOWN");
        }
    };

    private static String intKey(int bitWidth, boolean signed) {
        return "INT(" + bitWidth + "," + (signed ? "SIGNED" : "UNSIGNED") + ")";
    }

    private static String decimalKey(int precision, int scale) {
        return "DECIMAL(" + precision + "," + scale + ")";
    }

    private static String timeKey(String name, String unit, boolean adjustedToUtc) {
        return name + "(" + unit + "," + (adjustedToUtc ? "UTC" : "LOCAL") + ")";
    }
}
