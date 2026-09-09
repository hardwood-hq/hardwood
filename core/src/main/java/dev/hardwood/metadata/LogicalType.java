/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

/// Logical types that provide semantic meaning to physical types.
/// Sealed interface allows for parameterized types (e.g., DECIMAL with scale/precision).
///
/// Construct a member through the static factory of the same name — [#string()],
/// [#decimal(int, int)], and so on. The parameterless factories return a shared instance, so
/// calling one twice yields the same object.
///
/// Each implementation's `toString()` renders the Parquet-style annotation token shown in
/// schema dumps and metadata views — `STRING`, `LIST`, `DECIMAL(10, 2)`, `TIMESTAMP(MICROS, UTC)`,
/// and so on — not the default record representation.
///
/// @see <a href="https://parquet.apache.org/docs/file-format/types/logicaltypes/">File Format – Logical Types</a>
/// @see <a href="https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift">parquet.thrift</a>
public sealed
interface LogicalType
permits LogicalType.StringType,LogicalType.EnumType,LogicalType.UuidType,LogicalType.IntType,LogicalType.DecimalType,LogicalType.DateType,LogicalType.TimeType,LogicalType.TimestampType,LogicalType.IntervalType,LogicalType.JsonType,LogicalType.BsonType,LogicalType.ListType,LogicalType.MapType,LogicalType.VariantType,LogicalType.GeometryType,LogicalType.GeographyType,LogicalType.Float16Type,LogicalType.NullType
{

    /// The `STRING` annotation, for a UTF-8 encoded string.
    static StringType string() {
        return StringType.INSTANCE;
    }

    /// The `NULL` annotation, for a column whose every value is null.
    static NullType nullType() {
        return NullType.INSTANCE;
    }

    /// The `ENUM` annotation, for an enum stored as a UTF-8 string.
    static EnumType enumType() {
        return EnumType.INSTANCE;
    }

    /// The `UUID` annotation, for a UUID stored as a 16-byte fixed-length byte array.
    static UuidType uuid() {
        return UuidType.INSTANCE;
    }

    /// The `DATE` annotation, for a calendar date in days since the Unix epoch.
    static DateType date() {
        return DateType.INSTANCE;
    }

    /// The `JSON` annotation, for a JSON document stored as a UTF-8 string.
    static JsonType json() {
        return JsonType.INSTANCE;
    }

    /// The `BSON` annotation, for a BSON document stored as a byte array.
    static BsonType bson() {
        return BsonType.INSTANCE;
    }

    /// The `INTERVAL` annotation, for an interval stored as a 12-byte fixed-length byte array.
    static IntervalType interval() {
        return IntervalType.INSTANCE;
    }

    /// The `FLOAT16` annotation, for an IEEE 754 half-precision float stored as a 2-byte
    /// fixed-length byte array.
    static Float16Type float16() {
        return Float16Type.INSTANCE;
    }

    /// The `INT` annotation, for an integer of a given bit width and signedness.
    ///
    /// @param bitWidth number of bits (8, 16, 32, or 64)
    /// @param isSigned `true` for signed integers, `false` for unsigned
    static IntType intType(int bitWidth, boolean isSigned) {
        return new IntType(bitWidth, isSigned);
    }

    /// The `DECIMAL` annotation, for a decimal of a given precision and scale.
    ///
    /// @param precision total number of digits (must be positive)
    /// @param scale number of digits after the decimal point
    static DecimalType decimal(int precision, int scale) {
        return new DecimalType(precision, scale);
    }

    /// The `TIME` annotation, for a time of day of a given resolution.
    ///
    /// @param isAdjustedToUTC `true` if the value is normalized to UTC
    /// @param unit time resolution (millis, micros, or nanos)
    static TimeType time(boolean isAdjustedToUTC, TimeUnit unit) {
        return new TimeType(isAdjustedToUTC, unit);
    }

    /// The `TIMESTAMP` annotation, for a timestamp of a given resolution.
    ///
    /// @param isAdjustedToUTC `true` if the value is normalized to UTC
    /// @param unit time resolution (millis, micros, or nanos)
    static TimestampType timestamp(boolean isAdjustedToUTC, TimeUnit unit) {
        return new TimestampType(isAdjustedToUTC, unit);
    }

    /// The `LIST` annotation, for a group holding a repeated element.
    static ListType list() {
        return ListType.INSTANCE;
    }

    /// The `MAP` annotation, for a group holding key-value pairs.
    static MapType map() {
        return MapType.INSTANCE;
    }

    /// The `VARIANT` annotation, for a group holding a self-describing, semi-structured value.
    ///
    /// @param specVersion spec version declared by the writer (currently always `1`)
    static VariantType variant(int specVersion) {
        return new VariantType(specVersion);
    }

    /// The `GEOMETRY` annotation, for planar geospatial values in a given CRS.
    ///
    /// @param crs geospatial coordinate reference system, `OGC:CRS84` when `null`
    static GeometryType geometry(String crs) {
        return new GeometryType(crsOrDefault(crs));
    }

    /// The `GEOGRAPHY` annotation, for geodesic geospatial values in a given CRS.
    ///
    /// @param crs geospatial coordinate reference system, `OGC:CRS84` when `null`
    /// @param edgeInterpolation geodesic formulation, [EdgeInterpolationAlgorithm#SPHERICAL]
    ///        when `null`
    static GeographyType geography(String crs, EdgeInterpolationAlgorithm edgeInterpolation) {
        return new GeographyType(crsOrDefault(crs),
                edgeInterpolation != null ? edgeInterpolation : EdgeInterpolationAlgorithm.SPHERICAL);
    }

    /// The coordinate reference system a geospatial annotation carries when it names none: the
    /// format's default, which the two geospatial factories substitute so that an annotation
    /// built without a CRS matches one read back from a file that omitted the field.
    private static String crsOrDefault(String crs) {
        return crs != null ? crs : "OGC:CRS84";
    }

    private static void requireUnit(TimeUnit unit, String annotation) {
        if (unit == null) {
            throw new IllegalArgumentException(annotation + " requires a time unit");
        }
    }

    /// UTF-8 encoded string.
    record StringType() implements LogicalType {
        private static final StringType INSTANCE = new StringType();

        @Override
        public String toString() {
            return "STRING";
        }
    }

    /// Column whose every value is null. Carries no parameters; the value of
    /// every row in such a column is SQL NULL.
    record NullType() implements LogicalType {
        private static final NullType INSTANCE = new NullType();

        @Override
        public String toString() {
            return "NULL";
        }
    }

    /// Enum stored as a UTF-8 string.
    record EnumType() implements LogicalType {
        private static final EnumType INSTANCE = new EnumType();

        @Override
        public String toString() {
            return "ENUM";
        }
    }

    /// UUID stored as a 16-byte fixed-length byte array.
    record UuidType() implements LogicalType {
        private static final UuidType INSTANCE = new UuidType();

        @Override
        public String toString() {
            return "UUID";
        }
    }

    /// Calendar date (days since Unix epoch).
    record DateType() implements LogicalType {
        private static final DateType INSTANCE = new DateType();

        @Override
        public String toString() {
            return "DATE";
        }
    }

    /// JSON document stored as a UTF-8 string.
    record JsonType() implements LogicalType {
        private static final JsonType INSTANCE = new JsonType();

        @Override
        public String toString() {
            return "JSON";
        }
    }

    /// BSON document stored as a byte array.
    record BsonType() implements LogicalType {
        private static final BsonType INSTANCE = new BsonType();

        @Override
        public String toString() {
            return "BSON";
        }
    }

    /// Interval stored as a 12-byte fixed-length byte array (months, days, millis).
    record IntervalType() implements LogicalType {
        private static final IntervalType INSTANCE = new IntervalType();

        @Override
        public String toString() {
            return "INTERVAL";
        }
    }

    /// IEEE 754 half-precision (binary16) floating point, stored as a 2-byte
    /// `FIXED_LEN_BYTE_ARRAY` in little-endian byte order.
    record Float16Type() implements LogicalType {
        private static final Float16Type INSTANCE = new Float16Type();

        @Override
        public String toString() {
            return "FLOAT16";
        }
    }

    /// Integer type with a specific bit width and signedness.
    ///
    /// @param bitWidth number of bits (8, 16, 32, or 64)
    /// @param isSigned `true` for signed integers, `false` for unsigned
    record IntType(int bitWidth, boolean isSigned) implements LogicalType {
        public IntType {
            if (bitWidth != 8 && bitWidth != 16 && bitWidth != 32 && bitWidth != 64) {
                throw new IllegalArgumentException("Invalid bit width: " + bitWidth);
            }
        }

        @Override
        public String toString() {
            return (isSigned ? "INT" : "UINT") + "_" + bitWidth;
        }
    }

    /// Decimal with fixed precision and scale.
    ///
    /// @param precision total number of digits (must be positive)
    /// @param scale number of digits after the decimal point (must not exceed the precision)
    record DecimalType(int precision, int scale) implements LogicalType {
        public DecimalType {
            if (precision <= 0) {
                throw new IllegalArgumentException("Precision must be positive: " + precision);
            }
            if (scale < 0) {
                throw new IllegalArgumentException("Scale cannot be negative: " + scale);
            }
            if (scale > precision) {
                throw new IllegalArgumentException(
                        "Scale " + scale + " exceeds precision " + precision);
            }
        }

        @Override
        public String toString() {
            return "DECIMAL(" + precision + ", " + scale + ")";
        }
    }

    /// Time of day with configurable precision and UTC adjustment.
    ///
    /// @param isAdjustedToUTC `true` if the value is normalized to UTC
    /// @param unit time resolution (millis, micros, or nanos)
    record TimeType(boolean isAdjustedToUTC, TimeUnit unit) implements LogicalType {
        public TimeType {
            requireUnit(unit, "TIME");
        }

        @Override
        public String toString() {
            return "TIME(" + unit + ", " + (isAdjustedToUTC ? "UTC" : "local") + ")";
        }
    }

    /// Timestamp with configurable precision and UTC adjustment.
    ///
    /// @param isAdjustedToUTC `true` if the value is normalized to UTC
    /// @param unit time resolution (millis, micros, or nanos)
    record TimestampType(boolean isAdjustedToUTC, TimeUnit unit) implements LogicalType {
        public TimestampType {
            requireUnit(unit, "TIMESTAMP");
        }

        @Override
        public String toString() {
            return "TIMESTAMP(" + unit + ", " + (isAdjustedToUTC ? "UTC" : "local") + ")";
        }
    }

    /// List (repeated element) logical type.
    record ListType() implements LogicalType {
        private static final ListType INSTANCE = new ListType();

        @Override
        public String toString() {
            return "LIST";
        }
    }

    /// Map (key-value pairs) logical type.
    record MapType() implements LogicalType {
        private static final MapType INSTANCE = new MapType();

        @Override
        public String toString() {
            return "MAP";
        }
    }

    /// Variant (self-describing, semi-structured) logical type per the Parquet
    /// Variant spec. Annotates a group whose children are `metadata` (binary) and
    /// `value` (binary), optionally with a `typed_value` sibling for shredded form.
    ///
    /// @param specVersion spec version declared by the writer (currently always `1`)
    record VariantType(int specVersion) implements LogicalType {
        public VariantType {
            if (specVersion < 1) {
                throw new IllegalArgumentException("specVersion must be >= 1: " + specVersion);
            }
        }

        @Override
        public String toString() {
            return "VARIANT(" + specVersion + ")";
        }
    }

    /// Geometry type with configurable CRS
    ///
    /// @param crs geospatial coordinate reference system, OGC:CRS84 if absent
    record GeometryType(String crs) implements LogicalType {
        @Override
        public String toString() {
            return crs == null ? "GEOMETRY" : "GEOMETRY(" + crs + ")";
        }
    }

    /// Geography type with configurable CRS
    ///
    /// @param crs geospatial coordinate reference system, OGC:CRS84 if absent
    /// @param edgeInterpolation geodesic formulation
    record GeographyType(String crs, EdgeInterpolationAlgorithm edgeInterpolation) implements LogicalType {
        @Override
        public String toString() {
            StringBuilder params = new StringBuilder();
            if (crs != null) {
                params.append(crs);
            }
            if (edgeInterpolation != null) {
                if (params.length() > 0) {
                    params.append(", ");
                }
                params.append(edgeInterpolation);
            }
            return params.length() == 0 ? "GEOGRAPHY" : "GEOGRAPHY(" + params + ")";
        }
    }

    /// Resolution of time and timestamp logical types.
    enum TimeUnit {
        /// Millisecond resolution.
        MILLIS,
        /// Microsecond resolution.
        MICROS,
        /// Nanosecond resolution.
        NANOS
    }

    /// Geodesic formulations to model and compute the shortest path on Earth.
    enum EdgeInterpolationAlgorithm {
        SPHERICAL,
        VINCENTY,
        THOMAS,
        ANDOYER,
        KARNEY,
        /// An algorithm added to the format after this release. The column's values are
        /// unaffected — the algorithm only says how to interpolate between them — so the file
        /// reads normally and only the name of the model is unavailable.
        UNKNOWN
    }
}
