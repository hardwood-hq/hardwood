/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.conversion;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.UUID;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.row.PqInterval;

/// Converts physical values to their logical type representations.
public class LogicalTypeConverter {

    /// Why `logicalType` cannot be read from a column of this physical type and width, or
    /// `null` when it can.
    ///
    /// A property of the column, not of any value in it, so it is asked once — where the
    /// schema is built, by [dev.hardwood.schema.FileSchema], which drops an annotation this
    /// answers for. Nothing consults it per value, and by the time a conversion below runs
    /// the column has either a sound annotation or none.
    ///
    /// This mirrors what the conversions accept and nothing more. [LogicalTypeValidator]
    /// states the *writer's* rule, which is stricter — it pins `TIME(MILLIS)` to `INT32`
    /// where reading accepts either width — and applying it here would refuse files that
    /// read today.
    ///
    /// @param physicalType the column's physical type
    /// @param typeLength its `FIXED_LEN_BYTE_ARRAY` byte length, `null` for any other type
    /// @param logicalType its annotation, `null` for an unannotated column
    /// @return the reason, or `null` if the annotation can be read from the column
    public static String conversionFault(PhysicalType physicalType, Integer typeLength,
                                         LogicalType logicalType) {
        if (logicalType == null) {
            return null;
        }
        return switch (logicalType) {
            case LogicalType.StringType ignored -> requires(physicalType, "STRING", PhysicalType.BYTE_ARRAY);
            case LogicalType.JsonType ignored -> requires(physicalType, "JSON", PhysicalType.BYTE_ARRAY);
            case LogicalType.EnumType ignored -> requires(physicalType, "ENUM", PhysicalType.BYTE_ARRAY);
            case LogicalType.BsonType ignored -> requires(physicalType, "BSON", PhysicalType.BYTE_ARRAY);
            case LogicalType.DateType ignored -> requires(physicalType, "DATE", PhysicalType.INT32);
            case LogicalType.TimestampType ignored -> requires(physicalType, "TIMESTAMP", PhysicalType.INT64);
            case LogicalType.TimeType ignored -> requires(physicalType, "TIME",
                    PhysicalType.INT32, PhysicalType.INT64);
            case LogicalType.IntType ignored -> requires(physicalType, "INT",
                    PhysicalType.INT32, PhysicalType.INT64);
            case LogicalType.DecimalType ignored -> requires(physicalType, "DECIMAL",
                    PhysicalType.INT32, PhysicalType.INT64, PhysicalType.BYTE_ARRAY,
                    PhysicalType.FIXED_LEN_BYTE_ARRAY);
            case LogicalType.UuidType ignored -> requiresFixed(physicalType, typeLength, "UUID", 16);
            case LogicalType.IntervalType ignored -> requiresFixed(physicalType, typeLength, "INTERVAL", 12);
            case LogicalType.Float16Type ignored -> requiresFixed(physicalType, typeLength, "FLOAT16", 2);
            // Carried through as opaque payloads, so no physical type is imposed.
            case LogicalType.GeometryType ignored -> null;
            case LogicalType.GeographyType ignored -> null;
            // LIST, MAP and VARIANT annotate a group. On a primitive leaf the pairing is
            // one no version of the format defines, so it is dropped like any other.
            case LogicalType.ListType ignored -> structural("LIST");
            case LogicalType.MapType ignored -> structural("MAP");
            case LogicalType.VariantType ignored -> structural("VARIANT");
            // NULL is legal over any physical type — it says every value is null. Only the
            // values can contradict it, and this answers from the schema alone.
            case LogicalType.NullType ignored -> null;
        };
    }

    private static String structural(String annotation) {
        return annotation + " annotates a group, but the column is a primitive";
    }

    private static String requires(PhysicalType actual, String annotation, PhysicalType... allowed) {
        for (PhysicalType candidate : allowed) {
            if (actual == candidate) {
                return null;
            }
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < allowed.length; i++) {
            sb.append(i == 0 ? "" : i == allowed.length - 1 ? " or " : ", ").append(allowed[i]);
        }
        return annotation + " is read from " + sb + ", but the column is " + actual;
    }

    private static String requiresFixed(PhysicalType actual, Integer typeLength, String annotation,
                                        int width) {
        String wrongType = requires(actual, annotation, PhysicalType.FIXED_LEN_BYTE_ARRAY);
        if (wrongType != null) {
            return wrongType;
        }
        // A footer that omits type_length states no width to contradict the annotation, so
        // there is nothing here that can be proven wrong. That column cannot be decoded at
        // all and FixedWidthValidator refuses it by name; reporting it as a bad annotation
        // would drop a sound annotation and describe the wrong defect.
        if (typeLength != null && typeLength != width) {
            return annotation + " is exactly " + width + " bytes, but the column declares "
                    + typeLength;
        }
        return null;
    }

    /// Convert a physical value to its logical type representation.
    /// Returns the original value if no conversion is needed.
    ///
    /// Exhaustive over the sealed [LogicalType] hierarchy: a new subtype
    /// will fail to compile here until an explicit case is added, blocking
    /// the silent fall-through that masked the missing `JsonType` arm in
    /// earlier revisions.
    public static Object convert(Object physicalValue, PhysicalType physicalType, LogicalType logicalType) {
        if (physicalValue == null || logicalType == null) {
            return physicalValue;
        }

        return switch (logicalType) {
            case LogicalType.StringType t -> convertToString(physicalValue, physicalType);
            case LogicalType.DateType t -> convertToDate(physicalValue, physicalType);
            case LogicalType.TimestampType tt -> tt.isAdjustedToUTC()
                    ? convertToTimestamp(physicalValue, physicalType, tt)
                    : convertToLocalTimestamp(physicalValue, physicalType, tt);
            case LogicalType.TimeType tt -> convertToTime(physicalValue, physicalType, tt);
            case LogicalType.DecimalType dt -> convertToDecimal(physicalValue, physicalType, dt);
            case LogicalType.IntType it -> convertToInt(physicalValue, physicalType, it);
            case LogicalType.UuidType t -> convertToUuid(physicalValue, physicalType);
            case LogicalType.JsonType t -> convertToString(physicalValue, physicalType);
            case LogicalType.BsonType t -> convertToBson(physicalValue, physicalType);
            case LogicalType.IntervalType t -> convertToInterval(physicalValue, physicalType);
            case LogicalType.Float16Type t -> convertToFloat16(physicalValue, physicalType);
            // Enum stores a UTF-8 payload and the spec tells readers without a
            // native enum type to interpret it as a string, so it decodes like UTF8.
            case LogicalType.EnumType e -> convertToString(physicalValue, physicalType);
            // Geometry / Geography carry opaque WKB / WKT binary payloads with no
            // decoder yet — pass each through unchanged.
            case LogicalType.GeometryType g -> physicalValue;
            case LogicalType.GeographyType g -> physicalValue;
            // NullType columns must have all-null values; the null short-circuit
            // at the top of this method means this arm is unreachable for a
            // well-formed file.
            case LogicalType.NullType n -> throw new IllegalStateException(
                    "Non-null physical value on NULL-typed column: " + physicalValue);
            // Structural / self-describing logical types are carried on group
            // nodes (handled by RecordAssembler / variant flyweights) and
            // must never reach a primitive-typed conversion.
            case LogicalType.ListType l -> throw structuralReached(logicalType);
            case LogicalType.MapType m -> throw structuralReached(logicalType);
            case LogicalType.VariantType v -> throw structuralReached(logicalType);
        };
    }

    private static IllegalStateException structuralReached(LogicalType logicalType) {
        return new IllegalStateException(
                "Structural logical type " + logicalType.getClass().getSimpleName()
                        + " reached primitive-value conversion");
    }

    public static String convertToString(Object value, PhysicalType physicalType) {
        if (physicalType != PhysicalType.BYTE_ARRAY) {
            throw new IllegalArgumentException("STRING logical type requires BYTE_ARRAY physical type, got " + physicalType);
        }
        return new String((byte[]) value, StandardCharsets.UTF_8);
    }

    /// BSON is a binary format; expose the raw bytes rather than attempting a UTF-8 decode.
    public static byte[] convertToBson(Object value, PhysicalType physicalType) {
        if (physicalType != PhysicalType.BYTE_ARRAY) {
            throw new IllegalArgumentException("BSON logical type requires BYTE_ARRAY physical type, got " + physicalType);
        }
        return (byte[]) value;
    }

    public static LocalDate convertToDate(Object value, PhysicalType physicalType) {
        if (physicalType != PhysicalType.INT32) {
            throw new IllegalArgumentException("DATE logical type requires INT32 physical type, got " + physicalType);
        }
        // DATE is days since Unix epoch
        int daysSinceEpoch = (Integer) value;
        return LocalDate.ofEpochDay(daysSinceEpoch);
    }

    /// Decodes a UTC-adjusted `TIMESTAMP`. Which of the two timestamp kinds an accessor
    /// accepts is stated once, in `TimestampAccessorKind`, which names the column and the
    /// accessor that fits it; this method decodes whatever unit it is handed.
    public static Instant convertToTimestamp(Object value, PhysicalType physicalType,
                                             LogicalType.TimestampType timestampType) {
        if (physicalType != PhysicalType.INT64) {
            throw new IllegalArgumentException("TIMESTAMP logical type requires INT64 physical type, got " + physicalType);
        }
        long rawValue = (Long) value;
        return switch (timestampType.unit()) {
            case MILLIS -> Instant.ofEpochMilli(rawValue);
            case MICROS -> Instant.ofEpochSecond(rawValue / 1_000_000, (rawValue % 1_000_000) * 1000);
            case NANOS -> Instant.ofEpochSecond(rawValue / 1_000_000_000, rawValue % 1_000_000_000);
        };
    }

    /// Decodes a local-wall-clock `TIMESTAMP`. See [#convertToTimestamp] on where the
    /// kind itself is checked.
    public static LocalDateTime convertToLocalTimestamp(Object value, PhysicalType physicalType,
                                                        LogicalType.TimestampType timestampType) {
        if (physicalType != PhysicalType.INT64) {
            throw new IllegalArgumentException("TIMESTAMP logical type requires INT64 physical type, got " + physicalType);
        }
        // For a local-wall-clock TIMESTAMP the stored int64 is the offset from the
        // epoch *of the wall clock itself*, so the same epoch arithmetic that
        // produces an Instant gives the right LocalDateTime when read at UTC —
        // the bits never change, only the type label.
        long rawValue = (Long) value;
        return switch (timestampType.unit()) {
            case MILLIS -> LocalDateTime.ofEpochSecond(
                    Math.floorDiv(rawValue, 1_000L),
                    (int) (Math.floorMod(rawValue, 1_000L) * 1_000_000L),
                    ZoneOffset.UTC);
            case MICROS -> LocalDateTime.ofEpochSecond(
                    Math.floorDiv(rawValue, 1_000_000L),
                    (int) (Math.floorMod(rawValue, 1_000_000L) * 1_000L),
                    ZoneOffset.UTC);
            case NANOS -> LocalDateTime.ofEpochSecond(
                    Math.floorDiv(rawValue, 1_000_000_000L),
                    (int) Math.floorMod(rawValue, 1_000_000_000L),
                    ZoneOffset.UTC);
        };
    }

    public static PqInterval convertToInterval(Object value, PhysicalType physicalType) {
        if (physicalType != PhysicalType.FIXED_LEN_BYTE_ARRAY) {
            throw new IllegalArgumentException(
                    "INTERVAL logical type requires FIXED_LEN_BYTE_ARRAY physical type, got " + physicalType);
        }

        byte[] bytes = (byte[]) value;
        if (bytes.length != 12) {
            throw new IllegalArgumentException(
                    "INTERVAL requires exactly 12 bytes, got " + bytes.length);
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        long months = Integer.toUnsignedLong(buffer.getInt(0));
        long days = Integer.toUnsignedLong(buffer.getInt(4));
        long millis = Integer.toUnsignedLong(buffer.getInt(8));

        return new PqInterval(months, days, millis);
    }

    public static float convertToFloat16(Object value, PhysicalType physicalType) {
        if (physicalType != PhysicalType.FIXED_LEN_BYTE_ARRAY) {
            throw new IllegalArgumentException(
                    "FLOAT16 logical type requires FIXED_LEN_BYTE_ARRAY physical type, got " + physicalType);
        }

        byte[] bytes = (byte[]) value;
        if (bytes.length != 2) {
            throw new IllegalArgumentException(
                    "FLOAT16 requires exactly 2 bytes, got " + bytes.length);
        }
        // LE 2-byte short; `& 0xFF` blocks sign extension on the byte→int promotion.
        short raw = (short) ((bytes[0] & 0xFF) | ((bytes[1] & 0xFF) << 8));
        return Float.float16ToFloat(raw);
    }

    /// Julian day number of the Unix epoch (1970-01-01).
    private static final long JULIAN_EPOCH_OFFSET_DAYS = 2440588L;

    /// Convert a legacy INT96 timestamp (12 bytes, little-endian: 8 bytes nanos-of-day,
    /// 4 bytes Julian day) to an [Instant]. Used by Apache Spark and Hive.
    public static Instant int96ToInstant(byte[] bytes) {
        if (bytes.length != 12) {
            throw new IllegalArgumentException("INT96 requires exactly 12 bytes, got " + bytes.length);
        }
        ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        long nanosOfDay = bb.getLong(0);
        int julianDay = bb.getInt(8);
        long epochDay = julianDay - JULIAN_EPOCH_OFFSET_DAYS;
        long epochSecond = epochDay * 86400L + nanosOfDay / 1_000_000_000L;
        long nanoAdjustment = nanosOfDay % 1_000_000_000L;
        return Instant.ofEpochSecond(epochSecond, nanoAdjustment);
    }

    public static LocalTime convertToTime(Object value, PhysicalType physicalType,
                                          LogicalType.TimeType timeType) {
        if (physicalType != PhysicalType.INT32 && physicalType != PhysicalType.INT64) {
            throw new IllegalArgumentException(
                    "TIME logical type requires INT32 or INT64 physical type, got " + physicalType);
        }

        long rawValue = physicalType == PhysicalType.INT32 ? (Integer) value : (Long) value;

        return switch (timeType.unit()) {
            case MILLIS -> LocalTime.ofNanoOfDay(rawValue * 1_000_000);
            case MICROS -> LocalTime.ofNanoOfDay(rawValue * 1000);
            case NANOS -> LocalTime.ofNanoOfDay(rawValue);
        };
    }

    public static BigDecimal convertToDecimal(Object value, PhysicalType physicalType,
                                              LogicalType.DecimalType decimalType) {
        BigInteger unscaled = switch (physicalType) {
            case INT32 -> BigInteger.valueOf((Integer) value);
            case INT64 -> BigInteger.valueOf((Long) value);
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> {
                byte[] bytes = (byte[]) value;
                // Parquet uses big-endian two's complement for decimal
                yield new BigInteger(bytes);
            }
            default -> throw new IllegalArgumentException(
                    "DECIMAL requires INT32, INT64, BYTE_ARRAY, or FIXED_LEN_BYTE_ARRAY, got " + physicalType);
        };

        return new BigDecimal(unscaled, decimalType.scale());
    }

    public static Object convertToInt(Object value, PhysicalType physicalType,
                                      LogicalType.IntType intType) {
        if (physicalType != PhysicalType.INT32 && physicalType != PhysicalType.INT64) {
            throw new IllegalArgumentException("INT logical type requires INT32 or INT64 physical type, got " + physicalType);
        }

        // For signed integers with narrowing
        if (intType.isSigned()) {
            if (intType.bitWidth() == 8 && physicalType == PhysicalType.INT32) {
                return ((Integer) value).byteValue();
            }
            else if (intType.bitWidth() == 16 && physicalType == PhysicalType.INT32) {
                return ((Integer) value).shortValue();
            }
        }

        // For 32 or 64 bit, or unsigned types, pass through
        // Note: Java doesn't have native unsigned types, so we return the same value
        return value;
    }

    public static UUID convertToUuid(Object value, PhysicalType physicalType) {
        if (physicalType != PhysicalType.FIXED_LEN_BYTE_ARRAY) {
            throw new IllegalArgumentException("UUID logical type requires FIXED_LEN_BYTE_ARRAY physical type, got " + physicalType);
        }

        byte[] bytes = (byte[]) value;
        if (bytes.length != 16) {
            throw new IllegalArgumentException("UUID requires exactly 16 bytes, got " + bytes.length);
        }

        ByteBuffer bb = ByteBuffer.wrap(bytes);
        long mostSigBits = bb.getLong();
        long leastSigBits = bb.getLong();
        return new UUID(mostSigBits, leastSigBits);
    }
}
