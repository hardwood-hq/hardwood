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
import java.time.temporal.Temporal;
import java.util.UUID;

import dev.hardwood.internal.schema.LogicalTypeValidator;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.row.PqInterval;

/// Converts physical values to their logical type representations.
public final class LogicalTypeConverter {

    /// Bytes of the `FIXED_LEN_BYTE_ARRAY` payloads whose width the format fixes, and of
    /// the legacy `INT96` timestamp.
    private static final int UUID_BYTES = 16;
    private static final int INTERVAL_BYTES = 12;
    private static final int FLOAT16_BYTES = 2;
    private static final int INT96_BYTES = 12;

    /// Julian day number of the Unix epoch (1970-01-01).
    private static final long JULIAN_EPOCH_OFFSET_DAYS = 2440588L;

    private LogicalTypeConverter() {
    }

    /// Why `logicalType` cannot be read from a column of this physical type and width, or
    /// `null` when it can.
    ///
    /// A property of the column, not of any value in it, so it is asked once — where the
    /// schema is built, by [dev.hardwood.schema.FileSchema], which drops an annotation this
    /// answers for. Nothing consults it per value, and by the time a conversion below runs
    /// the column has either a sound annotation or none.
    ///
    /// An annotation is faulted where the format rules it out for the column's physical type
    /// or width: `TIME(MILLIS)` anywhere but an `INT32`, `INT(64)` anywhere but an `INT64`, a
    /// `DECIMAL` with more digits than its carrier holds. [LogicalTypeValidator] refuses the
    /// same pairings to the writer. The two differ only where a file on disk leaves nothing to
    /// prove wrong: a `FIXED_LEN_BYTE_ARRAY` without a usable width, which `FixedWidthValidator`
    /// refuses by name, an opaque `GEOMETRY` or `GEOGRAPHY` payload, and `NULL`.
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
            case LogicalType.TimeType time -> requires(physicalType, "TIME(" + time.unit() + ")",
                    time.unit() == LogicalType.TimeUnit.MILLIS ? PhysicalType.INT32 : PhysicalType.INT64);
            case LogicalType.IntType integer -> requires(physicalType, "INT(" + integer.bitWidth() + ")",
                    integer.bitWidth() == 64 ? PhysicalType.INT64 : PhysicalType.INT32);
            case LogicalType.DecimalType decimal -> decimalFault(physicalType, typeLength, decimal);
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

    /// A `DECIMAL` is stored in one of four physical types, and its precision must fit the
    /// digits that type holds.
    private static String decimalFault(PhysicalType actual, Integer typeLength,
                                       LogicalType.DecimalType decimal) {
        String wrongType = requires(actual, "DECIMAL", PhysicalType.INT32, PhysicalType.INT64,
                PhysicalType.BYTE_ARRAY, PhysicalType.FIXED_LEN_BYTE_ARRAY);
        if (wrongType != null) {
            return wrongType;
        }
        boolean fixed = actual == PhysicalType.FIXED_LEN_BYTE_ARRAY;
        // A width that is absent or not positive is FixedWidthValidator's to refuse, and it has
        // no digits to count. requiresFixed keeps only the absent case, because a declared
        // width it can still compare against the one its annotation fixes.
        if (fixed && (typeLength == null || typeLength <= 0)) {
            return null;
        }
        long maxPrecision = LogicalTypeValidator.maxDecimalPrecision(actual, typeLength);
        if (decimal.precision() <= maxPrecision) {
            return null;
        }
        return decimal + " has " + decimal.precision() + " digits, but "
                + (fixed ? actual + "(" + typeLength + ")" : actual) + " holds at most " + maxPrecision;
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
            case LogicalType.StringType t -> bytesToString((byte[]) physicalValue);
            case LogicalType.DateType t -> intToDate((Integer) physicalValue);
            case LogicalType.TimestampType tt -> longToTemporal((Long) physicalValue, tt);
            // TIME, DECIMAL and INT are the arms whose unboxing depends on the physical
            // type, so they keep a helper that takes it; the rest decode from one
            // representation and go straight to the primitive entry point.
            case LogicalType.TimeType tt -> convertToTime(physicalValue, physicalType, tt);
            case LogicalType.DecimalType dt -> convertToDecimal(physicalValue, physicalType, dt);
            case LogicalType.IntType it -> convertToInt(physicalValue, physicalType, it);
            case LogicalType.UuidType t -> bytesToUuid((byte[]) physicalValue);
            case LogicalType.JsonType t -> bytesToString((byte[]) physicalValue);
            // BSON is a binary format; expose the raw bytes rather than attempting a
            // UTF-8 decode.
            case LogicalType.BsonType t -> physicalValue;
            case LogicalType.IntervalType t -> bytesToInterval((byte[]) physicalValue);
            case LogicalType.Float16Type t -> bytesToFloat16((byte[]) physicalValue);
            // Enum stores a UTF-8 payload and the spec tells readers without a
            // native enum type to interpret it as a string, so it decodes like UTF8.
            case LogicalType.EnumType e -> bytesToString((byte[]) physicalValue);
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

    // ==================== Primitive entry points ====================
    //
    // A caller that has already resolved the column's physical type reads its value
    // straight out of the storage array and decodes it here, with no box and no second
    // dispatch on a type it knows. `convert` above is the one path that starts from an
    // `Object` and has to dispatch.

    /// The [LocalDate] a `DATE` column's day count stands for.
    public static LocalDate intToDate(int daysSinceEpoch) {
        return LocalDate.ofEpochDay(daysSinceEpoch);
    }

    /// The time of day a `TIME` column's value in `unit` stands for.
    public static LocalTime longToTime(long rawValue, LogicalType.TimeUnit unit) {
        return LocalTime.ofNanoOfDay(switch (unit) {
            case MILLIS -> rawValue * 1_000_000L;
            case MICROS -> rawValue * 1_000L;
            case NANOS -> rawValue;
        });
    }

    /// The instant a UTC-adjusted `TIMESTAMP` column's offset in `unit` stands for.
    ///
    /// Which of the two timestamp kinds an accessor accepts is stated once, in
    /// `TimestampAccessorKind`, which names the column and the kind it turned out to be.
    /// This decodes whatever unit it is handed.
    public static Instant longToTimestamp(long rawValue, LogicalType.TimeUnit unit) {
        return switch (unit) {
            case MILLIS -> Instant.ofEpochMilli(rawValue);
            case MICROS -> Instant.ofEpochSecond(rawValue / 1_000_000, (rawValue % 1_000_000) * 1000);
            case NANOS -> Instant.ofEpochSecond(rawValue / 1_000_000_000, rawValue % 1_000_000_000);
        };
    }

    /// The value a `TIMESTAMP` column's offset stands for, as the annotation's own
    /// `isAdjustedToUTC` flag decides it: an [Instant] for a UTC-adjusted column, a
    /// [LocalDateTime] for a local one.
    ///
    /// The one statement of that routing, for a caller decoding whatever the column holds
    /// rather than asking for one kind. A caller that has asked — a typed accessor — names
    /// the kind it wants in its return type and calls [#longToTimestamp] or
    /// [#longToLocalTimestamp] directly, having checked the column is that kind through
    /// `TimestampAccessorKind`.
    public static Temporal longToTemporal(long rawValue, LogicalType.TimestampType type) {
        return type.isAdjustedToUTC()
                ? longToTimestamp(rawValue, type.unit())
                : longToLocalTimestamp(rawValue, type.unit());
    }

    /// The wall-clock date and time a local `TIMESTAMP` column's offset in `unit` stands
    /// for. The stored int64 is the offset from the epoch *of the wall clock itself*, so
    /// the same epoch arithmetic that produces an [Instant] gives the right
    /// [LocalDateTime] when read at UTC — the bits never change, only the type label.
    ///
    /// See [#longToTimestamp] on where the kind itself is checked.
    public static LocalDateTime longToLocalTimestamp(long rawValue, LogicalType.TimeUnit unit) {
        return switch (unit) {
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

    /// The decimal an `INT32` or `INT64` `DECIMAL` column's unscaled value stands for.
    public static BigDecimal longToDecimal(long unscaled, int scale) {
        return BigDecimal.valueOf(unscaled, scale);
    }

    /// The decimal a `BYTE_ARRAY` or `FIXED_LEN_BYTE_ARRAY` `DECIMAL` column's payload
    /// stands for. Parquet stores it big-endian two's complement.
    public static BigDecimal bytesToDecimal(byte[] bytes, int scale) {
        return new BigDecimal(new BigInteger(bytes), scale);
    }

    /// The decimal the `DECIMAL` payload at `offset` stands for, for a caller holding
    /// the payload inside a larger buffer. Parquet stores it big-endian two's
    /// complement, which is the layout [BigInteger] reads.
    public static BigDecimal bytesToDecimal(byte[] bytes, int offset, int length, int scale) {
        return new BigDecimal(new BigInteger(bytes, offset, length), scale);
    }

    /// The UTF-8 text of a `STRING`, `ENUM` or `JSON` payload.
    public static String bytesToString(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /// The [UUID] a 16-byte `UUID` payload stands for, most significant half first.
    public static UUID bytesToUuid(byte[] bytes) {
        if (bytes.length != UUID_BYTES) {
            throw new IllegalArgumentException("UUID requires exactly " + UUID_BYTES + " bytes, got " + bytes.length);
        }
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        long mostSigBits = bb.getLong();
        long leastSigBits = bb.getLong();
        return new UUID(mostSigBits, leastSigBits);
    }

    /// The [UUID] the 16-byte `UUID` payload at `offset` stands for, for a caller
    /// holding the payload inside a larger buffer.
    public static UUID bytesToUuid(byte[] bytes, int offset, int length) {
        if (length != UUID_BYTES) {
            throw new IllegalArgumentException("UUID requires exactly " + UUID_BYTES + " bytes, got " + length);
        }
        return new UUID(longAt(bytes, offset), longAt(bytes, offset + 8));
    }

    /// The big-endian `long` at `offset`. `& 0xFF` blocks sign extension on each
    /// byte's promotion to `long`.
    private static long longAt(byte[] bytes, int offset) {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value = (value << 8) | (bytes[offset + i] & 0xFFL);
        }
        return value;
    }

    /// The [PqInterval] a 12-byte `INTERVAL` payload stands for: months, days and millis
    /// as little-endian unsigned 4-byte fields.
    public static PqInterval bytesToInterval(byte[] bytes) {
        if (bytes.length != INTERVAL_BYTES) {
            throw new IllegalArgumentException(
                    "INTERVAL requires exactly " + INTERVAL_BYTES + " bytes, got " + bytes.length);
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        long months = Integer.toUnsignedLong(buffer.getInt(0));
        long days = Integer.toUnsignedLong(buffer.getInt(4));
        long millis = Integer.toUnsignedLong(buffer.getInt(8));
        return new PqInterval(months, days, millis);
    }

    /// The [PqInterval] the 12-byte `INTERVAL` payload at `offset` stands for, for a
    /// caller holding the payload inside a larger buffer.
    public static PqInterval bytesToInterval(byte[] bytes, int offset, int length) {
        if (length != INTERVAL_BYTES) {
            throw new IllegalArgumentException(
                    "INTERVAL requires exactly " + INTERVAL_BYTES + " bytes, got " + length);
        }
        return new PqInterval(
                Integer.toUnsignedLong(intAt(bytes, offset)),
                Integer.toUnsignedLong(intAt(bytes, offset + 4)),
                Integer.toUnsignedLong(intAt(bytes, offset + 8)));
    }

    /// The little-endian `int` at `offset`, the byte order `INTERVAL` fields are
    /// stored in. `& 0xFF` blocks sign extension on each byte's promotion.
    private static int intAt(byte[] bytes, int offset) {
        return (bytes[offset] & 0xFF)
                | ((bytes[offset + 1] & 0xFF) << 8)
                | ((bytes[offset + 2] & 0xFF) << 16)
                | ((bytes[offset + 3] & 0xFF) << 24);
    }

    /// The single-precision value a 2-byte `FLOAT16` payload stands for.
    public static float bytesToFloat16(byte[] bytes) {
        return bytesToFloat16(bytes, 0, bytes.length);
    }

    /// The single-precision value the 2-byte `FLOAT16` payload at `offset` stands for, for
    /// a caller holding the payload inside a larger buffer.
    public static float bytesToFloat16(byte[] bytes, int offset, int length) {
        if (length != FLOAT16_BYTES) {
            throw new IllegalArgumentException(
                    "FLOAT16 requires exactly " + FLOAT16_BYTES + " bytes, got " + length);
        }
        // LE 2-byte short; `& 0xFF` blocks sign extension on the byte→int promotion.
        short raw = (short) ((bytes[offset] & 0xFF) | ((bytes[offset + 1] & 0xFF) << 8));
        return Float.float16ToFloat(raw);
    }

    /// Convert a legacy INT96 timestamp (12 bytes, little-endian: 8 bytes nanos-of-day,
    /// 4 bytes Julian day) to an [Instant]. Used by Apache Spark and Hive.
    public static Instant int96ToInstant(byte[] bytes) {
        if (bytes.length != INT96_BYTES) {
            throw new IllegalArgumentException("INT96 requires exactly " + INT96_BYTES + " bytes, got " + bytes.length);
        }
        ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        long nanosOfDay = bb.getLong(0);
        int julianDay = bb.getInt(8);
        long epochDay = julianDay - JULIAN_EPOCH_OFFSET_DAYS;
        long epochSecond = epochDay * 86400L + nanosOfDay / 1_000_000_000L;
        long nanoAdjustment = nanosOfDay % 1_000_000_000L;
        return Instant.ofEpochSecond(epochSecond, nanoAdjustment);
    }

    // ==================== Boxed unboxing helpers ====================
    //
    // The three arms of `convert` whose unboxing genuinely depends on the physical type,
    // because the same annotation is stored over more than one of them. Every other arm
    // decodes from a single representation and calls its primitive entry point directly.

    private static LocalTime convertToTime(Object value, PhysicalType physicalType,
                                           LogicalType.TimeType timeType) {
        long rawValue = physicalType == PhysicalType.INT32 ? (Integer) value : (Long) value;
        return longToTime(rawValue, timeType.unit());
    }

    private static BigDecimal convertToDecimal(Object value, PhysicalType physicalType,
                                               LogicalType.DecimalType decimalType) {
        return switch (physicalType) {
            case INT32 -> longToDecimal((Integer) value, decimalType.scale());
            case INT64 -> longToDecimal((Long) value, decimalType.scale());
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> bytesToDecimal((byte[]) value, decimalType.scale());
            default -> throw new IllegalArgumentException(
                    "DECIMAL requires INT32, INT64, BYTE_ARRAY, or FIXED_LEN_BYTE_ARRAY, got " + physicalType);
        };
    }

    /// Narrows an `INT(8)` or `INT(16)` value to the box its bit width states. Java has no
    /// native unsigned types, so an unsigned annotation passes its value through unchanged
    /// and the caller reads the magnitude with `Integer.toUnsignedLong` or
    /// `Long.toUnsignedString`.
    private static Object convertToInt(Object value, PhysicalType physicalType,
                                       LogicalType.IntType intType) {
        if (intType.isSigned() && physicalType == PhysicalType.INT32) {
            if (intType.bitWidth() == 8) {
                return ((Integer) value).byteValue();
            }
            if (intType.bitWidth() == 16) {
                return ((Integer) value).shortValue();
            }
        }
        return value;
    }
}
