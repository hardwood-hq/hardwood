/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.tools.predicateaudit;

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
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;

import dev.hardwood.row.PqInterval;
import dev.hardwood.tools.predicateaudit.Columns.Col;
import dev.hardwood.tools.predicateaudit.Columns.Sem;

/// The predicate literal rule of `_designs-legacy/PREDICATE_LITERALS.md`, written independently of the
/// resolver: which predicates a column refuses, and which stored values the others match.
///
/// Comparisons are exact: stored values and literals are compared as `BigInteger` counts of
/// nanoseconds or as `BigDecimal` numbers, so a literal the column cannot hold is answered by the
/// arithmetic, not by the resolver's rounding.
final class Oracle {

    private Oracle() {
    }

    enum Op {
        EQ, NE, LT, LE, GT, GE
    }

    /// SQL's three truth values; a row is returned only when its predicate is TRUE.
    enum Tri {
        T, F, U
    }

    sealed interface P permits Leaf, In, IsNull, IsNotNull, And, Or, Not, Intersects {
    }

    record Leaf(String column, Op op, Object literal) implements P {
    }

    record In(String column, List<Object> literals) implements P {
    }

    record IsNull(String column) implements P {
    }

    record IsNotNull(String column) implements P {
    }

    record And(List<P> children) implements P {
    }

    record Or(List<P> children) implements P {
    }

    record Not(P child) implements P {
    }

    record Intersects(String column, double xmin, double ymin, double xmax, double ymax) implements P {
    }

    /// The stored values the oracle evaluates against.
    interface Rows {

        Col column(String name);

        Object stored(String name, int row);

        /// Whether the named leaf or group is itself null in the row.
        boolean nodeNull(String name, int row);
    }

    private static final BigInteger NANOS_PER_DAY = BigInteger.valueOf(Columns.NANOS_PER_DAY);
    private static final BigInteger JULIAN_EPOCH_NANOS = BigInteger.valueOf(Columns.JULIAN_EPOCH_DAY)
            .multiply(NANOS_PER_DAY);
    private static final BigInteger INT32_MIN = BigInteger.valueOf(Integer.MIN_VALUE);
    private static final BigInteger INT32_MAX = BigInteger.valueOf(Integer.MAX_VALUE);
    private static final BigInteger INT64_MIN = BigInteger.valueOf(Long.MIN_VALUE);
    private static final BigInteger INT64_MAX = BigInteger.valueOf(Long.MAX_VALUE);
    private static final BigInteger INT96_MIN = INT32_MIN.multiply(NANOS_PER_DAY).add(INT64_MIN);
    private static final BigInteger INT96_MAX = INT32_MAX.multiply(NANOS_PER_DAY).add(INT64_MAX);
    private static final BigInteger COUNT96_MIN = BigInteger.ONE.shiftLeft(95).negate();
    private static final BigInteger COUNT96_MAX = BigInteger.ONE.shiftLeft(95).subtract(BigInteger.ONE);
    private static final HexFormat HEX = HexFormat.of();

    // ==================== Refusal ====================

    /// Why the predicate is refused, or `null` if the rule admits it.
    static String refusal(Rows rows, P predicate) {
        return switch (predicate) {
            case Leaf leaf -> refusal(rows.column(leaf.column()), leaf.op(), leaf.literal());
            case In in -> {
                for (Object literal : in.literals()) {
                    String reason = refusal(rows.column(in.column()), Op.EQ, literal);
                    if (reason != null) {
                        yield reason;
                    }
                }
                yield null;
            }
            case And and -> firstRefusal(rows, and.children());
            case Or or -> firstRefusal(rows, or.children());
            case Not not -> containsIntersects(not.child()) ? "not over intersects" : refusal(rows, not.child());
            case Intersects intersects -> rows.column(intersects.column()).sem() == Sem.GEOM ? null : "not a geometry";
            case IsNull ignored -> null;
            case IsNotNull ignored -> null;
        };
    }

    private static String firstRefusal(Rows rows, List<P> children) {
        for (P child : children) {
            String reason = refusal(rows, child);
            if (reason != null) {
                return reason;
            }
        }
        return null;
    }

    static boolean containsIntersects(P predicate) {
        return switch (predicate) {
            case Intersects ignored -> true;
            case And and -> and.children().stream().anyMatch(Oracle::containsIntersects);
            case Or or -> or.children().stream().anyMatch(Oracle::containsIntersects);
            case Not not -> containsIntersects(not.child());
            default -> false;
        };
    }

    static String refusal(Col column, Op op, Object literal) {
        boolean ordered = op != Op.EQ && op != Op.NE;
        if (!takes(column.sem(), literal, false)) {
            return "literal type";
        }
        if (ordered && !takes(column.sem(), literal, true)) {
            return "ordered operator";
        }
        return unholdable(column, literal, ordered);
    }

    /// Whether the column takes the literal, with an ordered operator if `ordered`: the per-column
    /// table of the design.
    private static boolean takes(Sem sem, Object literal, boolean ordered) {
        return switch (sem) {
            case BOOL -> literal instanceof Boolean;
            case I32, INT8S, UINT8, UINT32 -> literal instanceof Integer;
            case I64, UINT64 -> literal instanceof Long;
            case F32 -> literal instanceof Float;
            case F64 -> literal instanceof Double;
            case F16 -> literal instanceof Float || (literal instanceof byte[] && !ordered);
            case DATE -> literal instanceof LocalDate || literal instanceof Integer;
            case TIME_MS -> literal instanceof LocalTime || literal instanceof Integer;
            case TIME_US, TIME_NS -> literal instanceof LocalTime || literal instanceof Long;
            case TS_MS_UTC, TS_US_UTC, TS_NS_UTC -> literal instanceof Instant || literal instanceof Long;
            case TS_MS_LOCAL, TS_US_LOCAL -> literal instanceof LocalDateTime || literal instanceof Long;
            case TS12_MS_UTC, TS12_NS_UTC -> literal instanceof Instant || (literal instanceof byte[] && !ordered);
            case TS12_US_LOCAL -> literal instanceof LocalDateTime || (literal instanceof byte[] && !ordered);
            case INT96 -> literal instanceof Instant || (literal instanceof byte[] && !ordered);
            case DEC_I32 -> literal instanceof BigDecimal || literal instanceof Integer;
            case DEC_I64 -> literal instanceof BigDecimal || literal instanceof Long;
            case DEC_FLBA, DEC_BA -> literal instanceof BigDecimal || (literal instanceof byte[] && !ordered);
            case STRING, ENUM, JSON, BA, ZZ -> literal instanceof String || literal instanceof byte[];
            case BSON, FLBA -> literal instanceof byte[];
            case UUID -> literal instanceof UUID || literal instanceof byte[];
            case INTERVAL -> (literal instanceof PqInterval || literal instanceof byte[]) && !ordered;
            case NULL_I32 -> literal instanceof Integer && !ordered;
            case GEOM -> literal instanceof byte[] && !ordered;
        };
    }

    /// Why a literal is not a value the column can hold, or a byte literal does not decode; `null`
    /// where it is. An order literal only has to decode.
    private static String unholdable(Col column, Object literal, boolean ordered) {
        Sem sem = column.sem();
        if (literal instanceof byte[] bytes) {
            if (sem == Sem.F16 && bytes.length != 2) {
                return "FLOAT16 literal not 2 bytes";
            }
            if (sem == Sem.INT96 && bytes.length != 12) {
                return "INT96 literal not 12 bytes";
            }
            if (isTs12(sem) && bytes.length != 12) {
                return "TIMESTAMP literal not 12 bytes";
            }
            return !ordered && fixedWidth(sem) && bytes.length != column.width() ? "width" : null;
        }
        if (ordered) {
            return null;
        }
        return switch (literal) {
            case Float f when sem == Sem.F16 ->
                !Float.isNaN(f) && Float.float16ToFloat(Float.floatToFloat16(f)) != f ? "not a half" : null;
            case LocalDate d -> within(BigInteger.valueOf(d.toEpochDay()), INT32_MIN, INT32_MAX) ? null : "epoch day range";
            case LocalTime t -> t.toNanoOfDay() % unitNanos(sem) == 0 ? null : "finer than the unit";
            case Instant i when sem == Sem.INT96 ->
                within(nanos(i).add(JULIAN_EPOCH_NANOS), INT96_MIN, INT96_MAX) ? null : "INT96 range";
            case Instant i -> wholeUnits(nanos(i), sem);
            case LocalDateTime t -> wholeUnits(nanos(t), sem);
            case BigDecimal d -> decimalHeld(column, d);
            case PqInterval p -> component(p.months()) && component(p.days()) && component(p.milliseconds())
                    ? null
                    : "interval component";
            default -> null;
        };
    }

    private static boolean isTs12(Sem sem) {
        return sem == Sem.TS12_MS_UTC || sem == Sem.TS12_NS_UTC || sem == Sem.TS12_US_LOCAL;
    }

    private static boolean fixedWidth(Sem sem) {
        return switch (sem) {
            case F16, INT96, DEC_FLBA, FLBA, UUID, INTERVAL, TS12_MS_UTC, TS12_NS_UTC, TS12_US_LOCAL -> true;
            case BOOL, I32, I64, INT8S, UINT8, UINT32, UINT64, F32, F64, DATE, TIME_MS, TIME_US, TIME_NS, TS_MS_UTC,
                 TS_US_UTC, TS_NS_UTC, TS_MS_LOCAL, TS_US_LOCAL, DEC_I32, DEC_I64, DEC_BA, STRING, ENUM, JSON, BSON, BA,
                 NULL_I32, GEOM, ZZ -> false;
        };
    }

    private static boolean component(long value) {
        return value >= 0 && value <= 0xFFFF_FFFFL;
    }

    private static String wholeUnits(BigInteger nanos, Sem sem) {
        BigInteger[] quotientAndRemainder = nanos.divideAndRemainder(BigInteger.valueOf(unitNanos(sem)));
        if (quotientAndRemainder[1].signum() != 0) {
            return "finer than the unit";
        }
        if (isTs12(sem)) {
            return within(quotientAndRemainder[0], COUNT96_MIN, COUNT96_MAX) ? null : "96-bit range";
        }
        return within(quotientAndRemainder[0], INT64_MIN, INT64_MAX) ? null : "INT64 range";
    }

    private static String decimalHeld(Col column, BigDecimal value) {
        if (value.stripTrailingZeros().scale() > column.scale()) {
            return "past the scale";
        }
        BigInteger unscaled = value.setScale(column.scale()).unscaledValue();
        return switch (column.sem()) {
            case DEC_I32 -> within(unscaled, INT32_MIN, INT32_MAX) ? null : "INT32 range";
            case DEC_I64 -> within(unscaled, INT64_MIN, INT64_MAX) ? null : "INT64 range";
            case DEC_FLBA -> {
                BigInteger limit = BigInteger.ONE.shiftLeft(8 * column.width() - 1);
                yield within(unscaled, limit.negate(), limit.subtract(BigInteger.ONE)) ? null : "width";
            }
            default -> null;
        };
    }

    // ==================== Evaluation ====================

    static Tri eval(Rows rows, P predicate, int row) {
        return switch (predicate) {
            case Leaf leaf -> {
                Object stored = rows.stored(leaf.column(), row);
                if (stored == null) {
                    yield Tri.U;
                }
                yield holds(leaf.op(), compare(rows.column(leaf.column()), stored, leaf.literal())) ? Tri.T : Tri.F;
            }
            case In in -> {
                Tri result = Tri.F;
                for (Object literal : in.literals()) {
                    result = or(result, eval(rows, new Leaf(in.column(), Op.EQ, literal), row));
                }
                yield result;
            }
            case IsNull isNull -> rows.nodeNull(isNull.column(), row) ? Tri.T : Tri.F;
            case IsNotNull isNotNull -> rows.nodeNull(isNotNull.column(), row) ? Tri.F : Tri.T;
            case And and -> {
                Tri result = Tri.T;
                for (P child : and.children()) {
                    result = and(result, eval(rows, child, row));
                }
                yield result;
            }
            case Or or -> {
                Tri result = Tri.F;
                for (P child : or.children()) {
                    result = or(result, eval(rows, child, row));
                }
                yield result;
            }
            case Not not -> switch (eval(rows, not.child(), row)) {
                case T -> Tri.F;
                case F -> Tri.T;
                case U -> Tri.U;
            };
            case Intersects ignored -> throw new IllegalStateException("intersects decides row groups, not rows");
        };
    }

    private static Tri and(Tri a, Tri b) {
        if (a == Tri.F || b == Tri.F) {
            return Tri.F;
        }
        return a == Tri.T && b == Tri.T ? Tri.T : Tri.U;
    }

    private static Tri or(Tri a, Tri b) {
        if (a == Tri.T || b == Tri.T) {
            return Tri.T;
        }
        return a == Tri.F && b == Tri.F ? Tri.F : Tri.U;
    }

    private static boolean holds(Op op, int comparison) {
        return switch (op) {
            case EQ -> comparison == 0;
            case NE -> comparison != 0;
            case LT -> comparison < 0;
            case LE -> comparison <= 0;
            case GT -> comparison > 0;
            case GE -> comparison >= 0;
        };
    }

    /// The stored value compared with the literal, by sign; for comparisons defined only as
    /// equality, `0` or `1`.
    private static int compare(Col column, Object stored, Object literal) {
        Sem sem = column.sem();
        return switch (literal) {
            case Boolean b -> Boolean.compare((Boolean) stored, b);
            case Integer i -> sem == Sem.UINT8 || sem == Sem.UINT32
                    ? Integer.compareUnsigned((Integer) stored, i)
                    : Integer.compare((Integer) stored, i);
            case Long l -> sem == Sem.UINT64 ? Long.compareUnsigned((Long) stored, l) : Long.compare((Long) stored, l);
            case Float f -> sem == Sem.F16
                    ? Float.compare(half((byte[]) stored), f)
                    : Float.compare((Float) stored, f);
            case Double d -> Double.compare((Double) stored, d);
            case byte[] b -> byteOrdered(sem)
                    ? Integer.signum(Arrays.compareUnsigned((byte[]) stored, b))
                    : Arrays.equals((byte[]) stored, b) ? 0 : 1;
            case String s -> Integer.signum(Arrays.compareUnsigned((byte[]) stored, s.getBytes(StandardCharsets.UTF_8)));
            case LocalDate d -> Long.compare((Integer) stored, d.toEpochDay());
            case LocalTime t -> inNanos(stored, sem).compareTo(BigInteger.valueOf(t.toNanoOfDay()));
            case Instant i when sem == Sem.INT96 -> int96JulianNanos((byte[]) stored).compareTo(nanos(i).add(JULIAN_EPOCH_NANOS));
            case Instant i -> inNanos(stored, sem).compareTo(nanos(i));
            case LocalDateTime t -> inNanos(stored, sem).compareTo(nanos(t));
            case BigDecimal d -> new BigDecimal(unscaled(stored), column.scale()).compareTo(d);
            case UUID u -> Integer.signum(Arrays.compareUnsigned((byte[]) stored, uuidBytes(u)));
            case PqInterval p -> Arrays.equals((byte[]) stored, intervalBytes(p)) ? 0 : 1;
            default -> throw new IllegalStateException("No rule for a " + literal.getClass().getName());
        };
    }

    private static boolean byteOrdered(Sem sem) {
        return switch (sem) {
            case STRING, ENUM, JSON, BA, ZZ, BSON, FLBA, UUID -> true;
            case BOOL, I32, I64, INT8S, UINT8, UINT32, UINT64, F32, F64, F16, DATE, TIME_MS, TIME_US, TIME_NS, TS_MS_UTC,
                 TS_US_UTC, TS_NS_UTC, TS_MS_LOCAL, TS_US_LOCAL, TS12_MS_UTC, TS12_NS_UTC, TS12_US_LOCAL, INT96, DEC_I32,
                 DEC_I64, DEC_FLBA, DEC_BA, INTERVAL, NULL_I32, GEOM -> false;
        };
    }

    // ==================== Conversions ====================

    static long unitNanos(Sem sem) {
        return switch (sem) {
            case TIME_MS, TS_MS_UTC, TS_MS_LOCAL, TS12_MS_UTC -> 1_000_000L;
            case TIME_US, TS_US_UTC, TS_US_LOCAL, TS12_US_LOCAL -> 1_000L;
            case TIME_NS, TS_NS_UTC, TS12_NS_UTC -> 1L;
            case BOOL, I32, I64, INT8S, UINT8, UINT32, UINT64, F32, F64, F16, DATE, INT96, DEC_I32, DEC_I64, DEC_FLBA,
                 DEC_BA, STRING, ENUM, JSON, BSON, BA, FLBA, UUID, INTERVAL, NULL_I32, GEOM, ZZ ->
                throw new IllegalStateException("No time unit on a " + sem + " column");
        };
    }

    /// A stored time value in nanoseconds: an integer count of the unit, or the little-endian two's
    /// complement count of a `FIXED_LEN_BYTE_ARRAY(12)` `TIMESTAMP`.
    private static BigInteger inNanos(Object stored, Sem sem) {
        BigInteger count = isTs12(sem) ? littleEndianCount((byte[]) stored) : unscaled(stored);
        return count.multiply(BigInteger.valueOf(unitNanos(sem)));
    }

    private static BigInteger littleEndianCount(byte[] bytes) {
        byte[] bigEndian = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            bigEndian[i] = bytes[bytes.length - 1 - i];
        }
        return new BigInteger(bigEndian);
    }

    static BigInteger nanos(Instant instant) {
        return BigInteger.valueOf(instant.getEpochSecond()).multiply(BigInteger.valueOf(1_000_000_000L))
                .add(BigInteger.valueOf(instant.getNano()));
    }

    static BigInteger nanos(LocalDateTime wallClock) {
        return nanos(wallClock.toInstant(ZoneOffset.UTC));
    }

    private static boolean within(BigInteger value, BigInteger min, BigInteger max) {
        return value.compareTo(min) >= 0 && value.compareTo(max) <= 0;
    }

    static float half(byte[] bytes) {
        return Float.float16ToFloat((short) ((bytes[1] & 0xFF) << 8 | bytes[0] & 0xFF));
    }

    /// An integer or a big-endian two's complement byte string; no bytes at all is zero.
    static BigInteger unscaled(Object stored) {
        return switch (stored) {
            case Integer i -> BigInteger.valueOf(i);
            case Long l -> BigInteger.valueOf(l);
            case byte[] b -> b.length == 0 ? BigInteger.ZERO : new BigInteger(b);
            default -> throw new IllegalStateException("Not an integer: " + stored);
        };
    }

    private static BigInteger int96JulianNanos(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        long nanosOfDay = buffer.getLong();
        int day = buffer.getInt();
        return BigInteger.valueOf(day).multiply(NANOS_PER_DAY).add(BigInteger.valueOf(nanosOfDay));
    }

    static byte[] uuidBytes(UUID uuid) {
        return ByteBuffer.allocate(16).putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits()).array();
    }

    static byte[] intervalBytes(PqInterval interval) {
        return ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN)
                .putInt((int) interval.months()).putInt((int) interval.days()).putInt((int) interval.milliseconds())
                .array();
    }

    // ==================== Display ====================

    static String show(Object literal) {
        return switch (literal) {
            case byte[] b -> "0x" + HEX.formatHex(b);
            case Float f when Float.isNaN(f) -> "NaN(0x" + Integer.toHexString(Float.floatToRawIntBits(f)) + ")f";
            case Double d when Double.isNaN(d) -> "NaN(0x" + Long.toHexString(Double.doubleToRawLongBits(d)) + ")";
            case Float f -> f + "f";
            case Long l -> l + "L";
            case String s -> "\"" + s + "\"";
            case BigDecimal d -> "BigDecimal(" + d + ")";
            default -> String.valueOf(literal);
        };
    }

    static String show(P predicate) {
        return switch (predicate) {
            case Leaf leaf -> name(leaf.op()) + "(" + leaf.column() + ", " + show(leaf.literal()) + ")";
            case In in -> "in(" + in.column() + ", " + String.join(", ", in.literals().stream().map(Oracle::show).toList()) + ")";
            case IsNull isNull -> "isNull(" + isNull.column() + ")";
            case IsNotNull isNotNull -> "isNotNull(" + isNotNull.column() + ")";
            case And and -> "and(" + String.join(", ", and.children().stream().map(Oracle::show).toList()) + ")";
            case Or or -> "or(" + String.join(", ", or.children().stream().map(Oracle::show).toList()) + ")";
            case Not not -> "not(" + show(not.child()) + ")";
            case Intersects i -> "intersects(" + i.column() + ", " + i.xmin() + ", " + i.ymin() + ", " + i.xmax() + ", "
                    + i.ymax() + ")";
        };
    }

    private static String name(Op op) {
        return switch (op) {
            case EQ -> "eq";
            case NE -> "notEq";
            case LT -> "lt";
            case LE -> "ltEq";
            case GT -> "gt";
            case GE -> "gtEq";
        };
    }
}
