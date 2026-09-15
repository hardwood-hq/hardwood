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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import dev.hardwood.row.PqInterval;
import dev.hardwood.tools.predicateaudit.Columns.Col;
import dev.hardwood.tools.predicateaudit.Columns.Sem;
import dev.hardwood.tools.predicateaudit.Oracle.And;
import dev.hardwood.tools.predicateaudit.Oracle.In;
import dev.hardwood.tools.predicateaudit.Oracle.Intersects;
import dev.hardwood.tools.predicateaudit.Oracle.IsNotNull;
import dev.hardwood.tools.predicateaudit.Oracle.IsNull;
import dev.hardwood.tools.predicateaudit.Oracle.Leaf;
import dev.hardwood.tools.predicateaudit.Oracle.Not;
import dev.hardwood.tools.predicateaudit.Oracle.Op;
import dev.hardwood.tools.predicateaudit.Oracle.Or;
import dev.hardwood.tools.predicateaudit.Oracle.P;

/// The predicates the matrix puts to each column: literals of every kind the column takes and some
/// it does not, around a stored value, in the gaps between values, past the carrier's range and
/// at the edge cases of the type, under every operator, `not` form and set form.
final class Cases {

    private Cases() {
    }

    /// The predicates for every column of `columns` but `__row__` and `zz`.
    static List<P> forColumns(List<Col> columns) {
        List<P> cases = new ArrayList<>();
        for (Col column : columns) {
            if (column.name().equals("__row__") || column.name().equals("zz")) {
                continue;
            }
            cases.add(new IsNull(column.name()));
            cases.add(new IsNotNull(column.name()));
            cases.add(new Not(new IsNull(column.name())));
            List<Object> literals = literals(column);
            for (Object literal : literals) {
                forLiteral(cases, column.name(), literal, literals);
            }
        }
        return cases;
    }

    private static void forLiteral(List<P> cases, String column, Object literal, List<Object> literals) {
        List<Op> ops = literal instanceof PqInterval ? List.of(Op.EQ, Op.NE) : List.of(Op.values());
        for (Op op : ops) {
            Leaf leaf = new Leaf(column, op, literal);
            cases.add(leaf);
            cases.add(new Not(leaf));
            if (op == Op.EQ || op == Op.LT) {
                cases.add(new Not(new Not(leaf)));
                cases.add(new Not(new And(List.of(new Not(leaf), new IsNotNull(column)))));
            }
        }
        if (literal instanceof Boolean) {
            return;
        }
        Object other = literals.stream().filter(o -> o != literal && o.getClass() == literal.getClass()).findFirst().orElse(null);
        In in = new In(column, other == null ? List.of(literal) : List.of(literal, other));
        cases.add(in);
        cases.add(new Not(in));
        cases.add(new Not(new Not(in)));
    }

    /// Compositions across columns with their own null rows.
    static List<P> compositions() {
        P floatAbove = new Leaf("f32", Op.GT, 10.0f);
        P intAbove = new Leaf("i32", Op.GT, 0);
        return List.of(
                new And(List.of(floatAbove, intAbove)),
                new Or(List.of(floatAbove, new IsNull("i32"))),
                new Not(new And(List.of(new Not(new In("f16", List.of(1.5f))), intAbove))),
                new Not(new Or(List.of(new Leaf("date", Op.LT, LocalDate.MAX), new IsNull("date")))),
                new Not(new Not(new Leaf("ts_us_utc", Op.LT, Instant.MIN))),
                new Not(new Leaf("bool", Op.LT, false)),
                new Or(List.of(new Leaf("dec_ba", Op.EQ, new byte[] { 0x00, 0x02 }),
                        new Leaf("dec_ba", Op.EQ, new BigDecimal("0.002")))));
    }

    /// `intersects` cases, which decide row groups and are reported, not checked row by row.
    static List<P> intersects() {
        return List.of(
                new Intersects("geom", 0, 0, 10, 600),
                new And(List.of(new Intersects("geom", 0, 0, 10, 600), new IsNotNull("geom"))),
                new Intersects("geom", 10, 0, 0, 600));
    }

    /// Null tests on struct, list and leaf nodes, and comparisons on struct leaves with nulls at
    /// every level above them.
    static List<P> nested() {
        List<P> cases = new ArrayList<>();
        for (String node : List.of("s", "s.t", "l", "s.x", "s.name", "s.t.y")) {
            cases.add(new IsNull(node));
            cases.add(new IsNotNull(node));
            cases.add(new Not(new IsNull(node)));
        }
        for (Op op : Op.values()) {
            for (Object literal : List.of(20, 21, -601, 600)) {
                P leaf = new Leaf("s.x", op, literal);
                cases.add(leaf);
                cases.add(new Not(leaf));
                cases.add(new Not(new Not(leaf)));
                cases.add(new And(List.of(new IsNotNull("s"), leaf)));
                cases.add(new Or(List.of(new IsNull("s"), new Not(leaf))));
            }
            for (Object literal : List.of(305L, 306L)) {
                P leaf = new Leaf("s.t.y", op, literal);
                cases.add(leaf);
                cases.add(new Not(leaf));
                cases.add(new Or(List.of(leaf, new IsNull("s.t"))));
            }
            cases.add(new Leaf("s.name", op, "n0310"));
            cases.add(new Not(new Leaf("s.name", op, "n0310")));
        }
        cases.add(new In("s.x", List.of(20, 22)));
        cases.add(new Not(new In("s.x", List.of(20, 22))));
        cases.add(new And(List.of(new IsNotNull("s"), new IsNull("s.x"))));
        cases.add(new And(List.of(new IsNotNull("s.t"), new IsNull("s.t.y"))));
        return cases;
    }

    // ==================== Literals ====================

    private static byte[] bump(byte[] bytes) {
        byte[] copy = bytes.clone();
        if (copy.length > 0) {
            copy[copy.length - 1]++;
        }
        return copy;
    }

    /// The literals put to `column`, derived from the value it stores at [Columns#PROBE_ROW].
    static List<Object> literals(Col column) {
        Object probe = column.at(Columns.PROBE_ROW);
        List<Object> l = new ArrayList<>();
        switch (column.sem()) {
            case BOOL -> l.addAll(List.of(true, false));
            case I32, INT8S, UINT8, UINT32 -> {
                int value = (Integer) probe;
                l.addAll(List.of(value, value + 1, Integer.MIN_VALUE, Integer.MAX_VALUE, column.at(598), -1));
                if (column.sem() == Sem.INT8S) {
                    l.addAll(List.of(1000, 200, -1000));
                }
                if (column.sem() == Sem.UINT32) {
                    l.add(0);
                }
            }
            case I64, UINT64 -> {
                long value = (Long) probe;
                l.addAll(List.of(value, value + 1, Long.MIN_VALUE, Long.MAX_VALUE, -1L, 0L));
            }
            case F32 -> l.addAll(List.of(probe, (Float) probe + 0.25f, Float.NaN, Float.intBitsToFloat(0x7f800001), -0.0f,
                    0.0f, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, 0.1f));
            case F64 -> l.addAll(List.of(probe, (Double) probe + 0.25, Double.NaN, Double.longBitsToDouble(0x7ff0000000000001L),
                    -0.0, 0.0, Double.POSITIVE_INFINITY, 0.1));
            case F16 -> l.addAll(List.of(Oracle.half((byte[]) probe), 0.1f, Float.NaN, -0.0f, 0.0f, 70000f, 1.0009765625f,
                    Float.POSITIVE_INFINITY, 65520f, probe, new byte[] { 0x00, 0x7E }, new byte[] { 0x01, 0x7C },
                    new byte[] { 0x00, (byte) 0x80 }, new byte[] { 1, 2, 3 }));
            case DATE -> l.addAll(List.of(LocalDate.ofEpochDay((Integer) probe), LocalDate.ofEpochDay((Integer) probe + 1),
                    LocalDate.MAX, LocalDate.MIN, probe));
            case TIME_MS, TIME_US, TIME_NS -> {
                LocalTime time = LocalTime.ofNanoOfDay(((Number) probe).longValue() * Oracle.unitNanos(column.sem()));
                l.addAll(List.of(time, time.plusNanos(1), LocalTime.MAX, LocalTime.MIDNIGHT, probe));
                if (column.sem() == Sem.TIME_MS) {
                    l.add(90_000_000);
                }
            }
            case TS_MS_UTC, TS_US_UTC, TS_NS_UTC -> {
                Instant instant = Instant.EPOCH.plusNanos((Long) probe * Oracle.unitNanos(column.sem()));
                l.addAll(List.of(instant, instant.plusNanos(1), Instant.MAX, Instant.MIN, Instant.parse("2300-01-01T00:00:00Z"),
                        LocalDateTime.ofInstant(instant, ZoneOffset.UTC), probe));
            }
            case TS_MS_LOCAL, TS_US_LOCAL -> {
                LocalDateTime wallClock = LocalDateTime.ofInstant(
                        Instant.EPOCH.plusNanos((Long) probe * Oracle.unitNanos(column.sem())), ZoneOffset.UTC);
                l.addAll(List.of(wallClock, wallClock.plusNanos(1), LocalDateTime.MAX, LocalDateTime.MIN,
                        LocalDateTime.of(2300, 1, 1, 0, 0), wallClock.toInstant(ZoneOffset.UTC), probe));
            }
            case TS12_MS_UTC, TS12_NS_UTC, TS12_US_LOCAL -> ts12Literals(l, column, probe);
            case INT96 -> int96Literals(l, probe);
            case DEC_I32, DEC_I64, DEC_FLBA, DEC_BA -> decimalLiterals(l, column, probe);
            case STRING, ENUM, JSON -> l.addAll(List.of(new String((byte[]) probe, StandardCharsets.UTF_8), "", "～", "é",
                    "k0310x", probe, bump((byte[]) probe)));
            case BA, BSON -> {
                l.addAll(List.of(probe, bump((byte[]) probe), new byte[0], new byte[] { (byte) 0xFF }));
                if (column.sem() == Sem.BA) {
                    l.add("x");
                }
            }
            case FLBA -> l.addAll(List.of(probe, bump((byte[]) probe), new byte[] { (byte) 0x80 }, new byte[5]));
            case UUID -> {
                ByteBuffer buffer = ByteBuffer.wrap((byte[]) probe);
                l.addAll(List.of(new UUID(buffer.getLong(), buffer.getLong()), new UUID(0x8000000000000000L, 0), probe,
                        new byte[3], "x"));
            }
            case INTERVAL -> {
                ByteBuffer buffer = ByteBuffer.wrap((byte[]) probe).order(ByteOrder.LITTLE_ENDIAN);
                l.addAll(List.of(new PqInterval(Integer.toUnsignedLong(buffer.getInt()), Integer.toUnsignedLong(buffer.getInt()),
                        Integer.toUnsignedLong(buffer.getInt())), new PqInterval(4_294_967_295L, 599 % 31, 599_000L),
                        new PqInterval(4_294_967_296L, 0, 0), new PqInterval(-1, 0, 0), probe, new byte[11]));
            }
            case NULL_I32 -> l.addAll(List.of(1, 1L));
            case GEOM -> l.add(probe);
            case ZZ -> {
            }
        }
        return l;
    }

    /// Literals for a `FIXED_LEN_BYTE_ARRAY(12)` `TIMESTAMP`: the typed literal at the probe row, one
    /// nanosecond past it, before the epoch, past year 9999 and past the `INT64` nanosecond range, at
    /// the ends of `Instant`; the literal of the other timestamp kind and a `long`; and byte literals
    /// of the probe row, one count past it and one byte short.
    private static void ts12Literals(List<Object> l, Col column, Object probe) {
        BigInteger nanos = Columns.ts12Nanos(Columns.PROBE_ROW);
        BigInteger[] seconds = nanos.subtract(nanos.mod(BigInteger.valueOf(Oracle.unitNanos(column.sem()))))
                .divideAndRemainder(BigInteger.valueOf(1_000_000_000L));
        Instant atProbe = Instant.ofEpochSecond(seconds[0].longValueExact(), seconds[1].longValueExact());
        List<Instant> instants = List.of(atProbe, atProbe.plusNanos(1), Instant.parse("1969-12-31T23:59:59.999999999Z"),
                Instant.parse("9999-12-31T23:59:59.999999999Z"), Instant.parse("2300-01-01T00:00:00Z"),
                Instant.parse("-5000-01-01T00:00:00Z"));
        if (column.sem() == Sem.TS12_US_LOCAL) {
            instants.forEach(instant -> l.add(LocalDateTime.ofInstant(instant, ZoneOffset.UTC)));
            l.addAll(List.of(LocalDateTime.MAX, LocalDateTime.MIN, atProbe));
        }
        else {
            l.addAll(instants);
            l.addAll(List.of(Instant.MAX, Instant.MIN, LocalDateTime.ofInstant(atProbe, ZoneOffset.UTC)));
        }
        byte[] past = ((byte[]) probe).clone();
        past[0]++;
        l.addAll(List.of(probe, past, new byte[11], 0L));
    }

    private static void int96Literals(List<Object> l, Object probe) {
        Instant nonCanonicalPair = Instant.EPOCH.plusNanos(Columns.int96Nanos(6));
        Instant atProbe = Instant.EPOCH.plusNanos(Columns.int96Nanos(Columns.PROBE_ROW));
        // One past the latest instant an INT96 encodes: the last Julian day with Long.MAX_VALUE nanoseconds.
        Instant past = Instant.EPOCH.plusSeconds((Integer.MAX_VALUE - Columns.JULIAN_EPOCH_DAY) * 86_400L)
                .plusNanos(Long.MAX_VALUE).plusNanos(1);
        l.addAll(List.of(nonCanonicalPair, atProbe, atProbe.plusNanos(1), Instant.EPOCH.plusNanos(Columns.int96Nanos(1)),
                Instant.MAX, Instant.MIN, past, past.minusNanos(1), LocalDateTime.ofInstant(atProbe, ZoneOffset.UTC),
                Columns.int96(Columns.int96Nanos(6), true), Columns.int96(Columns.int96Nanos(6), false), new byte[11], probe));
    }

    private static void decimalLiterals(List<Object> l, Col column, Object probe) {
        BigDecimal value = new BigDecimal(Oracle.unscaled(probe), column.scale());
        l.addAll(List.of(value, value.setScale(column.scale() + 2), value.add(BigDecimal.ONE.movePointLeft(column.scale() + 1)),
                new BigDecimal("1E+40"), new BigDecimal("-1E+40"), new BigDecimal("12345678.90")));
        switch (column.sem()) {
            case DEC_I32 -> l.addAll(List.of(probe, Integer.MAX_VALUE));
            case DEC_I64 -> l.add(probe);
            case DEC_FLBA -> l.addAll(List.of(probe, new byte[] { 0x7D }));
            default -> l.addAll(List.of(probe, new byte[] { 0x7D }, BigDecimal.ZERO, new BigDecimal("0.002"), new byte[0],
                    new byte[] { 0x02 }, new byte[] { 0x00, 0x02 }, new byte[] { 0x00 }));
        }
    }
}
