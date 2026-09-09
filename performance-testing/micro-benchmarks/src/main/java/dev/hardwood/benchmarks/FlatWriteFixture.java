/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Random;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.FileSchema;

/// The source records [FlatWriteBenchmark] and [WriteEncodingBenchmark] encode, generated from
/// a fixed seed so every run on every machine writes the same file.
///
/// The shape follows the NYC taxi corpus rather than uniform random noise: encode cost is
/// dominated by dictionary behaviour and compression ratio, both of which are
/// distribution-sensitive, so a fixture of random bytes would measure a file nobody writes.
/// Six columns span the axes that change what the writer does — all-distinct against
/// low-cardinality, fixed against variable width, `REQUIRED` against `OPTIONAL`, annotated
/// against bare:
///
/// | Column | Type | Distribution |
/// |--------|------|--------------|
/// | `id` | `INT64` `REQUIRED` | all distinct, ascending |
/// | `pickup_ts` | `INT64` `REQUIRED`, `TIMESTAMP(MICROS)` | ascending with jitter |
/// | `passenger_count` | `INT32` `OPTIONAL` | 1–6, ~5% null |
/// | `fare` | `DOUBLE` `REQUIRED` | continuous |
/// | `payment_type` | `BYTE_ARRAY` `REQUIRED`, `STRING` | 4 distinct |
/// | `vendor` | `BYTE_ARRAY` `OPTIONAL`, `STRING` | ~20 distinct, ~10% null |
///
/// The columns are held batch by batch — one array per column per batch — so the columnar
/// contender hands its arrays over as they are, without copying a slice out of a
/// million-element column on every call.
///
/// Each column is materialized in the representations the three write APIs take, all built
/// here so that no contender pays a conversion inside the measured region that its API does
/// not actually require: the `STRING` columns as UTF-8 `byte[]` for the columnar API and as
/// `String` for the two record-shaped ones, and `pickup_ts` as `long` microseconds for the
/// APIs that take the stored value alongside [Instant] for the one that takes the annotated
/// value.
final class FlatWriteFixture {

    /// Seed of the fixture's generator. Any value would do; it is fixed so that a number
    /// measured today is comparable with one measured a year from now.
    private static final long SEED = 20_260_819L;

    /// Records generated per invocation unless `-Dperf.rows` says otherwise.
    private static final int DEFAULT_ROWS = 1_000_000;

    private static final String[] PAYMENT_TYPES = { "CREDIT", "CASH", "NO_CHARGE", "DISPUTE" };
    private static final int VENDOR_COUNT = 20;

    private static final int PASSENGER_COUNT_NULL_PERCENT = 5;
    private static final int VENDOR_NULL_PERCENT = 10;

    /// Start of the timestamp column: 2024-01-01T00:00:00Z in microseconds.
    private static final long BASE_MICROS = 1_704_067_200_000_000L;
    /// Mean spacing between two consecutive pickups, in microseconds. Also the width of the
    /// jitter drawn on top of it, which is why it is an `int`.
    private static final int PICKUP_SPACING_MICROS = 1_000_000;

    private static final double MIN_FARE = 3.5;
    private static final double FARE_SPREAD = 96.5;

    /// Stands in for the value of a null row, which no writer encodes. A shared instance
    /// rather than `null` so that a `byte[][]` column never carries a null element.
    private static final byte[] ABSENT = new byte[0];

    private final int rows;

    final long[][] id;
    final long[][] pickupMicros;
    final Instant[][] pickup;
    final int[][] passengerCount;
    final boolean[][] passengerCountNulls;
    final double[][] fare;
    final byte[][][] paymentTypeBytes;
    final String[][] paymentType;
    final byte[][][] vendorBytes;
    final String[][] vendor;
    final boolean[][] vendorNulls;

    /// Generates `rows` records, split into batches of at most `batchRows`.
    static FlatWriteFixture generate(int rows, int batchRows) {
        return new FlatWriteFixture(rows, batchRows);
    }

    /// The record count from `-Dperf.rows`, defaulting to one million (roughly 50 MB of source
    /// values). A value the fixture cannot hold is rejected rather than falling back to the
    /// default, so a number is never reported for a row count nobody asked for. The property is
    /// shared with [BenchmarkData], which reads it as a `long`.
    static int configuredRows() {
        String configured = System.getProperty("perf.rows");
        if (configured == null || configured.isBlank()) {
            return DEFAULT_ROWS;
        }
        try {
            return Math.toIntExact(Long.parseLong(configured.trim()));
        }
        catch (NumberFormatException | ArithmeticException e) {
            throw new IllegalArgumentException(
                    "perf.rows must be an int this fixture can hold but was '" + configured + "'", e);
        }
    }

    /// The schema these records are written under. The columns are flat, so a leaf's dotted
    /// path is its own name — which is what a per-column encoding policy is keyed by.
    static FileSchema schema() {
        return FileSchema.builder("flat")
                .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED)
                .addColumn("pickup_ts", PhysicalType.INT64, RepetitionType.REQUIRED,
                        LogicalType.timestamp(true, LogicalType.TimeUnit.MICROS))
                .addColumn("passenger_count", PhysicalType.INT32, RepetitionType.OPTIONAL)
                .addColumn("fare", PhysicalType.DOUBLE, RepetitionType.REQUIRED)
                .addColumn("payment_type", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                        LogicalType.string())
                .addColumn("vendor", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
                        LogicalType.string())
                .build();
    }

    private FlatWriteFixture(int rows, int batchRows) {
        if (rows <= 0 || batchRows <= 0) {
            throw new IllegalArgumentException(
                    "rows and batchRows must be positive but were " + rows + " and " + batchRows);
        }
        this.rows = rows;

        int batches = (rows + batchRows - 1) / batchRows;
        id = new long[batches][];
        pickupMicros = new long[batches][];
        pickup = new Instant[batches][];
        passengerCount = new int[batches][];
        passengerCountNulls = new boolean[batches][];
        fare = new double[batches][];
        paymentTypeBytes = new byte[batches][][];
        paymentType = new String[batches][];
        vendorBytes = new byte[batches][][];
        vendor = new String[batches][];
        vendorNulls = new boolean[batches][];

        String[] vendors = vendorNames();
        byte[][] vendorUtf8 = utf8(vendors);
        byte[][] paymentUtf8 = utf8(PAYMENT_TYPES);

        Random random = new Random(SEED);
        long nextId = 0;
        long micros = BASE_MICROS;
        for (int b = 0; b < batches; b++) {
            int length = Math.toIntExact(Math.min(batchRows, rows - (long) b * batchRows));
            long[] ids = new long[length];
            long[] timestamps = new long[length];
            Instant[] instants = new Instant[length];
            int[] passengers = new int[length];
            boolean[] passengersNull = new boolean[length];
            double[] fares = new double[length];
            byte[][] payments = new byte[length][];
            String[] paymentStrings = new String[length];
            byte[][] vendorValues = new byte[length][];
            String[] vendorStrings = new String[length];
            boolean[] vendorsNull = new boolean[length];

            for (int r = 0; r < length; r++) {
                ids[r] = nextId++;
                micros += PICKUP_SPACING_MICROS + random.nextInt(PICKUP_SPACING_MICROS);
                timestamps[r] = micros;
                instants[r] = toInstant(micros);

                passengersNull[r] = random.nextInt(100) < PASSENGER_COUNT_NULL_PERCENT;
                passengers[r] = passengersNull[r] ? 0 : 1 + random.nextInt(6);

                fares[r] = MIN_FARE + random.nextDouble() * FARE_SPREAD;

                int payment = random.nextInt(PAYMENT_TYPES.length);
                payments[r] = paymentUtf8[payment];
                paymentStrings[r] = PAYMENT_TYPES[payment];

                vendorsNull[r] = random.nextInt(100) < VENDOR_NULL_PERCENT;
                int vendorIndex = random.nextInt(VENDOR_COUNT);
                vendorValues[r] = vendorsNull[r] ? ABSENT : vendorUtf8[vendorIndex];
                vendorStrings[r] = vendorsNull[r] ? null : vendors[vendorIndex];
            }

            id[b] = ids;
            pickupMicros[b] = timestamps;
            pickup[b] = instants;
            passengerCount[b] = passengers;
            passengerCountNulls[b] = passengersNull;
            fare[b] = fares;
            paymentTypeBytes[b] = payments;
            paymentType[b] = paymentStrings;
            vendorBytes[b] = vendorValues;
            vendor[b] = vendorStrings;
            vendorNulls[b] = vendorsNull;
        }
    }

    /// Total number of records.
    int rows() {
        return rows;
    }

    /// Number of batches the records are split into.
    int batchCount() {
        return id.length;
    }

    private static Instant toInstant(long micros) {
        return Instant.ofEpochSecond(Math.floorDiv(micros, 1_000_000L), Math.floorMod(micros, 1_000_000L) * 1_000L);
    }

    private static String[] vendorNames() {
        String[] names = new String[VENDOR_COUNT];
        for (int i = 0; i < names.length; i++) {
            names[i] = String.format("vendor-%02d", i);
        }
        return names;
    }

    private static byte[][] utf8(String[] values) {
        byte[][] encoded = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            encoded[i] = values[i].getBytes(StandardCharsets.UTF_8);
        }
        return encoded;
    }
}
