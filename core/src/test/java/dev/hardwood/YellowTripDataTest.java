/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Tests for reading NYC Yellow Taxi trip data.
 */
public class YellowTripDataTest {

    @Test
    void testReadYellowTripDataSample() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/yellow_tripdata_sample.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            // Verify schema
            assertThat(fileReader.getFileSchema().getColumnCount()).isEqualTo(20);
            assertThat(fileReader.getFileMetaData().numRows()).isEqualTo(5);

            try (RowReader rowReader = fileReader.createRowReader()) {
                int rowCount = 0;

                // Row 0: VendorID=1, pickup=2025-01-01T00:18:38, dropoff=2025-01-01T00:26:59
                assertThat(rowReader.hasNext()).isTrue();
                rowReader.next();
                rowCount++;
                assertThat(rowReader.getInt("VendorID")).isEqualTo(1);
                assertThat(rowReader.getTimestamp("tpep_pickup_datetime"))
                        .isEqualTo(Instant.parse("2025-01-01T00:18:38Z"));
                assertThat(rowReader.getTimestamp("tpep_dropoff_datetime"))
                        .isEqualTo(Instant.parse("2025-01-01T00:26:59Z"));
                assertThat(rowReader.getLong("passenger_count")).isEqualTo(1L);
                assertThat(rowReader.getDouble("trip_distance")).isEqualTo(1.6);
                assertThat(rowReader.getLong("RatecodeID")).isEqualTo(1L);
                assertThat(rowReader.getString("store_and_fwd_flag")).isEqualTo("N");
                assertThat(rowReader.getInt("PULocationID")).isEqualTo(229);
                assertThat(rowReader.getInt("DOLocationID")).isEqualTo(237);
                assertThat(rowReader.getLong("payment_type")).isEqualTo(1L);
                assertThat(rowReader.getDouble("fare_amount")).isEqualTo(10.0);
                assertThat(rowReader.getDouble("extra")).isEqualTo(3.5);
                assertThat(rowReader.getDouble("mta_tax")).isEqualTo(0.5);
                assertThat(rowReader.getDouble("tip_amount")).isEqualTo(3.0);
                assertThat(rowReader.getDouble("tolls_amount")).isEqualTo(0.0);
                assertThat(rowReader.getDouble("improvement_surcharge")).isEqualTo(1.0);
                assertThat(rowReader.getDouble("total_amount")).isEqualTo(18.0);
                assertThat(rowReader.getDouble("congestion_surcharge")).isEqualTo(2.5);
                assertThat(rowReader.getDouble("Airport_fee")).isEqualTo(0.0);
                assertThat(rowReader.getDouble("cbd_congestion_fee")).isEqualTo(0.0);

                // Row 1: VendorID=1, pickup=2025-01-01T00:32:40, trip_distance=0.5
                assertThat(rowReader.hasNext()).isTrue();
                rowReader.next();
                rowCount++;
                assertThat(rowReader.getInt("VendorID")).isEqualTo(1);
                assertThat(rowReader.getTimestamp("tpep_pickup_datetime"))
                        .isEqualTo(Instant.parse("2025-01-01T00:32:40Z"));
                assertThat(rowReader.getTimestamp("tpep_dropoff_datetime"))
                        .isEqualTo(Instant.parse("2025-01-01T00:35:13Z"));
                assertThat(rowReader.getLong("passenger_count")).isEqualTo(1L);
                assertThat(rowReader.getDouble("trip_distance")).isEqualTo(0.5);
                assertThat(rowReader.getInt("PULocationID")).isEqualTo(236);
                assertThat(rowReader.getInt("DOLocationID")).isEqualTo(237);
                assertThat(rowReader.getDouble("fare_amount")).isEqualTo(5.1);
                assertThat(rowReader.getDouble("tip_amount")).isCloseTo(2.02, within(0.001));
                assertThat(rowReader.getDouble("total_amount")).isEqualTo(12.12);

                // Row 2: VendorID=1, pickup=2025-01-01T00:44:04, same PU/DO location
                assertThat(rowReader.hasNext()).isTrue();
                rowReader.next();
                rowCount++;
                assertThat(rowReader.getInt("VendorID")).isEqualTo(1);
                assertThat(rowReader.getTimestamp("tpep_pickup_datetime"))
                        .isEqualTo(Instant.parse("2025-01-01T00:44:04Z"));
                assertThat(rowReader.getInt("PULocationID")).isEqualTo(141);
                assertThat(rowReader.getInt("DOLocationID")).isEqualTo(141);
                assertThat(rowReader.getDouble("trip_distance")).isEqualTo(0.6);
                assertThat(rowReader.getDouble("total_amount")).isEqualTo(12.1);

                // Row 3: VendorID=2, 3 passengers, no tip (payment_type=2 = cash)
                assertThat(rowReader.hasNext()).isTrue();
                rowReader.next();
                rowCount++;
                assertThat(rowReader.getInt("VendorID")).isEqualTo(2);
                assertThat(rowReader.getTimestamp("tpep_pickup_datetime"))
                        .isEqualTo(Instant.parse("2025-01-01T00:14:27Z"));
                assertThat(rowReader.getLong("passenger_count")).isEqualTo(3L);
                assertThat(rowReader.getDouble("trip_distance")).isEqualTo(0.52);
                assertThat(rowReader.getInt("PULocationID")).isEqualTo(244);
                assertThat(rowReader.getInt("DOLocationID")).isEqualTo(244);
                assertThat(rowReader.getLong("payment_type")).isEqualTo(2L);
                assertThat(rowReader.getDouble("tip_amount")).isEqualTo(0.0);
                assertThat(rowReader.getDouble("congestion_surcharge")).isEqualTo(0.0);
                assertThat(rowReader.getDouble("total_amount")).isEqualTo(9.7);

                // Row 4: VendorID=2, 3 passengers, different DO location
                assertThat(rowReader.hasNext()).isTrue();
                rowReader.next();
                rowCount++;
                assertThat(rowReader.getInt("VendorID")).isEqualTo(2);
                assertThat(rowReader.getTimestamp("tpep_pickup_datetime"))
                        .isEqualTo(Instant.parse("2025-01-01T00:21:34Z"));
                assertThat(rowReader.getTimestamp("tpep_dropoff_datetime"))
                        .isEqualTo(Instant.parse("2025-01-01T00:25:06Z"));
                assertThat(rowReader.getLong("passenger_count")).isEqualTo(3L);
                assertThat(rowReader.getDouble("trip_distance")).isEqualTo(0.66);
                assertThat(rowReader.getInt("PULocationID")).isEqualTo(244);
                assertThat(rowReader.getInt("DOLocationID")).isEqualTo(116);
                assertThat(rowReader.getDouble("fare_amount")).isEqualTo(5.8);
                assertThat(rowReader.getDouble("total_amount")).isEqualTo(8.3);

                // Verify no more rows
                assertThat(rowReader.hasNext()).isFalse();
                assertThat(rowCount).isEqualTo(5);
            }
        }
    }

    @Test
    void printRows() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/yellow_tripdata_sample.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            FileSchema fileSchema = fileReader.getFileSchema();
            List<ColumnSchema> columns = fileSchema.getColumns();
            DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                    .withZone(ZoneId.of("UTC"));

            // Print header
            StringBuilder header = new StringBuilder();
            StringBuilder separator = new StringBuilder();
            for (ColumnSchema col : columns) {
                String name = col.name();
                int width = Math.max(name.length(), 12);
                header.append(String.format("%-" + width + "s | ", name));
                separator.append("-".repeat(width)).append("-+-");
            }
            System.out.println(header);
            System.out.println(separator);

            // Print first 10 rows
            try (RowReader rowReader = fileReader.createRowReader()) {
                int count = 0;
                while (rowReader.hasNext() && count < 10) {
                    rowReader.next();
                    count++;

                    StringBuilder line = new StringBuilder();
                    for (ColumnSchema col : columns) {
                        String formatted = formatValue(rowReader, col, fmt);
                        int width = Math.max(col.name().length(), 12);
                        line.append(String.format("%-" + width + "s | ", formatted));
                    }
                    System.out.println(line);
                }
            }
        }
    }

    private String formatValue(RowReader rowReader, ColumnSchema col, DateTimeFormatter fmt) {
        String name = col.name();
        if (rowReader.isNull(name)) {
            return "null";
        }

        // Check logical type first for timestamps
        LogicalType logicalType = col.logicalType();
        if (logicalType instanceof LogicalType.TimestampType) {
            Instant inst = rowReader.getTimestamp(name);
            return fmt.format(inst);
        }

        // Fall back to physical type
        return switch (col.type()) {
            case INT32 -> String.valueOf(rowReader.getInt(name));
            case INT64 -> String.valueOf(rowReader.getLong(name));
            case FLOAT -> String.format("%.2f", rowReader.getFloat(name));
            case DOUBLE -> String.format("%.2f", rowReader.getDouble(name));
            case BOOLEAN -> String.valueOf(rowReader.getBoolean(name));
            case BYTE_ARRAY -> rowReader.getString(name);
            default -> String.valueOf(rowReader.getValue(name));
        };
    }
}
