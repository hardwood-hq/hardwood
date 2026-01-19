/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.morling.hardwood.reader.ParquetFileReader;
import dev.morling.hardwood.reader.RowReader;
import dev.morling.hardwood.row.PqRow;

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
                List<PqRow> rows = new ArrayList<>();
                for (PqRow row : rowReader) {
                    rows.add(row);
                }

                assertThat(rows).hasSize(5);

                // Row 0: VendorID=1, pickup=2025-01-01T00:18:38, dropoff=2025-01-01T00:26:59
                PqRow row0 = rows.get(0);
                assertThat(row0.getInt("VendorID")).isEqualTo(1);
                assertThat(row0.getTimestamp("tpep_pickup_datetime"))
                        .isEqualTo(Instant.parse("2025-01-01T00:18:38Z"));
                assertThat(row0.getTimestamp("tpep_dropoff_datetime"))
                        .isEqualTo(Instant.parse("2025-01-01T00:26:59Z"));
                assertThat(row0.getLong("passenger_count")).isEqualTo(1L);
                assertThat(row0.getDouble("trip_distance")).isEqualTo(1.6);
                assertThat(row0.getLong("RatecodeID")).isEqualTo(1L);
                assertThat(row0.getString("store_and_fwd_flag")).isEqualTo("N");
                assertThat(row0.getInt("PULocationID")).isEqualTo(229);
                assertThat(row0.getInt("DOLocationID")).isEqualTo(237);
                assertThat(row0.getLong("payment_type")).isEqualTo(1L);
                assertThat(row0.getDouble("fare_amount")).isEqualTo(10.0);
                assertThat(row0.getDouble("extra")).isEqualTo(3.5);
                assertThat(row0.getDouble("mta_tax")).isEqualTo(0.5);
                assertThat(row0.getDouble("tip_amount")).isEqualTo(3.0);
                assertThat(row0.getDouble("tolls_amount")).isEqualTo(0.0);
                assertThat(row0.getDouble("improvement_surcharge")).isEqualTo(1.0);
                assertThat(row0.getDouble("total_amount")).isEqualTo(18.0);
                assertThat(row0.getDouble("congestion_surcharge")).isEqualTo(2.5);
                assertThat(row0.getDouble("Airport_fee")).isEqualTo(0.0);
                assertThat(row0.getDouble("cbd_congestion_fee")).isEqualTo(0.0);

                // Row 1: VendorID=1, pickup=2025-01-01T00:32:40, trip_distance=0.5
                PqRow row1 = rows.get(1);
                assertThat(row1.getInt("VendorID")).isEqualTo(1);
                assertThat(row1.getTimestamp("tpep_pickup_datetime"))
                        .isEqualTo(Instant.parse("2025-01-01T00:32:40Z"));
                assertThat(row1.getTimestamp("tpep_dropoff_datetime"))
                        .isEqualTo(Instant.parse("2025-01-01T00:35:13Z"));
                assertThat(row1.getLong("passenger_count")).isEqualTo(1L);
                assertThat(row1.getDouble("trip_distance")).isEqualTo(0.5);
                assertThat(row1.getInt("PULocationID")).isEqualTo(236);
                assertThat(row1.getInt("DOLocationID")).isEqualTo(237);
                assertThat(row1.getDouble("fare_amount")).isEqualTo(5.1);
                assertThat(row1.getDouble("tip_amount")).isCloseTo(2.02, within(0.001));
                assertThat(row1.getDouble("total_amount")).isEqualTo(12.12);

                // Row 2: VendorID=1, pickup=2025-01-01T00:44:04, same PU/DO location
                PqRow row2 = rows.get(2);
                assertThat(row2.getInt("VendorID")).isEqualTo(1);
                assertThat(row2.getTimestamp("tpep_pickup_datetime"))
                        .isEqualTo(Instant.parse("2025-01-01T00:44:04Z"));
                assertThat(row2.getInt("PULocationID")).isEqualTo(141);
                assertThat(row2.getInt("DOLocationID")).isEqualTo(141);
                assertThat(row2.getDouble("trip_distance")).isEqualTo(0.6);
                assertThat(row2.getDouble("total_amount")).isEqualTo(12.1);

                // Row 3: VendorID=2, 3 passengers, no tip (payment_type=2 = cash)
                PqRow row3 = rows.get(3);
                assertThat(row3.getInt("VendorID")).isEqualTo(2);
                assertThat(row3.getTimestamp("tpep_pickup_datetime"))
                        .isEqualTo(Instant.parse("2025-01-01T00:14:27Z"));
                assertThat(row3.getLong("passenger_count")).isEqualTo(3L);
                assertThat(row3.getDouble("trip_distance")).isEqualTo(0.52);
                assertThat(row3.getInt("PULocationID")).isEqualTo(244);
                assertThat(row3.getInt("DOLocationID")).isEqualTo(244);
                assertThat(row3.getLong("payment_type")).isEqualTo(2L);
                assertThat(row3.getDouble("tip_amount")).isEqualTo(0.0);
                assertThat(row3.getDouble("congestion_surcharge")).isEqualTo(0.0);
                assertThat(row3.getDouble("total_amount")).isEqualTo(9.7);

                // Row 4: VendorID=2, 3 passengers, different DO location
                PqRow row4 = rows.get(4);
                assertThat(row4.getInt("VendorID")).isEqualTo(2);
                assertThat(row4.getTimestamp("tpep_pickup_datetime"))
                        .isEqualTo(Instant.parse("2025-01-01T00:21:34Z"));
                assertThat(row4.getTimestamp("tpep_dropoff_datetime"))
                        .isEqualTo(Instant.parse("2025-01-01T00:25:06Z"));
                assertThat(row4.getLong("passenger_count")).isEqualTo(3L);
                assertThat(row4.getDouble("trip_distance")).isEqualTo(0.66);
                assertThat(row4.getInt("PULocationID")).isEqualTo(244);
                assertThat(row4.getInt("DOLocationID")).isEqualTo(116);
                assertThat(row4.getDouble("fare_amount")).isEqualTo(5.8);
                assertThat(row4.getDouble("total_amount")).isEqualTo(8.3);
            }
        }
    }
}
