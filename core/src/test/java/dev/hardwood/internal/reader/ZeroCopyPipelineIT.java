/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.jfr.AbstractJfrRecorderTest;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import jdk.jfr.consumer.RecordedEvent;

import static org.assertj.core.api.Assertions.assertThat;

class ZeroCopyPipelineIT extends AbstractJfrRecorderTest {

    private static final Path FIXTURE = Paths.get("src/test/resources/ffm_coverage_test.parquet");
    private static final int ROWS = 1000;

    @Test
    void testZeroCopyDecodePaths() throws Exception {
        Assumptions.assumeTrue(Files.exists(FIXTURE), "Run simple-datagen.py first");

        // The file contains INT32, INT64, FLOAT, DOUBLE using both PLAIN and BYTE_STREAM_SPLIT
        // It uses GZIP and DATA_PAGE_V2 to trigger the FFM fast paths.
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rowReader = reader.createRowReader()) {

            int count = 0;
            while (rowReader.hasNext()) {
                rowReader.next();
                
                int i = count;
                boolean isEven = i % 2 == 0;

                // PLAIN reads
                assertThat(rowReader.getInt("i32_plain")).isEqualTo(i);
                assertThat(rowReader.isNull("i32_plain_opt")).isEqualTo(!isEven);
                
                assertThat(rowReader.getLong("i64_plain")).isEqualTo(i * 10L);
                assertThat(rowReader.isNull("i64_plain_opt")).isEqualTo(!isEven);
                
                assertThat(rowReader.getFloat("f32_plain")).isEqualTo((float) i * 1.5f);
                assertThat(rowReader.isNull("f32_plain_opt")).isEqualTo(!isEven);
                
                assertThat(rowReader.getDouble("f64_plain")).isEqualTo((double) i * 3.14);
                assertThat(rowReader.isNull("f64_plain_opt")).isEqualTo(!isEven);

                // BSS reads
                assertThat(rowReader.getInt("i32_bss")).isEqualTo(i);
                assertThat(rowReader.isNull("i32_bss_opt")).isEqualTo(!isEven);
                
                assertThat(rowReader.getLong("i64_bss")).isEqualTo(i * 10L);
                assertThat(rowReader.isNull("i64_bss_opt")).isEqualTo(!isEven);
                
                assertThat(rowReader.getFloat("f32_bss")).isEqualTo((float) i * 1.5f);
                assertThat(rowReader.isNull("f32_bss_opt")).isEqualTo(!isEven);
                
                assertThat(rowReader.getDouble("f64_bss")).isEqualTo((double) i * 3.14);
                assertThat(rowReader.isNull("f64_bss_opt")).isEqualTo(!isEven);

                count++;
            }
            assertThat(count).isEqualTo(ROWS);
        }

        awaitEvents();

        // 16 columns * 1 row group = 16 pages minimally (actually may be more if split, but PyArrow writes 1 page per col for 1000 rows)
        Stream<RecordedEvent> decodedEvents = events("dev.hardwood.PageDecoded");
        
        // Assert that the nativeFastPath was true for the FFM compatible pages!
        // We know they are V2 and GZIP, and all types are primitive numeric types we support.
        // Wait, does Java 22 FFM actually run in testing? Yes, failsafe runs with the packaged JAR.
        long totalPages = events("dev.hardwood.PageDecoded").count();
        long ffmPages = events("dev.hardwood.PageDecoded")
                .filter(e -> e.getBoolean("nativeFastPath"))
                .count();

        // If running on Java 22+, ffmPages should equal totalPages since all columns in this file are supported.
        // If running on Java 21 fallback, ffmPages will be 0.
        // But since this is a coverage enhancement, we want to ensure we hit the FFM paths natively!
        // The project test suite executes on Java 22 for failsafe.
        // Let's at least log it, but assert softly if needed.
        if (Runtime.version().feature() >= 22) {
            assertThat(ffmPages).isGreaterThan(0).isEqualTo(totalPages);
        }
    }
}
