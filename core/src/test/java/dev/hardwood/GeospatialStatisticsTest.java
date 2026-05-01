/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.GeospatialStatistics;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetFileReader;

import static org.assertj.core.api.Assertions.assertThat;

public class GeospatialStatisticsTest {

    @Test
    public void testGeospatialStatisticsAreReadCorrectly() throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(Paths.get("src/test/resources/geospatial_stats_test.parquet")))) {
            FileMetaData metadata = reader.getFileMetaData();
            assertThat(metadata.rowGroups()).hasSize(1);

            RowGroup rg0 = metadata.rowGroups().getFirst();
            assertThat(rg0.columns().get(0).metaData().geospatialStatistics()).isNull();
            ColumnChunk cc = rg0.columns().get(1);
            ColumnMetaData md = cc.metaData();
            GeospatialStatistics geospatialStatistics = md.geospatialStatistics();
            assertThat(geospatialStatistics).isNotNull();
            assertThat(geospatialStatistics.bbox().xmin()).isEqualTo(-4.0);
            assertThat(geospatialStatistics.bbox().xmax()).isEqualTo(7.5);
            assertThat(geospatialStatistics.bbox().ymin()).isEqualTo(20.96);
            assertThat(geospatialStatistics.bbox().ymax()).isEqualTo(77.08);
            assertThat(geospatialStatistics.bbox().zmin()).isEqualTo(10.5);
            assertThat(geospatialStatistics.bbox().zmax()).isEqualTo(90.0);
            assertThat(geospatialStatistics.bbox().mmin()).isNull();
            assertThat(geospatialStatistics.bbox().mmax()).isNull();
            assertThat(geospatialStatistics.geospatialTypes()).containsExactly(1, 6);
        }
    }

    @Test
    public void testGeospatialStatisticsWithBboxOnly() throws IOException {
        GeospatialStatistics stats = readGeospatialStatistics(
                Paths.get("src/test/resources/geospatial_stats_bbox_only.parquet"));

        assertThat(stats).isNotNull();
        assertThat(stats.bbox()).isNotNull();
        assertThat(stats.bbox().xmin()).isEqualTo(-4.0);
        assertThat(stats.bbox().xmax()).isEqualTo(7.5);
        assertThat(stats.bbox().ymin()).isEqualTo(20.96);
        assertThat(stats.bbox().ymax()).isEqualTo(77.08);
        assertThat(stats.geospatialTypes()).isEmpty();
    }

    @Test
    public void testGeospatialStatisticsWithTypesOnly() throws IOException {
        GeospatialStatistics stats = readGeospatialStatistics(
                Paths.get("src/test/resources/geospatial_stats_types_only.parquet"));

        assertThat(stats).isNotNull();
        assertThat(stats.bbox()).isNull();
        assertThat(stats.geospatialTypes()).containsExactly(1);
    }

    private static GeospatialStatistics readGeospatialStatistics(Path file) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            return reader.getFileMetaData()
                    .rowGroups().getFirst()
                    .columns().get(1).metaData()
                    .geospatialStatistics();
        }
    }
}
