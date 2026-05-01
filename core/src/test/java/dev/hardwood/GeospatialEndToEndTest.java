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
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

public class GeospatialEndToEndTest {

    @Test
    void spatialFilterSkipsNonMatchingRowGroups() throws IOException, ParseException {
        // Query: find geometries within Europe (roughly)
        FilterPredicate filter = FilterPredicate.intersects("location",
                -25.0, 35.0, 45.0, 72.0);

        int totalRows;
        Path path = Paths.get("src/test/resources/geospatial_e2e_test.parquet");
        try (ParquetFileReader unfiltered = ParquetFileReader.open(InputFile.of(path))) {
            totalRows = Math.toIntExact(unfiltered.getFileMetaData().numRows());
        }

        int filteredRows = 0;
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(path));
             RowReader rowReader = fileReader.buildRowReader()
                     .filter(filter)
                     .build()) {
            List<String> cities = new ArrayList<>();
            GeometryFactory gf = new GeometryFactory();
            Point london = gf.createPoint(new Coordinate(-0.12, 51.50));
            Point paris = gf.createPoint(new Coordinate(2.35, 48.85));
            Point berlin = gf.createPoint(new Coordinate(13.40, 52.52));

            List<Geometry> actualLocations = new ArrayList<>();
            WKBReader wkbReader = new WKBReader();
            while (rowReader.hasNext()) {
                rowReader.next();
                filteredRows++;

                // Verify WKB is decodable by JTS
                byte[] wkb = rowReader.getBinary("location");
                Geometry geom = wkbReader.read(wkb);
                assertThat(geom).isNotNull();
                assertThat(geom).isInstanceOf(Point.class);
                cities.add(rowReader.getString("city_name"));
                actualLocations.add(geom);
            }
            assertThat(cities).containsExactly("London", "Paris", "Berlin");
            assertThat(actualLocations).containsExactly(london, paris, berlin);
        }

        // Pushdown must have skipped at least one row group
        assertThat(filteredRows).isLessThan(totalRows);
        assertThat(filteredRows).isGreaterThan(0);
        assertThat(totalRows).isEqualTo(9);
        assertThat(filteredRows).isEqualTo(3);
    }
}
