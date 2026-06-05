/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.testing;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

import dev.hardwood.InputFile;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/// Exercises geospatial filter pushdown over the upstream `geospatial-with-nan.parquet` fixture.
///
/// That file's `geometry` column carries `NaN` coordinates in one row's WKB (the `LINESTRING ZM`
/// has a `nan nan nan nan` vertex), while the column's bounding-box statistics stay finite
/// (`x` 10..130, `y` 20..140) — the writer excluded `NaN` from the bounds, as the spec requires.
/// These tests pin that an `intersects` filter prunes on that finite box correctly and that the
/// `NaN` in the data neither corrupts stats reading nor drops a matching row, cross-checking the
/// result against the reference parquet-java reader.
class GeospatialNaNFilterTest {

    private static Path file;

    @BeforeAll
    static void setUp() throws IOException {
        ParquetTestingRepoCloner.ensureCloned();
        file = Paths.get("target", "parquet-testing", "data", "geospatial", "geospatial-with-nan.parquet");
    }

    @Test
    void coveringQueryReturnsSameGeometriesAsParquetJava() throws IOException, ParseException {
        // parquet-java applies no spatial pushdown, so it reads the whole file — the oracle for a
        // query whose box covers the column's finite bbox, which hardwood must reproduce exactly.
        List<byte[]> reference = geometryWkb(Utils.readWithParquetJava(file));
        List<byte[]> actual = readGeometryWkb(FilterPredicate.intersects("geometry", 0.0, 0.0, 200.0, 200.0));

        // Compare raw WKB bytes, not decoded geometries: the NaN vertex makes JTS equality false
        // (NaN != NaN), so byte identity is the meaningful "same results" check.
        assertThat(toHex(actual)).containsExactlyElementsOf(toHex(reference));

        // The NaN-bearing linestring must still decode, with its NaN vertex preserved.
        assertThat(actual).hasSize(3);
        LineString lineString = (LineString) new WKBReader().read(actual.get(2));
        assertThat(lineString.getNumPoints()).isEqualTo(3);
        assertThat(lineString.getCoordinateN(0).x).isEqualTo(90.0);
        assertThat(Double.isNaN(lineString.getCoordinateN(1).x)).isTrue();
        assertThat(lineString.getCoordinateN(2).x).isEqualTo(130.0);
    }

    @Test
    void disjointQueryDropsRowGroup() throws IOException {
        // Query box lies entirely outside the finite bbox: the row group is pruned to zero rows.
        // parquet-java has no spatial pushdown, so there is no like-for-like oracle here — this pins
        // that hardwood's bbox pruning fires (and a NaN bound would not have caused this drop, since
        // the bounds are finite).
        List<byte[]> actual = readGeometryWkb(FilterPredicate.intersects("geometry", -100.0, -100.0, -50.0, -50.0));

        assertThat(actual).isEmpty();
    }

    private static List<byte[]> readGeometryWkb(FilterPredicate filter) throws IOException {
        List<byte[]> result = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
                RowReader rowReader = reader.buildRowReader().filter(filter).build()) {
            while (rowReader.hasNext()) {
                rowReader.next();
                result.add(rowReader.getBinary("geometry"));
            }
        }
        return result;
    }

    /// Extracts the `geometry` WKB bytes from each parquet-java reference row.
    private static List<byte[]> geometryWkb(List<GenericRecord> rows) {
        List<byte[]> result = new ArrayList<>();
        for (GenericRecord row : rows) {
            ByteBuffer buffer = (ByteBuffer) row.get("geometry");
            byte[] bytes = new byte[buffer.remaining()];
            buffer.duplicate().get(bytes);
            result.add(bytes);
        }
        return result;
    }

    private static List<String> toHex(List<byte[]> values) {
        List<String> result = new ArrayList<>();
        for (byte[] value : values) {
            result.add(HexFormat.of().formatHex(value));
        }
        return result;
    }
}
