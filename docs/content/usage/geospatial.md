<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Geospatial Support

Hardwood reads the [Parquet geospatial metadata layer](https://parquet.apache.org/docs/file-format/types/geospatial/) — GEOMETRY / GEOGRAPHY logical types and per-chunk / per-page `GeospatialStatistics` — and offers a bounding-box filter predicate that pushes spatial selectivity down to the row group and page level. Hardwood does not decode WKB payloads itself — geometry decoding is left to the caller, so the reader has no runtime geometry-library dependency. The de-facto standard Java library for this is the [JTS Topology Suite](https://locationtech.github.io/jts/); the snippets below assume JTS, but any WKB decoder works.

To use JTS in the examples below, add the `jts-core` dependency:

```xml
<dependency>
    <groupId>org.locationtech.jts</groupId>
    <artifactId>jts-core</artifactId>
    <version>1.20.0</version>
</dependency>
```

### Identifying Geospatial Columns

GEOMETRY (planar) and GEOGRAPHY (geodesic on an ellipsoid) appear as `LogicalType.GeometryType` and `LogicalType.GeographyType` on the column schema. Both carry a CRS (defaulting to `OGC:CRS84`); GEOGRAPHY also carries an edge-interpolation algorithm.

```java
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path))) {
    FileSchema schema = reader.getFileSchema();
    for (int i = 0; i < schema.getColumnCount(); i++) {
        ColumnSchema column = schema.getColumn(i);
        if (column.logicalType() instanceof LogicalType.GeometryType geomType) {
            System.out.println(column.name() + " is a GEOMETRY column, CRS: " + geomType.crs());
        }
        else if (column.logicalType() instanceof LogicalType.GeographyType geoType) {
            System.out.println(column.name() + " is a GEOGRAPHY column, CRS: " + geoType.crs()
                + ", edge interpolation: " + geoType.edgeInterpolation());
        }
    }
}
```

### Bounding-Box Statistics

Per-chunk geospatial statistics are exposed on `ColumnMetaData.geospatialStatistics()`. The `BoundingBox` carries `xmin/xmax/ymin/ymax` (always present) plus optional `zmin/zmax/mmin/mmax`. For GEOGRAPHY columns, `xmin > xmax` is legal and indicates a chunk that wraps the antimeridian. The same struct also appears per-page on `ColumnIndex.geospatialStatistics()` when the file was written with a Page Index.

```java
import dev.hardwood.metadata.BoundingBox;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.GeospatialStatistics;
import dev.hardwood.metadata.RowGroup;

for (RowGroup rowGroup : reader.getFileMetaData().rowGroups()) {
    for (ColumnChunk chunk : rowGroup.columns()) {
        GeospatialStatistics geo = chunk.metaData().geospatialStatistics();
        if (geo == null) {
            continue;
        }
        BoundingBox bbox = geo.bbox();
        if (bbox != null) {
            System.out.println("  bbox: x=[" + bbox.xmin() + ", " + bbox.xmax() + "], "
                + "y=[" + bbox.ymin() + ", " + bbox.ymax() + "]");
        }
        // Geospatial type codes (Point=1, LineString=2, Polygon=3, MultiPoint=4, etc.)
        System.out.println("  types: " + geo.geospatialTypes());
    }
}
```

### Spatial Filter Pushdown

`FilterPredicate.intersects(column, xmin, ymin, xmax, ymax)` produces a predicate that drops row groups and pages whose stored bounding box does not overlap the query box. The argument order follows the GeoJSON / WKT convention (bottom-left corner, then top-right corner). Antimeridian wrapping is handled automatically.

```java
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBReader;

FilterPredicate filter = FilterPredicate.intersects("location", -25.0, 35.0, 45.0, 72.0);
try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(path));
        RowReader rowReader = fileReader.createRowReader(filter)) {
    WKBReader wkbReader = new WKBReader();
    while (rowReader.hasNext()) {
        rowReader.next();
        byte[] wkb = rowReader.getBinary("location");
        // Decode the WKB payload with a geometry library of your choice (e.g. JTS).
        Geometry geom = wkbReader.read(wkb);
        // Apply any exact-geometry tests here.
    }
}
```

The predicate composes with the standard `and` / `or` combinators, e.g.:

```java
FilterPredicate.and(
    FilterPredicate.intersects("location", -5.0, 50.0, 2.0, 60.0),
    FilterPredicate.gt("population", 100_000));
```

### Limitations

`intersects` is **coarse-grained**: Hardwood drops row groups and pages whose bounding box is disjoint from the query box, but every row in a surviving page is returned. Rows whose individual geometry falls outside the query box are emitted along with truly intersecting ones. Apply your own per-row check on the WKB payload (e.g. via JTS) when you need exact geometric filtering — the bounding-box pushdown still saves the I/O for non-overlapping chunks.

Negation of `intersects` is **not supported**: `FilterPredicate.not(FilterPredicate.intersects(...))` throws `UnsupportedOperationException` at resolve time. The chunk-level criterion for "no row intersects" requires bbox containment rather than overlap, and the per-row dual would require decoding every WKB payload inside the reader — which would pull a geometry library into the runtime. If you need "geometries outside this box", read without a spatial filter and apply the negation yourself against `getBinary("location")`.

Both limitations are tracked by [hardwood#414](https://github.com/hardwood-hq/hardwood/issues/414), which proposes opt-in row-level evaluation via an optional WKB-decoder dependency.

