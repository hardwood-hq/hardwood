# Geospatial Support

**Status: Implemented**

## Context

The Parquet format defines geospatial support through two complementary mechanisms:
logical types (GEOMETRY, GEOGRAPHY) that declare a column's geospatial semantics, and
`GeospatialStatistics` (bounding box + type codes) that enable spatial filter pushdown.
The statistics are only meaningful when the reader can identify geospatial columns via
their logical type, so both must be implemented together.

parquet-java 1.16.0 added the metadata layer (logical types + statistics read/write)
but does not provide spatial filter pushdown — engines like Sedona and DuckDB implement
their own pushdown on top of the Parquet metadata. Hardwood's `Filter.intersects()`
with row group and page-level evaluation would go beyond what parquet-java offers.

## Logical Types

### GeometryType (field 17 in LogicalType union)

Planar/Euclidean geometry stored as WKB in a BYTE_ARRAY column.

| Thrift field | Type | Default | Description |
|---|---|---|---|
| 1 | `optional string` | `"OGC:CRS84"` | CRS identifier |

Edges are always linear (no interpolation field).

### GeographyType (field 18 in LogicalType union)

Geodesic geometry on an ellipsoid, stored as WKB in a BYTE_ARRAY column.

| Thrift field | Type | Default | Description |
|---|---|---|---|
| 1 | `optional string` | `"OGC:CRS84"` | CRS identifier |
| 2 | `optional EdgeInterpolationAlgorithm` | `SPHERICAL` | Edge interpolation |

### EdgeInterpolationAlgorithm union

`EdgeInterpolationAlgorithm` is a Thrift union of empty structs (one per
algorithm), not a flat enum. The selected algorithm is identified by the
field id of the populated struct.

| Field id | Name |
|---|---|
| 1 | SPHERICAL |
| 2 | VINCENTY |
| 3 | THOMAS |
| 4 | ANDOYER |
| 5 | KARNEY |

## Geospatial Statistics

### GeospatialStatistics (field 17 in ColumnMetaData)

Per-column-chunk statistics for spatial filter pushdown.

| Thrift field | Type | Description |
|---|---|---|
| 1 | `optional BoundingBox` | Bounding box of all values in the chunk |
| 2 | `optional list<i32>` | WKB geometry type codes present (unique) |

### BoundingBox

| Thrift field | Type | Description |
|---|---|---|
| 1 | `required double` | xmin |
| 2 | `required double` | xmax |
| 3 | `required double` | ymin |
| 4 | `required double` | ymax |
| 5 | `optional double` | zmin |
| 6 | `optional double` | zmax |
| 7 | `optional double` | mmin |
| 8 | `optional double` | mmax |

For GEOGRAPHY columns, x is longitude ([-180, 180]) and y is latitude ([-90, 90]).
`xmin > xmax` is legal and indicates antimeridian wrapping.

The same `GeospatialStatistics` struct also appears in `ColumnIndex` (field 7) for
page-level spatial filtering.

## Implementation

Steps 1–4 build on each other and should land in order. Step 1 (logical types) is a
prerequisite for step 2 — the statistics are not useful to consumers until the reader
can identify geospatial columns.

### Step 1 — Logical types

Add `GeometryType` and `GeographyType` to the `LogicalType` sealed interface, with
an `EdgeInterpolation` enum.

**`LogicalType.java`**: Add to the `permits` clause and define:

```java
record GeometryType(String crs) implements LogicalType {}
record GeographyType(String crs, EdgeInterpolation edgeInterpolation) implements LogicalType {}

enum EdgeInterpolation {
    SPHERICAL, VINCENTY, THOMAS, ANDOYER, KARNEY
}
```

Both CRS fields default to `"OGC:CRS84"` when absent in the Thrift stream. The reader
should materialize this default rather than leaving it `null`.

**`LogicalTypeReader.java`**: Add cases 17 and 18 in the field switch with dedicated
`readGeometryType` / `readGeographyType` methods following the existing pattern
(e.g. `readTimestampType`).

No changes needed in `SchemaElementReader` — it already delegates field 10 to
`LogicalTypeReader.read()`.

### Step 2 — Geospatial statistics on ColumnMetaData

`GeospatialStatistics` and `BoundingBox` records in the `metadata` package,
`GeospatialStatisticsReader` and `BoundingBoxReader` in the Thrift package, and a
`geospatialStatistics` field on `ColumnMetaData`.

### Step 3 — Page-level geospatial statistics on ColumnIndex

Add `List<GeospatialStatistics> geospatialStatistics` (one per page) to the `ColumnIndex`
record, parsed from field 7 of the Thrift `ColumnIndex` struct. This enables page-level
spatial filter pushdown analogous to the existing min/max page index.

### Step 4 — Spatial bounding box predicate

Add a `IntersectsPredicate` to the `FilterPredicate` sealed interface:

```java
record IntersectsPredicate(String column, double xmin, double ymin,
        double xmax, double ymax) implements FilterPredicate {}
```

Parameter order follows the GeoJSON/WKT convention: `(xmin, ymin, xmax, ymax)` —
bottom-left corner then top-right corner. This differs from JTS `Envelope(xmin, xmax,
ymin, ymax)`, so care is needed when converting between the two.

Z/M dimensions are omitted from the initial predicate — 2D bounding box filtering
covers the vast majority of use cases. A 3D overload can be added later if needed.

With a factory method on `FilterPredicate`:

```java
static FilterPredicate intersects(String column, double xmin, double ymin,
        double xmax, double ymax) {
    return new IntersectsPredicate(column, xmin, ymin, xmax, ymax);
}
```

The predicate composes with existing combinators (`and`, `or`, `not`):

```java
FilterPredicate.and(
    FilterPredicate.intersects("geom", -5.0, 50.0, 2.0, 60.0),
    FilterPredicate.gt("population", 100_000)
)
```

#### Row group evaluation

In `RowGroupFilterEvaluator`, add a case for `IntersectsPredicate`. Instead of
decoding `Statistics` min/max bytes, it reads `ColumnMetaData.geospatialStatistics().bbox()`
and checks whether the column chunk's bounding box intersects the query box:

```
canDrop = chunk bbox is entirely outside the query bbox
```

Two axis-aligned bounding boxes do *not* intersect when any of the following is true:

- `chunkBbox.xmax < queryXmin`
- `chunkBbox.xmin > queryXmax`
- `chunkBbox.ymax < queryYmin`
- `chunkBbox.ymin > queryYmax`

For GEOGRAPHY columns with antimeridian wrapping (`xmin > xmax`), the x-axis overlap
check must account for the wrap: the chunk spans `[xmin, 180] ∪ [-180, xmax]`. The
simplest approach is to detect wrapping on either box and split into two half-ranges
before testing overlap.

If `geospatialStatistics` or `bbox` is null but the column has a geospatial logical
type, the evaluator returns false (don't drop) — consistent with the existing behavior
for absent statistics.

If the resolved column does not have a GEOMETRY or GEOGRAPHY logical type, the
evaluator throws `IllegalArgumentException`.

#### Page-level evaluation

In `PageFilterEvaluator`, add a case for `IntersectsPredicate`. For each page, read
the per-page `GeospatialStatistics` from the `ColumnIndex` (step 3) and apply the same
intersection test. Pages whose bounding box does not intersect the query box are excluded
from the `RowRanges` result.

If the column index does not contain geospatial statistics, fall back to
`RowRanges.all()` (no page-level filtering), consistent with existing behavior.

### Step 5 — End-to-end test with JTS

Add JTS (`org.locationtech.jts:jts-core`) as a test dependency in the core module.
Write an end-to-end test that exercises the full chain a real user would follow:

1. **Generate a test Parquet file** (via `simple-datagen.py`) with a GEOMETRY column
   containing WKB-encoded points spread across multiple row groups with distinct
   bounding boxes — e.g. one row group with European cities, one with Asian cities,
   one with North American cities.

2. **Read with spatial filter pushdown**:

```java
@Test
void spatialFilterSkipsNonMatchingRowGroups() throws IOException {
    // Query: find geometries within Europe (roughly)
    FilterPredicate filter = FilterPredicate.intersects("location",
            -25.0, 35.0, 45.0, 72.0);

    int totalRows;
    try (ParquetFileReader unfiltered = ParquetFileReader.open(InputFile.of(path))) {
        totalRows = Math.toIntExact(unfiltered.getFileMetaData().numRows());
    }

    int filteredRows = 0;
    try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(path));
            RowReader rowReader = fileReader.createRowReader(filter)) {

        WKBReader wkbReader = new WKBReader();

        while (rowReader.hasNext()) {
            rowReader.next();
            filteredRows++;

            // Verify WKB is decodable by JTS
            byte[] wkb = rowReader.getBytes("location");
            Geometry geom = wkbReader.read(wkb);
            assertThat(geom).isNotNull();
        }
    }

    // Pushdown must have skipped at least one row group
    assertThat(filteredRows).isLessThan(totalRows);
    assertThat(filteredRows).isGreaterThan(0);
}
```

The test validates:
- WKB byte layout produced by standard tools is decodable by JTS
- the `intersects` predicate actually drops non-overlapping row groups (fewer rows returned)
- the end-to-end user experience from filter creation to geometry decoding

Note that individual rows within a surviving row group may fall outside the query
box — pushdown is coarse-grained. The test asserts that *some* row groups were
skipped, not that every row matches.

### Step 6 — Documentation

- Update `docs/content/usage.md` with examples showing:
    - how to identify geospatial columns via their logical type
    - how to access bounding box metadata
    - how to use `intersects` for filter pushdown
- Add test Parquet files (via `simple-datagen.py`) that contain GEOMETRY/GEOGRAPHY
  logical types with geospatial statistics at both column chunk and page level.
- Unit tests for the spatial predicate evaluator:
    - basic intersection / non-intersection
    - antimeridian wrapping
    - absent statistics (should not drop)
    - composition with other predicates via `and`/`or`

## Files to create or modify

| File | Action |
|---|---|
| `core/.../metadata/LogicalType.java` | Add `GeometryType`, `GeographyType`, `EdgeInterpolation` |
| `core/.../thrift/LogicalTypeReader.java` | Add cases 17, 18 with read methods |
| `core/.../metadata/GeospatialStatistics.java` | Create |
| `core/.../metadata/BoundingBox.java` | Create |
| `core/.../thrift/GeospatialStatisticsReader.java` | Create |
| `core/.../thrift/BoundingBoxReader.java` | Create |
| `core/.../thrift/ColumnMetaDataReader.java` | Add field 17 |
| `core/.../metadata/ColumnMetaData.java` | Add `geospatialStatistics` field |
| `core/.../metadata/ColumnIndex.java` | Add `geospatialStatistics` field |
| `core/.../thrift/ColumnIndexReader.java` | Add field 7 |
| `core/.../reader/FilterPredicate.java` | Add `IntersectsPredicate` record and factory |
| `core/.../internal/reader/RowGroupFilterEvaluator.java` | Add spatial bbox evaluation |
| `core/.../internal/reader/PageFilterEvaluator.java` | Add spatial bbox evaluation |
| `core/pom.xml` | Add JTS test dependency |
| `core/.../GeospatialEndToEndTest.java` | End-to-end test with JTS |
| `docs/content/usage.md` | Update with geospatial examples |
| `simple-datagen.py` | Generate test files with geospatial logical types |
