package dev.hardwood.metadata;

import java.util.List;

/// @param bbox bounding box, or `null` if absent
/// @param geospatialTypes list of geospatial type for geometry/geography column, empty list if not known
/// @see <a href="https://github.com/apache/parquet-format/blob/master/Geospatial.md#statistics">Geospatial – statistics</a>
/// @see <a href="https://github.com/apache/parquet-format/blob/master/Geospatial.md#geospatial-types">Geospatial - types</a>
public record GeospatialStatistics(
        BoundingBox bbox,
        List<Integer> geospatialTypes
) {
}
