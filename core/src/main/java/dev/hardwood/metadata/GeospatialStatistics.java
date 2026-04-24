/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

import java.util.List;

/// @param bbox bounding box, or `null` if absent
/// @param geospatialTypes list of geospatial type codes for geometry/geography column, empty list if not known.
///     Values correspond to the Parquet `GeospatialType` enum:
///     0=Point, 1=LineString, 2=Polygon, 3=MultiPoint, 4=MultiLineString, 5=MultiPolygon,
///     6=GeometryCollection.
/// @see <a href="https://github.com/apache/parquet-format/blob/master/Geospatial.md#statistics">Geospatial – statistics</a>
/// @see <a href="https://github.com/apache/parquet-format/blob/master/Geospatial.md#geospatial-types">Geospatial - types</a>
public record GeospatialStatistics(
        BoundingBox bbox,
        List<Integer> geospatialTypes
) {
}
