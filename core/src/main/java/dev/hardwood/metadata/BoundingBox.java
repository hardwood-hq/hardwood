package dev.hardwood.metadata;

/// @param xmin x coordinate of bottom-left vertex
/// @param ymin y coordinate of bottom-left vertex
/// @param xmax x coordinate of top-right vertex
/// @param ymax y coordinate of top-right vertex
/// @param zmin minimum height of bounded volume, or `null` if absent
/// @param zmax maximum height of bounded volume, or `null` if absent
/// @param mmin minimum of a value in 4th dimension, or `null` if absent
/// @param mmax maximum of a value in 4th dimension, or `null` if absent
/// @see <a href="https://github.com/apache/parquet-format/blob/master/Geospatial.md#bounding-box">Geospatial - bounding-box</a>
public record BoundingBox(
        Double xmin,
        Double xmax,
        Double ymin,
        Double ymax,
        Double zmin,
        Double zmax,
        Double mmin,
        Double mmax) {
}
