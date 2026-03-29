/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
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
        double xmin,
        double xmax,
        double ymin,
        double ymax,
        Double zmin,
        Double zmax,
        Double mmin,
        Double mmax) {
}
