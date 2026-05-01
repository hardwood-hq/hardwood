/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.util;

/// Helpers for geospatial analysis.
public class Geospatial {

    /// Tests whether two longitude ranges overlap, accounting for antimeridian wrapping
    /// (`xmin > xmax` indicates a range that crosses the 180/-180 boundary).
    public static boolean xAxisOverlaps(double xMinA, double xMaxA, double xMinB, double xMaxB) {
        boolean aWraps = xMinA > xMaxA;
        boolean bWraps = xMinB > xMaxB;

        if (!aWraps && !bWraps) {
            return !(xMaxA < xMinB || xMinA > xMaxB);
        }
        if (aWraps && bWraps) {
            // Both wrap — both contain the antimeridian, so they always overlap.
            return true;
        }
        if (aWraps) {
            // A spans [xMinA, 180] ∪ [-180, xMaxA]
            return xMaxB >= xMinA || xMinB <= xMaxA;
        }
        // B wraps: [xMinB, 180] ∪ [-180, xMaxB]
        return xMaxA >= xMinB || xMinA <= xMaxB;
    }
}
