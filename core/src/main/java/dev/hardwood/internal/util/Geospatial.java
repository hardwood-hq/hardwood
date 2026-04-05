/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.util;

/// Helpers for geospatial analysis
public class Geospatial {

    /// Check for overlap along X-axis taking into account antimeridian wrapping
    public static boolean checkForXaxisOverlap(double xMinA, double xMaxA,
                                           double xMinB, double xMaxB) {
        boolean aWraps = xMinA > xMaxA;
        boolean bWraps = xMinB > xMaxB;

        if (!aWraps && !bWraps) {
            // Standard case
            return !(xMaxA < xMinB || xMinA > xMaxB);
        }
        if (aWraps && bWraps) {
            // Both wrap — always overlap on x axis
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
