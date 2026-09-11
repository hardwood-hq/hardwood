/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.math.BigInteger;

/// A predicate literal brought onto the values a column's physical carrier holds: the greatest
/// such value at or below the literal, and the least at or above it.
///
/// The two are equal exactly when the column holds the literal itself. They differ when the
/// literal falls between two values the column holds — a sub-microsecond instant on a `MICROS`
/// column, a third decimal place on a `DECIMAL(9, 2)` — and one of them is absent when the
/// literal lies past that end of the carrier's range, where the column holds nothing further.
/// Neither is ever absent on both sides, since a carrier always holds something.
///
/// [BigInteger] rather than `long` because a literal reaches here precisely when it may not fit:
/// `Instant.MAX` in nanoseconds and a `DECIMAL(38)` both overflow the carrier they are being
/// measured against, and the measurement has to survive that to report it.
///
/// @param below the greatest value the column holds at or below the literal, or `null` if it
///        holds none
/// @param above the least value the column holds at or above the literal, or `null` if it holds
///        none
record CarriedLiteral(BigInteger below, BigInteger above) {

    /// The literal narrowed to `[min, max]`, the values the carrier holds.
    ///
    /// @param floor the greatest whole value at or below the literal, before the range is applied
    /// @param ceiling the least whole value at or above the literal, before the range is applied
    static CarriedLiteral within(BigInteger floor, BigInteger ceiling, BigInteger min, BigInteger max) {
        return new CarriedLiteral(
                floor.compareTo(min) < 0 ? null : floor.min(max),
                ceiling.compareTo(max) > 0 ? null : ceiling.max(min));
    }

    /// The literal narrowed to a carrier of unbounded range, which only a `BYTE_ARRAY` `DECIMAL`
    /// has.
    static CarriedLiteral unbounded(BigInteger floor, BigInteger ceiling) {
        return new CarriedLiteral(floor, ceiling);
    }

    /// The inclusive bounds of the values a carrier holds.
    record Range(BigInteger min, BigInteger max) {

        /// The bounds of a two's complement carrier `bytes` bytes wide.
        static Range ofBytes(int bytes) {
            BigInteger magnitude = BigInteger.ONE.shiftLeft(8 * bytes - 1);
            return new Range(magnitude.negate(), magnitude.subtract(BigInteger.ONE));
        }

        boolean holds(BigInteger value) {
            return value.compareTo(min) >= 0 && value.compareTo(max) <= 0;
        }
    }

    /// Whether the column holds the literal itself.
    boolean exact() {
        return below != null && below.equals(above);
    }
}
