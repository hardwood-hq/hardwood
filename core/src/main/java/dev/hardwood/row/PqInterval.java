/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.row;

/// Representation of Interval Logical Type
///
/// @param months       number of months
/// @param days         number of days
/// @param milliseconds number of milliseconds
public record PqInterval(int months, int days, int milliseconds) {}
