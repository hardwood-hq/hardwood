/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine.micro;

/// How far the consumer's demand reaches, in rows: what its next batch needs now (`need`), and how
/// far the read may run ahead of it (`want`). The poll computes it once from the consumer's
/// position and hands it to every stage that starts work, which takes it as given: the read-ahead
/// cap and the priority of needs over wants are this one value.
record Demand(long need, long want) {

    /// The demand of a consumer that has taken every row before `consumerRow`.
    static Demand at(long consumerRow) {
        return new Demand(consumerRow + Engine.BATCH_ROWS, consumerRow + Engine.AHEAD_ROWS);
    }
}
