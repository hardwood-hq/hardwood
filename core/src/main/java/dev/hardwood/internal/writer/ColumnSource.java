/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

/// A read-only, bulk view over one column's values that the writer pulls from while packing
/// pages. The base carries only the value count — all the [RecordShredder] needs to compute
/// levels and validate offsets. Typed sub-interfaces (`IntColumnSource`, `LongColumnSource`,
/// …) add the primitive bulk copy the per-type value writer reads through, so a source
/// implemented over a foreign columnar container is copied in bounded, primitive chunks
/// rather than one boxed element at a time.
///
/// This is the internal seam behind the public `ColumnBatch` primitive-array setters. A
/// public `ColumnVector` SPI over the same shape — letting callers write from their own
/// containers without an intervening copy — is a later additive layer.
public interface ColumnSource {

    /// The number of values in this source.
    int size();
}
