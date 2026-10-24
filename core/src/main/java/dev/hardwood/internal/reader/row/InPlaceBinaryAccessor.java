/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.row;

import dev.hardwood.internal.reader.batch.BinaryValueTest;
import dev.hardwood.row.StructAccessor;

/// A [StructAccessor] that hands a binary field's value to a [BinaryValueTest] where it sits in
/// its batch, so a record matcher tests the value without a copy of it.
public interface InPlaceBinaryAccessor extends StructAccessor {

    /// Tests the value of the binary field `name`.
    ///
    /// @throws IllegalStateException if the field is null
    boolean testBinary(String name, BinaryValueTest test);

    /// Tests the value of the binary field at `index`, in the index space of the accessor's
    /// `getBinary(int)`.
    ///
    /// @throws IllegalStateException if the field is null
    boolean testBinary(int index, BinaryValueTest test);
}
