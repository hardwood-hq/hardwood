/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import dev.hardwood.Experimental;

/// The dictionary of one column chunk: the distinct values a dictionary-encoded column
/// stores once, which [ColumnReader#getDictionaryIds()] refers to by entry. One subtype
/// per physical storage.
///
/// **This API is [Experimental]:** it may change in future releases without prior
/// deprecation.
@Experimental
public sealed interface ColumnDictionary permits BinaryDictionary {

    /// The number of entries; entry ids run from `0` to `size() - 1`.
    int size();
}
