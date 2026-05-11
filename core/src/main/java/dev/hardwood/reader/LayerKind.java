/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import dev.hardwood.Experimental;

/// Classifies a [ColumnReader] layer between root and leaf.
///
/// A layer is contributed by a user-authored `OPTIONAL` group ([#STRUCT]) or
/// by a `LIST`/`MAP`-annotated group ([#REPEATED]). `REQUIRED` groups and the
/// synthetic scaffolding inside a `LIST`/`MAP` (the inner `repeated group`)
/// do not contribute layers.
///
/// Stable for the lifetime of the [ColumnReader] and safe to cache once at
/// open time.
@Experimental
public enum LayerKind {
    /// A user-authored `OPTIONAL` group along the schema chain. Contributes a
    /// validity bitmap (`getLayerValidity`) but no offsets.
    STRUCT,
    /// A `LIST` or `MAP`-annotated group. Contributes both a validity bitmap
    /// (for the list-itself-null distinction) and an offsets buffer
    /// (`getLayerOffsets`) that walks into the next inner layer's items.
    REPEATED
}
