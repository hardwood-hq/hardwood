/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.ByteBuffer;

/// Raw index buffers for a single column chunk within a row group.
///
/// Parquet stores two complementary page-level indexes per column chunk:
///
/// - **Offset Index** — the *location* of each page: file offset,
///       compressed size, and first row index. Used to build an
///       [IndexedFetchPlan], which seeks directly to pages without scanning
///       headers sequentially.
/// - **Column Index** — the *statistics* of each page: min/max
///       values, null counts, and boundary order. Used for page-level
///       predicate pushdown (skipping pages whose value range doesn't match
///       the filter).
///
/// Either buffer may be `null` if the file does not contain that index
/// type. The buffers are slices of a shared region fetched by
/// [RowGroupIndexBuffers].
///
/// @param offsetIndex raw bytes of the offset index, or `null`
/// @param columnIndex raw bytes of the column index, or `null`
public record ColumnIndexBuffers(ByteBuffer offsetIndex, ByteBuffer columnIndex) {
}
