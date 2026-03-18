/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.ByteBuffer;

/**
 * Raw index buffers for a single column chunk within a row group.
 * <p>
 * Parquet stores two complementary page-level indexes per column chunk:
 * </p>
 * <ul>
 *   <li><b>Offset Index</b> — the <em>location</em> of each page: file offset,
 *       compressed size, and first row index. Used by
 *       {@link PageScanner#scanPagesFromIndex()} to seek directly to pages
 *       without scanning headers sequentially.</li>
 *   <li><b>Column Index</b> — the <em>statistics</em> of each page: min/max
 *       values, null counts, and boundary order. Will be used for page-level
 *       predicate pushdown (skipping pages whose value range doesn't match
 *       the filter).</li>
 * </ul>
 * <p>
 * Either buffer may be {@code null} if the file does not contain that index
 * type. The buffers are slices of a shared region fetched by
 * {@link RowGroupIndexBuffers}.
 * </p>
 *
 * @param offsetIndex raw bytes of the offset index, or {@code null}
 * @param columnIndex raw bytes of the column index, or {@code null}
 */
public record ColumnIndexBuffers(ByteBuffer offsetIndex, ByteBuffer columnIndex) {
}
