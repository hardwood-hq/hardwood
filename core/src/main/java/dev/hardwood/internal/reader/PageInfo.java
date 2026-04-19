/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.ByteBuffer;

import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.schema.ColumnSchema;

/// Page metadata and resolved data buffer.
///
/// The fetch plan resolves the page bytes before creating the `PageInfo`.
/// By the time a decode task calls [#pageData()], the buffer is ready —
/// no lazy I/O, no [ChunkHandle] reference. This keeps `PageInfo` a
/// simple data holder.
public class PageInfo {

    private final ByteBuffer pageData;
    private final ColumnSchema columnSchema;
    private final ColumnMetaData columnMetaData;
    private final Dictionary dictionary;

    public PageInfo(ByteBuffer pageData, ColumnSchema columnSchema,
                    ColumnMetaData columnMetaData, Dictionary dictionary) {
        this.pageData = pageData;
        this.columnSchema = columnSchema;
        this.columnMetaData = columnMetaData;
        this.dictionary = dictionary;
    }

    /// Returns the page data buffer (header + compressed data).
    public ByteBuffer pageData() {
        return pageData;
    }

    public ColumnSchema columnSchema() {
        return columnSchema;
    }

    public ColumnMetaData columnMetaData() {
        return columnMetaData;
    }

    public Dictionary dictionary() {
        return dictionary;
    }
}
