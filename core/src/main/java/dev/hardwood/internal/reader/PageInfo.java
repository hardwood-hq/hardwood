/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.MappedByteBuffer;

import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.schema.ColumnSchema;

/**
 * Holds metadata about a page and the data needed to decode it.
 * <p>
 * The pageData buffer is a slice from the pre-mapped column chunk, avoiding
 * per-page memory mapping overhead.
 * </p>
 */
public record PageInfo(
    MappedByteBuffer pageData,
    ColumnSchema columnSchema,
    ColumnMetaData columnMetaData,
    Dictionary dictionary
) {}
