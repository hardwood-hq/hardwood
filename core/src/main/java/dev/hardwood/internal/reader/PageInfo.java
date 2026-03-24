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

/// Holds metadata about a page and the data needed to decode it.
///
/// The pageData buffer is a slice from the file data, avoiding
/// per-page copy overhead.
public record PageInfo(
    ByteBuffer pageData,
    ColumnSchema columnSchema,
    ColumnMetaData columnMetaData,
    Dictionary dictionary
) {}
