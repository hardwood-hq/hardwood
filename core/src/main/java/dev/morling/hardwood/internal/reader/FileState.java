/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.reader;

import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.List;

import dev.morling.hardwood.metadata.FileMetaData;
import dev.morling.hardwood.schema.FileSchema;

/**
 * Holds prepared state for a Parquet file ready for cross-file prefetching.
 * <p>
 * Contains pre-scanned pages organized by column index, allowing PageCursors
 * to extend with pages from the next file without re-scanning.
 * </p>
 *
 * @param path the path to the Parquet file
 * @param channel the open file channel for memory-mapped access
 * @param fileMetaData the parsed file metadata
 * @param fileSchema the parsed file schema
 * @param pageInfosByColumn pre-scanned pages for each column (by projected column index)
 */
public record FileState(
    Path path,
    FileChannel channel,
    FileMetaData fileMetaData,
    FileSchema fileSchema,
    List<List<PageInfo>> pageInfosByColumn
) {}
