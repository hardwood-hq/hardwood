/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.MappedByteBuffer;
import java.nio.file.Path;
import java.util.List;

import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.schema.FileSchema;

/**
 * Holds prepared state for a Parquet file ready for multi-file reading.
 * <p>
 * Contains pre-scanned pages organized by column index, allowing PageCursors
 * to extend with pages from the next file without re-scanning.
 * </p>
 * <p>
 * The file channel is closed immediately after memory-mapping; the MappedByteBuffer
 * remains valid and is released when garbage collected.
 * </p>
 *
 * @param path the path to the Parquet file
 * @param fileMapping the memory-mapped buffer covering the entire file
 * @param fileMetaData the parsed file metadata
 * @param fileSchema the parsed file schema
 * @param pageInfosByColumn pre-scanned pages for each column (by projected column index)
 */
public record FileState(
    Path path,
    MappedByteBuffer fileMapping,
    FileMetaData fileMetaData,
    FileSchema fileSchema,
    List<List<PageInfo>> pageInfosByColumn
) {}
