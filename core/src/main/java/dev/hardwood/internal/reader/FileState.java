/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.List;

import dev.hardwood.InputFile;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.schema.FileSchema;

/// Holds prepared state for a Parquet file ready for multi-file reading.
///
/// Contains pre-scanned pages organized by column index, allowing PageCursors
/// to extend with pages from the next file without re-scanning.
///
/// @param inputFile the input file
/// @param fileMetaData the parsed file metadata
/// @param fileSchema the parsed file schema
/// @param pageInfosByColumn pre-scanned pages for each column (by projected column index)
public record FileState(
    InputFile inputFile,
    FileMetaData fileMetaData,
    FileSchema fileSchema,
    List<List<PageInfo>> pageInfosByColumn
) {}
