/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.Function;

import dev.hardwood.InputFile;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.io.InputFileCloser;
import dev.hardwood.internal.predicate.FilterPredicateResolver;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.reader.engine.PreparedRead;
import dev.hardwood.internal.reader.engine.ReadEngine;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.internal.schema.ReadProjection;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;

/// Prepares the reads of tests that drive a [ReadEngine] without a reader.
public final class ReadEngines {

    private ReadEngines() {
    }

    /// A read over the files of `metadata`; see [PreparedRead#prepare]. The close of the engine it
    /// starts closes `metadata` and the files, as the parent reader of a read does, and so does a
    /// failure to prepare it.
    public static PreparedRead prepare(FileMetadataCache metadata, HardwoodContextImpl context, FileSchema schema,
                                       List<RowGroup> rowGroups, ReadProjection projection, ResolvedPredicate filter,
                                       boolean metadataFiltering, long limit, long skip) throws IOException {
        try {
            return PreparedRead.prepare(metadata, context, closing(metadata), schema, rowGroups, projection, filter,
                    metadataFiltering, limit, skip);
        }
        catch (IOException | RuntimeException | Error e) {
            close(metadata);
            throw e;
        }
    }

    /// A read over `inputFiles` of `projection`, filtered by `filter` unless it is `null`, with a
    /// row limit of `maxRows` unless it is `0`, prepared against the first file's schema and row
    /// groups. The close of the engine it starts closes the files' metadata and the files.
    public static PreparedRead prepared(List<InputFile> inputFiles, HardwoodContextImpl context,
                                        ColumnProjection projection, FilterPredicate filter, long maxRows)
            throws IOException {
        return prepared(inputFiles, context,
                schema -> ReadProjection.of(ProjectedSchema.create(schema, projection)), filter, maxRows);
    }

    /// As [#prepared(List, HardwoodContextImpl, ColumnProjection, FilterPredicate, long)], reading
    /// the projection `projection` makes of the first file's schema.
    public static PreparedRead prepared(List<InputFile> inputFiles, HardwoodContextImpl context,
                                        Function<FileSchema, ReadProjection> projection,
                                        FilterPredicate filter, long maxRows) throws IOException {
        FileMetadataCache metadata = new FileMetadataCache(inputFiles, context);
        try {
            FileMetadataCache.PreparedFile first = metadata.getFile(0);
            FileSchema schema = first.schema();
            return PreparedRead.prepare(metadata, context, closing(metadata), schema, first.rowGroups(),
                    projection.apply(schema), filter == null ? null : FilterPredicateResolver.resolve(filter, schema),
                    true, maxRows, 0);
        }
        catch (IOException | RuntimeException | Error e) {
            close(metadata);
            throw e;
        }
    }

    /// Whether a read of `file` under `projection` that skips its first row masks that row out of
    /// the first row group's plan, rather than returning it for the reader to drop
    /// ([PreparedRead#leadingRows]): the row group takes a page mask.
    public static boolean masksLeadingRows(InputFile file, ColumnProjection projection) throws IOException {
        try (HardwoodContextImpl context = HardwoodContextImpl.create(0)) {
            FileMetadataCache metadata = new FileMetadataCache(List.of(file), context);
            try {
                FileMetadataCache.PreparedFile first = metadata.getFile(0);
                FileSchema schema = first.schema();
                PreparedRead read = prepare(metadata, context, schema, first.rowGroups(),
                        ReadProjection.of(ProjectedSchema.create(schema, projection)), null, true, 0, 1);
                return read.leadingRows() == 0;
            }
            finally {
                close(metadata);
            }
        }
    }

    /// Tracks nothing, and closes `metadata` and its files as the engine closes.
    private static PreparedRead.Tracking closing(FileMetadataCache metadata) {
        return new PreparedRead.Tracking() {
            @Override
            public void started(PreparedRead read, ReadEngine engine) {
            }

            @Override
            public void closed(PreparedRead read) {
                close(metadata);
            }
        };
    }

    private static void close(FileMetadataCache metadata) {
        try {
            metadata.close();
            InputFileCloser.rethrow(InputFileCloser.closeAll(metadata.inputFiles(), null));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
