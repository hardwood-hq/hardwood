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
import dev.hardwood.internal.reader.engine.ReadEngine;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.internal.schema.ReadProjection;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;

/// Builds the reads of tests that drive a [ReadEngine] without a reader.
public final class ReadEngines {

    private ReadEngines() {
    }

    /// A read over `inputFiles`, not yet initialized. Its close closes the files' metadata and the
    /// files, as the parent reader of a read does.
    public static ReadEngine standalone(List<InputFile> inputFiles, HardwoodContextImpl context) {
        return standalone(new FileMetadataCache(inputFiles, context), context);
    }

    /// A read over the files of `metadata`, not yet initialized. Its close closes `metadata` and
    /// the files.
    public static ReadEngine standalone(FileMetadataCache metadata, HardwoodContextImpl context) {
        return new ReadEngine(metadata, context, engine -> {
            try {
                metadata.close();
                InputFileCloser.rethrow(InputFileCloser.closeAll(metadata.inputFiles(), null));
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    /// A read over `inputFiles` of `projection`, filtered by `filter` unless it is `null`, with a
    /// row limit of `maxRows` unless it is `0`, initialized against the first file's schema and
    /// row groups. Its close closes the files' metadata and the files.
    public static ReadEngine initialized(List<InputFile> inputFiles, HardwoodContextImpl context,
                                               ColumnProjection projection, FilterPredicate filter, long maxRows)
            throws IOException {
        return initialized(inputFiles, context,
                schema -> ReadProjection.of(ProjectedSchema.create(schema, projection)), filter, maxRows);
    }

    /// As [#initialized(List, HardwoodContextImpl, ColumnProjection, FilterPredicate, long)], reading
    /// the projection `projection` makes of the first file's schema.
    public static ReadEngine initialized(List<InputFile> inputFiles, HardwoodContextImpl context,
                                               Function<FileSchema, ReadProjection> projection,
                                               FilterPredicate filter, long maxRows) throws IOException {
        FileMetadataCache metadata = new FileMetadataCache(inputFiles, context);
        ReadEngine engine = standalone(metadata, context);
        try {
            FileMetadataCache.PreparedFile first = metadata.getFile(0);
            FileSchema schema = first.schema();
            engine.initialize(schema, first.rowGroups(), projection.apply(schema),
                    filter == null ? null : FilterPredicateResolver.resolve(filter, schema), true, maxRows, 0);
            return engine;
        }
        catch (IOException | RuntimeException | Error e) {
            engine.close();
            throw e;
        }
    }

    /// Whether a read of `file` under `projection` that skips its first row masks that row out of
    /// the first row group's plan, rather than returning it for the reader to drop
    /// ([ReadEngine#leadingRows]): the row group takes a page mask.
    public static boolean masksLeadingRows(InputFile file, ColumnProjection projection) throws IOException {
        try (HardwoodContextImpl context = HardwoodContextImpl.create(0)) {
            FileMetadataCache metadata = new FileMetadataCache(List.of(file), context);
            try (ReadEngine engine = standalone(metadata, context)) {
                FileMetadataCache.PreparedFile first = metadata.getFile(0);
                FileSchema schema = first.schema();
                engine.initialize(schema, first.rowGroups(), ReadProjection.of(ProjectedSchema.create(schema, projection)),
                        null, true, 0, 1);
                return engine.leadingRows() == 0;
            }
        }
    }
}
