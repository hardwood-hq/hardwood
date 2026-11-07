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
import dev.hardwood.internal.reader.engine.RowGroupIterator;
import dev.hardwood.internal.reader.fetch.CoalescingPolicy;
import dev.hardwood.internal.reader.fetch.FetchPlan;
import dev.hardwood.internal.reader.fetch.InPlaceSections;
import dev.hardwood.internal.reader.memory.ReadAheadBuffer;
import dev.hardwood.internal.reader.memory.WorkingSet;
import dev.hardwood.internal.reader.step.Need;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.internal.schema.ReadProjection;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;

/// Builds the reads of tests that drive a [RowGroupIterator] without a reader, and plans the work
/// items of a read outside one ([#planning]).
public final class RowGroupIterators {

    private RowGroupIterators() {
    }

    /// A read over `inputFiles`, not yet initialized. Its close closes the files' metadata and the
    /// files, as the parent reader of a read does.
    public static RowGroupIterator standalone(List<InputFile> inputFiles, HardwoodContextImpl context) {
        return standalone(new FileMetadataCache(inputFiles, context), context);
    }

    /// A read over the files of `metadata`, not yet initialized. Its close closes `metadata` and
    /// the files.
    public static RowGroupIterator standalone(FileMetadataCache metadata, HardwoodContextImpl context) {
        return new RowGroupIterator(metadata, context, iterator -> {
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
    public static RowGroupIterator initialized(List<InputFile> inputFiles, HardwoodContextImpl context,
                                               ColumnProjection projection, FilterPredicate filter, long maxRows)
            throws IOException {
        return initialized(inputFiles, context,
                schema -> ReadProjection.of(ProjectedSchema.create(schema, projection)), filter, maxRows);
    }

    /// As [#initialized(List, HardwoodContextImpl, ColumnProjection, FilterPredicate, long)], reading
    /// the projection `projection` makes of the first file's schema.
    public static RowGroupIterator initialized(List<InputFile> inputFiles, HardwoodContextImpl context,
                                               Function<FileSchema, ReadProjection> projection,
                                               FilterPredicate filter, long maxRows) throws IOException {
        FileMetadataCache metadata = new FileMetadataCache(inputFiles, context);
        RowGroupIterator iterator = standalone(metadata, context);
        try {
            FileMetadataCache.PreparedFile first = metadata.getFile(0);
            FileSchema schema = first.schema();
            iterator.initialize(schema, first.rowGroups(), projection.apply(schema),
                    filter == null ? null : FilterPredicateResolver.resolve(filter, schema), true, maxRows, 0);
            return iterator;
        }
        catch (IOException | RuntimeException | Error e) {
            iterator.close();
            throw e;
        }
    }

    /// The planning of a read of `projection` over the files of `metadata`, without a filter, row
    /// limit or batch plan, whose bytes are read in place on the calling thread
    /// ([InPlaceSections]); its first file is listed. The caller closes `metadata`.
    public static ReadPlanning planning(FileMetadataCache metadata, HardwoodContextImpl context,
                                        ColumnProjection projection) throws IOException {
        return planning(metadata, context, projection, 16L * 1024 * 1024);
    }

    /// As [#planning(FileMetadataCache, HardwoodContextImpl, ColumnProjection)], whose page-index
    /// windows read at most `indexWindowBytes` each, unless a single row group needs more.
    public static ReadPlanning planning(FileMetadataCache metadata, HardwoodContextImpl context,
                                        ColumnProjection projection, long indexWindowBytes) throws IOException {
        FileMetadataCache.PreparedFile first = metadata.getFile(0);
        FileSchema schema = first.schema();
        ReadProjection read = ReadProjection.of(ProjectedSchema.create(schema, projection));
        int columns = read.decoded().getProjectedColumnCount();
        ReadPlanning planning = new ReadPlanning(metadata, new InPlaceSections(), context, schema,
                first.rowGroups(), read, null, true, 0, 0, CoalescingPolicy.maxSpanBytes(), indexWindowBytes,
                new WorkingSet(columns, new ReadAheadBuffer(0)));
        planning.append(planning.listFile(0, 0));
        return planning;
    }

    /// The fetch plans of work item `index` of `planning`, planned on the calling thread.
    public static FetchPlan[] plans(ReadPlanning planning, int index) throws IOException {
        for (int attempt = 0; attempt < 100; attempt++) {
            Need need = planning.plan(index);
            if (need == null) {
                return planning.plans(index);
            }
        }
        throw new IllegalStateException("Work item " + index + " was not planned");
    }
}
