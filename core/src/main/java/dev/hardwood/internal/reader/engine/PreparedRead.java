/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.io.IOException;
import java.util.List;

import dev.hardwood.InputFile;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.predicate.ColumnBatchMatcher;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.reader.batch.BatchExchange;
import dev.hardwood.internal.reader.batch.NestedIndexMode;
import dev.hardwood.internal.reader.batch.NestedLevelComputer;
import dev.hardwood.internal.reader.footer.FileColumnOrdinals;
import dev.hardwood.internal.reader.plan.FileMetadataCache;
import dev.hardwood.internal.reader.plan.RowGroupPruning;
import dev.hardwood.internal.reader.plan.SchemaCompatibility;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.internal.schema.ReadProjection;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/// A read of a [dev.hardwood.reader.ParquetFileReader] before its columns exist: checks the first
/// file against the read's schema, prunes its row groups and finds where a skip lands, then starts
/// the read's [ReadEngine] once the reader has built its columns ([#start]). Runs no job and holds
/// nothing to close; only the engine it starts is tracked by the reader.
public final class PreparedRead {

    /// How the reader tracks the engine a prepared read starts, until the engine closes.
    public interface Tracking {

        /// `read` started `engine`, which the reader closes where the caller never does.
        void started(PreparedRead read, ReadEngine engine);

        /// The engine `read` started has closed.
        void closed(PreparedRead read);
    }

    private final FileMetadataCache files;
    private final HardwoodContextImpl context;
    private final Tracking tracking;
    private final FileSchema schema;
    /// the first file, as the reader holds it: with its tail
    private final InputFile file;
    private final ReadProjection projection;
    private final ResolvedPredicate filter;
    /// the read's row limit, `0` for none: under a filter it counts matching rows
    private final long limit;
    private final long skip;
    private final RowGroupPruning pruning;
    private final SchemaCompatibility compatibility;
    private final FileColumnOrdinals firstOrdinals;
    /// the first file's row groups its statistics keep
    private final RowGroupPruning.FilteredRowGroups firstKept;
    /// the rows of the row group the skip lands in that precede it, and whether that row group's
    /// plan masks them out; where it cannot, the reader drops them ([#leadingRows])
    private final long leading;
    private final boolean masked;
    private boolean started;

    private PreparedRead(FileMetadataCache files, HardwoodContextImpl context, Tracking tracking, FileSchema schema,
                         InputFile file, ReadProjection projection, ResolvedPredicate filter, long limit, long skip,
                         RowGroupPruning pruning, SchemaCompatibility compatibility, FileColumnOrdinals firstOrdinals,
                         RowGroupPruning.FilteredRowGroups firstKept, long leading, boolean masked) {
        this.files = files;
        this.context = context;
        this.tracking = tracking;
        this.schema = schema;
        this.file = file;
        this.projection = projection;
        this.filter = filter;
        this.limit = limit;
        this.skip = skip;
        this.pruning = pruning;
        this.compatibility = compatibility;
        this.firstOrdinals = firstOrdinals;
        this.firstKept = firstKept;
        this.leading = leading;
        this.masked = masked;
    }

    /// Prepares a read over the files of `files`.
    ///
    /// @param tracking how the reader tracks the engine the read starts
    /// @param rowGroups the first file's row groups, `null` for all of them
    /// @param filter the read's resolved filter, `null` for none
    /// @param metadataFiltering whether the filter takes metadata-derived shortcuts
    /// @param limit the read's row limit, `0` for none; under a filter it counts matching rows, and
    ///        bounds the listing only over row groups statistics prove to match
    /// @param skip the read's leading rows it does not return, `0` for none; a read with a filter
    ///        skips none, its reader skipping matching rows
    /// @throws IOException the failure of a footer of a file the skip reaches, read on the caller's
    ///         thread
    /// @throws IllegalStateException on a thread of `context`'s own pool, whose read would wait for
    ///         the jobs it holds up
    public static PreparedRead prepare(FileMetadataCache files, HardwoodContextImpl context, Tracking tracking,
                                       FileSchema schema, List<RowGroup> rowGroups, ReadProjection projection,
                                       ResolvedPredicate filter, boolean metadataFiltering, long limit, long skip)
            throws IOException {
        context.requireOffPool();
        List<RowGroup> first = rowGroups != null ? rowGroups : files.getFileMetaData(0).rowGroups();
        InputFile file = files.preparedInputFile(0);
        SchemaCompatibility.validateFirstFile(file, schema, projection.decoded(), first);
        RowGroupPruning pruning = new RowGroupPruning(context, filter, metadataFiltering);
        SchemaCompatibility compatibility = new SchemaCompatibility(schema, projection.decoded(), filter);
        FileColumnOrdinals firstOrdinals = FileColumnOrdinals.identity(schema.getColumnCount(), filter,
                files.getFile(0).boundsReadability());
        RowGroupPruning.FilteredRowGroups firstKept = pruning.filterRowGroups(first, file, firstOrdinals);
        // the row group the skip lands in, past the row groups it drops whole, and the rows of it
        // before the skip's end, reading the footers of the files it reaches on the caller's thread
        long left = skip;
        for (int i = 0; i < files.inputFiles().size() && left > 0; i++) {
            for (RowGroup rg : i == 0 ? first : files.getFileMetaData(i).rowGroups()) {
                if (left < rg.numRows()) {
                    boolean masked = i == 0 && takesMask(schema, projection.decoded(), rg);
                    return new PreparedRead(files, context, tracking, schema, file, projection, filter, limit, skip,
                            pruning, compatibility, firstOrdinals, firstKept, left, masked);
                }
                left -= rg.numRows();
            }
        }
        return new PreparedRead(files, context, tracking, schema, file, projection, filter, limit, skip, pruning,
                compatibility, firstOrdinals, firstKept, 0, false);
    }

    /// Whether the plan of `rg`, a row group of the first file, can mask out the rows before the
    /// skip's end: every nested decoded column has an OffsetIndex, which a page mask needs.
    private static boolean takesMask(FileSchema schema, ProjectedSchema decoded, RowGroup rg) {
        for (int c = 0; c < decoded.getProjectedColumnCount(); c++) {
            ColumnSchema column = schema.getColumn(decoded.toOriginalIndex(c));
            if (column.maxRepetitionLevel() > 0 && rg.columns().get(column.columnIndex()).offsetIndexOffset() == null) {
                return false;
            }
        }
        return true;
    }

    /// The leading rows the reader drops itself before it returns a row: those of the row group the
    /// skip lands in that precede it, where its plan cannot mask them out; `0` otherwise. The read
    /// returns them, and counts them against its limit.
    public long leadingRows() {
        return masked ? 0 : leading;
    }

    public String referenceFileName() {
        return files.inputFiles().getFirst().name();
    }

    /// Starts the read's decoded columns, publishing into detaching exchanges without filter
    /// fragments, as a column read without batch matchers does ([ReadEngine#exchanges]).
    ///
    /// @param layers the layers of each decoded column decoded through levels, `null` for a flat
    ///        one, in decoded order
    /// @param indexMode what the batches of a column decoded through levels carry for their reader
    /// @throws IllegalStateException if the read has started already
    public ReadEngine start(ProjectedSchema decoded, List<NestedLevelComputer.Layers> layers,
                            NestedIndexMode indexMode, boolean fixedListFastPathEnabled, int batchSize,
                            long budgetRows) {
        return start(decoded, layers, indexMode, fixedListFastPathEnabled, batchSize, budgetRows, null, null);
    }

    /// Starts the read's decoded columns, and the reader tracks its engine until it closes.
    ///
    /// @param layers the layers of each decoded column decoded through levels, `null` for a flat
    ///        one, in decoded order
    /// @param indexMode what the batches of a column decoded through levels carry for their reader
    /// @param budgetRows the rows of the read's batch byte budget, which bound its look-ahead
    /// @param exchanges the exchange each decoded column publishes into, in decoded order: a flat
    ///        column's of [BatchExchange.Batch], one decoded through levels of
    ///        [dev.hardwood.internal.reader.batch.NestedBatch]; `null` for detaching exchanges
    /// @param filters the filter fragment of each flat decoded column, `null` for a column without
    ///        one; `null` for none
    /// @throws IllegalStateException if the read has started already
    public ReadEngine start(ProjectedSchema decoded, List<NestedLevelComputer.Layers> layers,
                            NestedIndexMode indexMode, boolean fixedListFastPathEnabled, int batchSize,
                            long budgetRows, List<BatchExchange<?>> exchanges, ColumnBatchMatcher[] filters) {
        if (started) {
            throw new IllegalStateException("The read has started already");
        }
        started = true;
        ReadEngine engine = new ReadEngine(this, decoded, layers, indexMode, fixedListFastPathEnabled, batchSize,
                budgetRows, exchanges, filters, () -> tracking.closed(this));
        tracking.started(this, engine);
        return engine;
    }

    FileMetadataCache files() {
        return files;
    }

    HardwoodContextImpl context() {
        return context;
    }

    FileSchema schema() {
        return schema;
    }

    InputFile file() {
        return file;
    }

    ReadProjection projection() {
        return projection;
    }

    ResolvedPredicate filter() {
        return filter;
    }

    long limit() {
        return limit;
    }

    long skip() {
        return skip;
    }

    RowGroupPruning pruning() {
        return pruning;
    }

    SchemaCompatibility compatibility() {
        return compatibility;
    }

    FileColumnOrdinals firstOrdinals() {
        return firstOrdinals;
    }

    RowGroupPruning.FilteredRowGroups firstKept() {
        return firstKept;
    }

    /// The rows before the skip's end the first row group's plan masks out, `0` where it masks none.
    long maskedRows() {
        return masked ? leading : 0;
    }
}
