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
import java.util.function.Supplier;

import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.reader.batch.NestedLevelComputer;
import dev.hardwood.internal.reader.memory.WorkingSet;
import dev.hardwood.internal.reader.pipeline.PageTask;
import dev.hardwood.internal.reader.plan.BatchPlan;
import dev.hardwood.schema.ColumnSchema;

/// Gives the read's parts each decoded column, by index: its schema, its layers where it is decoded
/// through levels, and its handle on the read's working set. Charges each column's dictionaries to
/// the working set one at a time, and adds the column, file, row group and page to a failure met
/// reading it. Holds the read's working set; used on any thread.
final class ColumnContext {

    private final ColumnSchema[] schemas;
    /// per column, its layers where it is decoded through levels, `null` for a flat one
    private final List<NestedLevelComputer.Layers> layers;
    /// the read's working set, which every decode and dictionary charges before it allocates
    private final WorkingSet workingSet;
    private final boolean footersSupplied;
    /// per column, what its dictionaries' charges are made under, one at a time
    private final Object[] dictionaryCharges;

    ColumnContext(ColumnSchema[] schemas, List<NestedLevelComputer.Layers> layers, WorkingSet workingSet,
                  boolean footersSupplied) {
        this.schemas = schemas;
        this.layers = layers;
        this.workingSet = workingSet;
        this.footersSupplied = footersSupplied;
        this.dictionaryCharges = new Object[schemas.length];
        for (int c = 0; c < schemas.length; c++) {
            dictionaryCharges[c] = new Object();
        }
    }

    /// The number of decoded columns.
    int count() {
        return schemas.length;
    }

    ColumnSchema schema(int column) {
        return schemas[column];
    }

    /// Whether `column` is decoded through levels.
    boolean nested(int column) {
        return layers.get(column) != null;
    }

    /// The layers of `column`, `null` for a flat one.
    NestedLevelComputer.Layers layers(int column) {
        return layers.get(column);
    }

    /// The working set's handle for `column`.
    WorkingSet.Column workingSet(int column) {
        return workingSet.column(column, schemas[column]);
    }

    /// Runs `charge`, which charges a dictionary of `column`'s chunk of `u`, against the batches
    /// `u` places: one column's dictionaries one at a time, so that each counts beside
    /// those of its own batch alone, in whichever order they decode; on any thread.
    <T> T chargeDictionary(int column, Unit.Batched u, Supplier<T> charge) {
        BatchPlan.RowGroupBatches batches = u.batches();
        synchronized (dictionaryCharges[column]) {
            workingSet.enter(column, batches.readRowsBefore() - batches.continuedRows(),
                    batches.readRowsBefore() + batches.plannedRows());
            return charge.get();
        }
    }

    /// `e`, with the column and the file, row group and page of `page` as its context.
    Exception withContext(int column, Exception e, PageTask page) {
        return withContext(column, e, page.fileName(), page.rowGroup(), page.pageIndex());
    }

    /// `e` met reading `column`, typed as a read failure, with the file, row group and page it came
    /// from as its context. Where a [dev.hardwood.MetadataSource] supplied the footers, a read
    /// failure also says so ([ExceptionContext#addSuppliedFooterHint]): the checks a supplied footer
    /// passes on open do not catch every file rewritten since the footer was read.
    Exception withContext(int column, Exception e, String fileName, int rowGroup, int page) {
        return withContext(schemas[column].fieldPath().toString(), e, fileName, rowGroup, page);
    }

    /// `e` met planning a row group, which belongs to no column: with the file and row group alone
    /// as its context.
    Exception withContext(Exception e, String fileName, int rowGroup) {
        return withContext(null, e, fileName, rowGroup, ExceptionContext.UNKNOWN_PAGE);
    }

    private Exception withContext(String columnPath, Exception e, String fileName, int rowGroup, int page) {
        if (HardwoodContextImpl.isContextClosed(e)) {
            // the context's, not the file's, whichever thread of the read met the refusal first
            return e;
        }
        Exception typed = e instanceof RuntimeException runtime ? ExceptionContext.asReadFailure(runtime) : e;
        if (fileName == null || fileName.isEmpty()) {
            return typed;
        }
        if (typed instanceof RuntimeException re) {
            RuntimeException read = ExceptionContext.addReadContext(fileName, rowGroup, columnPath, page, re);
            return footersSupplied ? ExceptionContext.addSuppliedFooterHint(read) : read;
        }
        if (typed instanceof IOException ioe) {
            // stays checked: the read carries a failure across threads as a Throwable
            return ExceptionContext.addReadContext(fileName, rowGroup, columnPath, page, ioe);
        }
        return typed;
    }
}
