/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;

import dev.hardwood.internal.compression.DecompressorFactory;
import dev.hardwood.internal.reader.FlatColumnData;
import dev.hardwood.internal.reader.Page;
import dev.hardwood.internal.reader.PageInfo;
import dev.hardwood.internal.reader.PageReader;
import dev.hardwood.internal.reader.TypedColumnData;
import dev.hardwood.schema.ColumnSchema;

/**
 * Minimal synchronous column assembler for benchmarking.
 * Decodes pages and returns TypedColumnData batches with array copying
 * to ensure consistent batch sizes across columns.
 */
public class SyncColumnAssembler {

    private final List<PageInfo> pages;
    private final ColumnSchema column;
    private final PageReader pageReader;

    private int pageIndex;
    private Page currentPage;
    private int position;

    public SyncColumnAssembler(List<PageInfo> pages, ColumnSchema column, DecompressorFactory decompressorFactory) {
        this.pages = pages;
        this.column = column;
        if (!pages.isEmpty()) {
            PageInfo first = pages.get(0);
            this.pageReader = new PageReader(first.columnMetaData(), first.columnSchema(), decompressorFactory);
        }
        else {
            this.pageReader = null;
        }
    }

    public boolean hasMore() {
        return ensurePageLoaded();
    }

    public TypedColumnData nextBatch(int maxRecords) {
        if (!ensurePageLoaded()) {
            return emptyBatch();
        }

        return switch (currentPage) {
            case Page.IntPage p -> assembleInt(maxRecords);
            case Page.LongPage p -> assembleLong(maxRecords);
            case Page.FloatPage p -> assembleFloat(maxRecords);
            case Page.DoublePage p -> assembleDouble(maxRecords);
            case Page.BooleanPage p -> assembleBoolean(maxRecords);
            case Page.ByteArrayPage p -> assembleByteArray(maxRecords);
        };
    }

    private boolean ensurePageLoaded() {
        if (currentPage != null && position < currentPage.size()) {
            return true;
        }
        if (pageIndex >= pages.size()) {
            return false;
        }
        try {
            PageInfo pageInfo = pages.get(pageIndex++);
            currentPage = pageReader.decodePage(pageInfo.pageData(), pageInfo.dictionary());
            position = 0;
            return currentPage != null;
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to decode page", e);
        }
    }

    private void markNulls(BitSet nulls, int[] defLevels, int srcPos, int destPos, int count, int maxDefLevel) {
        if (nulls == null) {
            return;
        }
        for (int i = 0; i < count; i++) {
            if (defLevels[srcPos + i] < maxDefLevel) {
                nulls.set(destPos + i);
            }
        }
    }

    private TypedColumnData emptyBatch() {
        return switch (column.type()) {
            case INT32 -> new FlatColumnData.IntColumn(column, new int[0], null, 0);
            case INT64 -> new FlatColumnData.LongColumn(column, new long[0], null, 0);
            case FLOAT -> new FlatColumnData.FloatColumn(column, new float[0], null, 0);
            case DOUBLE -> new FlatColumnData.DoubleColumn(column, new double[0], null, 0);
            case BOOLEAN -> new FlatColumnData.BooleanColumn(column, new boolean[0], null, 0);
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 ->
                    new FlatColumnData.ByteArrayColumn(column, new byte[0][], null, 0);
        };
    }

    private TypedColumnData assembleInt(int maxRecords) {
        int maxDef = column.maxDefinitionLevel();
        int[] values = new int[maxRecords];
        BitSet nulls = maxDef > 0 ? new BitSet(maxRecords) : null;
        int count = 0;

        while (count < maxRecords && ensurePageLoaded()) {
            Page.IntPage page = (Page.IntPage) currentPage;
            int available = page.size() - position;
            int toRead = Math.min(available, maxRecords - count);

            System.arraycopy(page.values(), position, values, count, toRead);
            markNulls(nulls, page.definitionLevels(), position, count, toRead, maxDef);

            position += toRead;
            count += toRead;
        }

        if (count < maxRecords) {
            values = java.util.Arrays.copyOf(values, count);
        }

        return new FlatColumnData.IntColumn(column, values, nulls, count);
    }

    private TypedColumnData assembleLong(int maxRecords) {
        int maxDef = column.maxDefinitionLevel();
        long[] values = new long[maxRecords];
        BitSet nulls = maxDef > 0 ? new BitSet(maxRecords) : null;
        int count = 0;

        while (count < maxRecords && ensurePageLoaded()) {
            Page.LongPage page = (Page.LongPage) currentPage;
            int available = page.size() - position;
            int toRead = Math.min(available, maxRecords - count);

            System.arraycopy(page.values(), position, values, count, toRead);
            markNulls(nulls, page.definitionLevels(), position, count, toRead, maxDef);

            position += toRead;
            count += toRead;
        }

        if (count < maxRecords) {
            values = java.util.Arrays.copyOf(values, count);
        }

        return new FlatColumnData.LongColumn(column, values, nulls, count);
    }

    private TypedColumnData assembleFloat(int maxRecords) {
        int maxDef = column.maxDefinitionLevel();
        float[] values = new float[maxRecords];
        BitSet nulls = maxDef > 0 ? new BitSet(maxRecords) : null;
        int count = 0;

        while (count < maxRecords && ensurePageLoaded()) {
            Page.FloatPage page = (Page.FloatPage) currentPage;
            int available = page.size() - position;
            int toRead = Math.min(available, maxRecords - count);

            System.arraycopy(page.values(), position, values, count, toRead);
            markNulls(nulls, page.definitionLevels(), position, count, toRead, maxDef);

            position += toRead;
            count += toRead;
        }

        if (count < maxRecords) {
            values = java.util.Arrays.copyOf(values, count);
        }

        return new FlatColumnData.FloatColumn(column, values, nulls, count);
    }

    private TypedColumnData assembleDouble(int maxRecords) {
        int maxDef = column.maxDefinitionLevel();
        double[] values = new double[maxRecords];
        BitSet nulls = maxDef > 0 ? new BitSet(maxRecords) : null;
        int count = 0;

        while (count < maxRecords && ensurePageLoaded()) {
            Page.DoublePage page = (Page.DoublePage) currentPage;
            int available = page.size() - position;
            int toRead = Math.min(available, maxRecords - count);

            System.arraycopy(page.values(), position, values, count, toRead);
            markNulls(nulls, page.definitionLevels(), position, count, toRead, maxDef);

            position += toRead;
            count += toRead;
        }

        if (count < maxRecords) {
            values = java.util.Arrays.copyOf(values, count);
        }

        return new FlatColumnData.DoubleColumn(column, values, nulls, count);
    }

    private TypedColumnData assembleBoolean(int maxRecords) {
        int maxDef = column.maxDefinitionLevel();
        boolean[] values = new boolean[maxRecords];
        BitSet nulls = maxDef > 0 ? new BitSet(maxRecords) : null;
        int count = 0;

        while (count < maxRecords && ensurePageLoaded()) {
            Page.BooleanPage page = (Page.BooleanPage) currentPage;
            int available = page.size() - position;
            int toRead = Math.min(available, maxRecords - count);

            System.arraycopy(page.values(), position, values, count, toRead);
            markNulls(nulls, page.definitionLevels(), position, count, toRead, maxDef);

            position += toRead;
            count += toRead;
        }

        if (count < maxRecords) {
            values = java.util.Arrays.copyOf(values, count);
        }

        return new FlatColumnData.BooleanColumn(column, values, nulls, count);
    }

    private TypedColumnData assembleByteArray(int maxRecords) {
        int maxDef = column.maxDefinitionLevel();
        byte[][] values = new byte[maxRecords][];
        BitSet nulls = maxDef > 0 ? new BitSet(maxRecords) : null;
        int count = 0;

        while (count < maxRecords && ensurePageLoaded()) {
            Page.ByteArrayPage page = (Page.ByteArrayPage) currentPage;
            int available = page.size() - position;
            int toRead = Math.min(available, maxRecords - count);

            System.arraycopy(page.values(), position, values, count, toRead);
            markNulls(nulls, page.definitionLevels(), position, count, toRead, maxDef);

            position += toRead;
            count += toRead;
        }

        if (count < maxRecords) {
            values = java.util.Arrays.copyOf(values, count);
        }

        return new FlatColumnData.ByteArrayColumn(column, values, nulls, count);
    }
}
