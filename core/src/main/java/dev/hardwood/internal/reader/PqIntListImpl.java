/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.function.IntConsumer;

import dev.hardwood.internal.reader.NestedColumnData.IntColumn;
import dev.hardwood.row.PqIntList;

/**
 * Flyweight {@link PqIntList} that reads int values directly from a column array.
 * Zero boxing for {@link #forEach(IntConsumer)}, {@link #toArray()}, and {@link #get(int)}.
 */
final class PqIntListImpl implements PqIntList {

    private final NestedBatchIndex batch;
    private final int projectedCol;
    private final int start;
    private final int end;

    PqIntListImpl(NestedBatchIndex batch, int projectedCol, int start, int end) {
        this.batch = batch;
        this.projectedCol = projectedCol;
        this.start = start;
        this.end = end;
    }

    @Override
    public int size() {
        return end - start;
    }

    @Override
    public boolean isEmpty() {
        return start >= end;
    }

    @Override
    public int get(int index) {
        checkBounds(index);
        int valueIdx = start + index;
        if (batch.isElementNull(projectedCol, valueIdx)) {
            throw new NullPointerException("Element at index " + index + " is null");
        }
        return ((NestedColumnData.IntColumn) batch.columns[projectedCol]).get(valueIdx);
    }

    @Override
    public boolean isNull(int index) {
        checkBounds(index);
        return batch.isElementNull(projectedCol, start + index);
    }

    @Override
    public PrimitiveIterator.OfInt iterator() {
        return new PrimitiveIterator.OfInt() {
            private int pos = start;

            @Override
            public boolean hasNext() {
                return pos < end;
            }

            @Override
            public int nextInt() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                if (batch.isElementNull(projectedCol, pos)) {
                    throw new NullPointerException("Element is null");
                }
                return ((NestedColumnData.IntColumn) batch.columns[projectedCol]).get(pos++);
            }
        };
    }

    @Override
    public void forEach(IntConsumer action) {
        NestedColumnData.IntColumn col = (NestedColumnData.IntColumn) batch.columns[projectedCol];
        for (int i = start; i < end; i++) {
            if (batch.isElementNull(projectedCol, i)) {
                throw new NullPointerException("Element at index " + (i - start) + " is null");
            }
            action.accept(col.get(i));
        }
    }

    @Override
    public int[] toArray() {
        int size = size();
        int[] result = new int[size];
        NestedColumnData.IntColumn col = (NestedColumnData.IntColumn) batch.columns[projectedCol];
        for (int i = 0; i < size; i++) {
            if (batch.isElementNull(projectedCol, start + i)) {
                throw new NullPointerException("Element at index " + i + " is null");
            }
            result[i] = col.get(start + i);
        }
        return result;
    }

    private void checkBounds(int index) {
        if (index < 0 || index >= size()) {
            throw new IndexOutOfBoundsException("Index " + index + " out of range [0, " + size() + ")");
        }
    }
}
