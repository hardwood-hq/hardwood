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
import java.util.function.DoubleConsumer;

import dev.hardwood.row.PqDoubleList;

/// Flyweight [PqDoubleList] that reads double values directly from a column array.
final class PqDoubleListImpl implements PqDoubleList {

    private final NestedBatchIndex batch;
    private final int projectedCol;
    private final int start;
    private final int end;

    PqDoubleListImpl(NestedBatchIndex batch, int projectedCol, int start, int end) {
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
    public double get(int index) {
        checkBounds(index);
        int valueIdx = start + index;
        if (batch.isElementNull(projectedCol, valueIdx)) {
            throw new NullPointerException("Element at index " + index + " is null");
        }
        return ((double[]) batch.valueArrays[projectedCol])[valueIdx];
    }

    @Override
    public boolean isNull(int index) {
        checkBounds(index);
        return batch.isElementNull(projectedCol, start + index);
    }

    @Override
    public PrimitiveIterator.OfDouble iterator() {
        return new PrimitiveIterator.OfDouble() {
            private int pos = start;

            @Override
            public boolean hasNext() {
                return pos < end;
            }

            @Override
            public double nextDouble() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                if (batch.isElementNull(projectedCol, pos)) {
                    throw new NullPointerException("Element is null");
                }
                return ((double[]) batch.valueArrays[projectedCol])[pos++];
            }
        };
    }

    @Override
    public void forEach(DoubleConsumer action) {
        double[] values = (double[]) batch.valueArrays[projectedCol];
        for (int i = start; i < end; i++) {
            if (batch.isElementNull(projectedCol, i)) {
                throw new NullPointerException("Element at index " + (i - start) + " is null");
            }
            action.accept(values[i]);
        }
    }

    @Override
    public double[] toArray() {
        int size = size();
        double[] result = new double[size];
        double[] values = (double[]) batch.valueArrays[projectedCol];
        for (int i = 0; i < size; i++) {
            if (batch.isElementNull(projectedCol, start + i)) {
                throw new NullPointerException("Element at index " + i + " is null");
            }
            result[i] = values[start + i];
        }
        return result;
    }

    private void checkBounds(int index) {
        if (index < 0 || index >= size()) {
            throw new IndexOutOfBoundsException("Index " + index + " out of range [0, " + size() + ")");
        }
    }

    @Override
    public String toString() {
        double[] values = (double[]) batch.valueArrays[projectedCol];
        StringBuilder sb = new StringBuilder("[");
        for (int i = start; i < end; i++) {
            if (i > start) sb.append(", ");
            if (batch.isElementNull(projectedCol, i)) {
                sb.append("null");
            } else {
                sb.append(values[i]);
            }
        }
        sb.append(']');
        return sb.toString();
    }
}
