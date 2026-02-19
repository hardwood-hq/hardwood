/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.function.LongConsumer;

import dev.hardwood.row.PqLongList;
import dev.hardwood.schema.SchemaNode;

/**
 * Implementation of PqLongList interface.
 */
public class PqLongListImpl implements PqLongList {

    private final MutableList elements;
    private final SchemaNode elementSchema;

    public PqLongListImpl(MutableList elements, SchemaNode elementSchema) {
        this.elements = elements;
        this.elementSchema = elementSchema;
    }

    @Override
    public int size() {
        return elements.size();
    }

    @Override
    public boolean isEmpty() {
        return elements.size() == 0;
    }

    @Override
    public long get(int index) {
        Long val = ValueConverter.convertToLong(elements.elements().get(index), elementSchema);
        if (val == null) {
            throw new NullPointerException("Element at index " + index + " is null");
        }
        return val;
    }

    @Override
    public boolean isNull(int index) {
        return elements.elements().get(index) == null;
    }

    @Override
    public PrimitiveIterator.OfLong iterator() {
        return new PrimitiveIterator.OfLong() {
            private final List<Object> list = elements.elements();
            private int pos = 0;

            @Override
            public boolean hasNext() {
                return pos < list.size();
            }

            @Override
            public long nextLong() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                Object val = list.get(pos++);
                if (val == null) {
                    throw new NullPointerException("Element is null");
                }
                return (Long) val;
            }
        };
    }

    @Override
    public void forEach(LongConsumer action) {
        List<Object> list = elements.elements();
        for (int i = 0; i < list.size(); i++) {
            Object val = list.get(i);
            if (val == null) {
                throw new NullPointerException("Element at index " + i + " is null");
            }
            action.accept((Long) val);
        }
    }

    @Override
    public long[] toArray() {
        List<Object> list = elements.elements();
        long[] result = new long[list.size()];
        for (int i = 0; i < list.size(); i++) {
            Object val = list.get(i);
            if (val == null) {
                throw new NullPointerException("Element at index " + i + " is null");
            }
            result[i] = (Long) val;
        }
        return result;
    }
}
