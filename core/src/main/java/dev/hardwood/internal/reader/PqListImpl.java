/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.function.Function;

import dev.hardwood.row.PqDoubleList;
import dev.hardwood.row.PqIntList;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqLongList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.schema.SchemaNode;

/**
 * Implementation of PqList interface.
 */
public class PqListImpl implements PqList {

    private final MutableList elements;
    private final SchemaNode elementSchema;

    public PqListImpl(MutableList elements, SchemaNode elementSchema) {
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
    public Object get(int index) {
        return ValueConverter.convertValue(elements.elements().get(index), elementSchema);
    }

    @Override
    public boolean isNull(int index) {
        return elements.elements().get(index) == null;
    }

    @Override
    public Iterable<Object> values() {
        return () -> new ConvertingIterator<>(elements.elements(),
            raw -> ValueConverter.convertValue(raw, elementSchema));
    }

    // ==================== Primitive Type Accessors ====================

    @Override
    public Iterable<Integer> ints() {
        return () -> new ConvertingIterator<>(elements.elements(),
            raw -> ValueConverter.convertToInt(raw, elementSchema));
    }

    @Override
    public Iterable<Long> longs() {
        return () -> new ConvertingIterator<>(elements.elements(),
            raw -> ValueConverter.convertToLong(raw, elementSchema));
    }

    @Override
    public Iterable<Float> floats() {
        return () -> new ConvertingIterator<>(elements.elements(),
            raw -> ValueConverter.convertToFloat(raw, elementSchema));
    }

    @Override
    public Iterable<Double> doubles() {
        return () -> new ConvertingIterator<>(elements.elements(),
            raw -> ValueConverter.convertToDouble(raw, elementSchema));
    }

    @Override
    public Iterable<Boolean> booleans() {
        return () -> new ConvertingIterator<>(elements.elements(),
            raw -> ValueConverter.convertToBoolean(raw, elementSchema));
    }

    // ==================== Object Type Accessors ====================

    @Override
    public Iterable<String> strings() {
        return () -> new ConvertingIterator<>(elements.elements(),
            raw -> ValueConverter.convertToString(raw, elementSchema));
    }

    @Override
    public Iterable<byte[]> binaries() {
        return () -> new ConvertingIterator<>(elements.elements(),
            raw -> ValueConverter.convertToBinary(raw, elementSchema));
    }

    @Override
    public Iterable<LocalDate> dates() {
        return () -> new ConvertingIterator<>(elements.elements(),
            raw -> ValueConverter.convertToDate(raw, elementSchema));
    }

    @Override
    public Iterable<LocalTime> times() {
        return () -> new ConvertingIterator<>(elements.elements(),
            raw -> ValueConverter.convertToTime(raw, elementSchema));
    }

    @Override
    public Iterable<Instant> timestamps() {
        return () -> new ConvertingIterator<>(elements.elements(),
            raw -> ValueConverter.convertToTimestamp(raw, elementSchema));
    }

    @Override
    public Iterable<BigDecimal> decimals() {
        return () -> new ConvertingIterator<>(elements.elements(),
            raw -> ValueConverter.convertToDecimal(raw, elementSchema));
    }

    @Override
    public Iterable<UUID> uuids() {
        return () -> new ConvertingIterator<>(elements.elements(),
            raw -> ValueConverter.convertToUuid(raw, elementSchema));
    }

    // ==================== Nested Type Accessors ====================

    @Override
    public Iterable<PqStruct> structs() {
        return () -> new ConvertingIterator<>(elements.elements(),
            raw -> ValueConverter.convertToStruct(raw, elementSchema));
    }

    @Override
    public Iterable<PqList> lists() {
        return () -> new ConvertingIterator<>(elements.elements(),
            raw -> ValueConverter.convertToList(raw, elementSchema));
    }

    @Override
    public Iterable<PqIntList> intLists() {
        return () -> new ConvertingIterator<>(elements.elements(),
            raw -> ValueConverter.convertToIntList(raw, elementSchema));
    }

    @Override
    public Iterable<PqLongList> longLists() {
        return () -> new ConvertingIterator<>(elements.elements(),
            raw -> ValueConverter.convertToLongList(raw, elementSchema));
    }

    @Override
    public Iterable<PqDoubleList> doubleLists() {
        return () -> new ConvertingIterator<>(elements.elements(),
            raw -> ValueConverter.convertToDoubleList(raw, elementSchema));
    }

    @Override
    public Iterable<PqMap> maps() {
        return () -> new ConvertingIterator<>(elements.elements(),
            raw -> ValueConverter.convertToMap(raw, elementSchema));
    }

    /**
     * Iterator that converts raw values using a provided function.
     */
    private static class ConvertingIterator<T> implements Iterator<T> {
        private final List<Object> list;
        private final Function<Object, T> converter;
        private int pos = 0;

        ConvertingIterator(List<Object> list, Function<Object, T> converter) {
            this.list = list;
            this.converter = converter;
        }

        @Override
        public boolean hasNext() {
            return pos < list.size();
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Object raw = list.get(pos++);
            if (raw == null) {
                return null;
            }
            return converter.apply(raw);
        }
    }
}
