/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import dev.hardwood.row.PqDoubleList;
import dev.hardwood.row.PqIntList;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqLongList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RowReaderTest {
    @Test
    void toIterator() {
        try (final var reader = new InMemoryRowReader()) {
            final var iterator = reader.toIterator();
            assertTrue(iterator.hasNext());
            assertNext(iterator, "a", "1");
            assertNext(iterator, "b", "2");
            assertNext(iterator, "c", "3");
        }
    }

    private void assertNext(final Iterator<RowReader> iterator, final String c1, final String c2) {
        final var next = iterator.next();
        assertEquals(c1, next.getString(0));
        assertEquals(c2, next.getString(1));
    }

    private static class InMemoryRowReader implements RowReader {
        private final Iterator<String[]> backingData = List.of(
                new String[]{"a", "1"},
                new String[]{"b", "2"},
                new String[]{"c", "3"}
        ).iterator();
        private String[] current;

        @Override
        public boolean hasNext() {
            return backingData.hasNext();
        }

        @Override
        public void next() {
            current = backingData.next();
        }

        @Override
        public void close() {
            current = null;
        }

        @Override
        public int getInt(int fieldIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getLong(int fieldIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public float getFloat(int fieldIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public double getDouble(int fieldIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean getBoolean(int fieldIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getString(int fieldIndex) {
            return current[fieldIndex];
        }

        @Override
        public byte[] getBinary(int fieldIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public LocalDate getDate(int fieldIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public LocalTime getTime(int fieldIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Instant getTimestamp(int fieldIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BigDecimal getDecimal(int fieldIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public UUID getUuid(int fieldIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PqStruct getStruct(int fieldIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PqIntList getListOfInts(int fieldIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PqLongList getListOfLongs(int fieldIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PqDoubleList getListOfDoubles(int fieldIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PqList getList(int fieldIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PqMap getMap(int fieldIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object getValue(int fieldIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isNull(int fieldIndex) {
            return false;
        }

        @Override
        public int getInt(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getLong(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public float getFloat(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public double getDouble(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean getBoolean(String name) {
            return false;
        }

        @Override
        public String getString(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] getBinary(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public LocalDate getDate(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public LocalTime getTime(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Instant getTimestamp(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BigDecimal getDecimal(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public UUID getUuid(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PqStruct getStruct(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PqIntList getListOfInts(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PqLongList getListOfLongs(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PqDoubleList getListOfDoubles(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PqList getList(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PqMap getMap(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object getValue(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isNull(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getFieldCount() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getFieldName(int index) {
            throw new UnsupportedOperationException();
        }
    }
}
