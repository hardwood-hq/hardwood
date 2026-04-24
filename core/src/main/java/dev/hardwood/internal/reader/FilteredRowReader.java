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
import java.util.UUID;

import dev.hardwood.internal.predicate.RecordFilterEvaluator;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqDoubleList;
import dev.hardwood.row.PqIntList;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqLongList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.row.PqVariant;
import dev.hardwood.schema.FileSchema;

/// Filtered wrapper around any [RowReader] that skips non-matching rows.
///
/// Advances the delegate until a matching row is found, then exposes that row
/// to the consumer. Works with both flat and nested readers.
public final class FilteredRowReader implements RowReader {

    private final RowReader delegate;
    private final ResolvedPredicate predicate;
    private final FileSchema schema;

    private boolean hasMatch;

    FilteredRowReader(RowReader delegate, ResolvedPredicate predicate, FileSchema schema) {
        this.delegate = delegate;
        this.predicate = predicate;
        this.schema = schema;
    }

    @Override
    public boolean hasNext() {
        while (delegate.hasNext()) {
            delegate.next();
            if (RecordFilterEvaluator.matchesRow(predicate, delegate, schema)) {
                hasMatch = true;
                return true;
            }
        }
        return false;
    }

    @Override
    public void next() {
        if (!hasMatch) {
            throw new java.util.NoSuchElementException("No matching row available. Call hasNext() first.");
        }
        hasMatch = false;
    }

    // ==================== Delegation ====================

    @Override public int getInt(int i) { return delegate.getInt(i); }
    @Override public int getInt(String name) { return delegate.getInt(name); }
    @Override public long getLong(int i) { return delegate.getLong(i); }
    @Override public long getLong(String name) { return delegate.getLong(name); }
    @Override public float getFloat(int i) { return delegate.getFloat(i); }
    @Override public float getFloat(String name) { return delegate.getFloat(name); }
    @Override public double getDouble(int i) { return delegate.getDouble(i); }
    @Override public double getDouble(String name) { return delegate.getDouble(name); }
    @Override public boolean getBoolean(int i) { return delegate.getBoolean(i); }
    @Override public boolean getBoolean(String name) { return delegate.getBoolean(name); }
    @Override public String getString(int i) { return delegate.getString(i); }
    @Override public String getString(String name) { return delegate.getString(name); }
    @Override public byte[] getBinary(int i) { return delegate.getBinary(i); }
    @Override public byte[] getBinary(String name) { return delegate.getBinary(name); }
    @Override public LocalDate getDate(int i) { return delegate.getDate(i); }
    @Override public LocalDate getDate(String name) { return delegate.getDate(name); }
    @Override public LocalTime getTime(int i) { return delegate.getTime(i); }
    @Override public LocalTime getTime(String name) { return delegate.getTime(name); }
    @Override public Instant getTimestamp(int i) { return delegate.getTimestamp(i); }
    @Override public Instant getTimestamp(String name) { return delegate.getTimestamp(name); }
    @Override public BigDecimal getDecimal(int i) { return delegate.getDecimal(i); }
    @Override public BigDecimal getDecimal(String name) { return delegate.getDecimal(name); }
    @Override public UUID getUuid(int i) { return delegate.getUuid(i); }
    @Override public UUID getUuid(String name) { return delegate.getUuid(name); }
    @Override public boolean isNull(int i) { return delegate.isNull(i); }
    @Override public boolean isNull(String name) { return delegate.isNull(name); }
    @Override public Object getValue(int i) { return delegate.getValue(i); }
    @Override public Object getValue(String name) { return delegate.getValue(name); }
    @Override public int getFieldCount() { return delegate.getFieldCount(); }
    @Override public String getFieldName(int i) { return delegate.getFieldName(i); }
    @Override public PqStruct getStruct(String name) { return delegate.getStruct(name); }
    @Override public PqStruct getStruct(int i) { return delegate.getStruct(i); }
    @Override public PqIntList getListOfInts(String name) { return delegate.getListOfInts(name); }
    @Override public PqIntList getListOfInts(int i) { return delegate.getListOfInts(i); }
    @Override public PqLongList getListOfLongs(String name) { return delegate.getListOfLongs(name); }
    @Override public PqLongList getListOfLongs(int i) { return delegate.getListOfLongs(i); }
    @Override public PqDoubleList getListOfDoubles(String name) { return delegate.getListOfDoubles(name); }
    @Override public PqDoubleList getListOfDoubles(int i) { return delegate.getListOfDoubles(i); }
    @Override public PqList getList(String name) { return delegate.getList(name); }
    @Override public PqList getList(int i) { return delegate.getList(i); }
    @Override public PqMap getMap(String name) { return delegate.getMap(name); }
    @Override public PqMap getMap(int i) { return delegate.getMap(i); }
    @Override public PqVariant getVariant(String name) { return delegate.getVariant(name); }

    @Override
    public void close() {
        delegate.close();
    }
}
