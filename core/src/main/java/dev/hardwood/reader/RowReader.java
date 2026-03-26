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
import java.util.UUID;

import dev.hardwood.row.PqDoubleList;
import dev.hardwood.row.PqIntList;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqLongList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.row.StructAccessor;

/// Provides row-oriented iteration over a Parquet file.
///
/// A `RowReader` is a stateful, mutable view providing access to the current row
/// in the iterator. The values returned by its accessors change between calls of [#next()].
///
/// Usage example:
/// ```java
/// try (RowReader rowReader = fileReader.createRowReader()) {
///     while (rowReader.hasNext()) {
///         rowReader.next();
///         long id = rowReader.getLong("id");
///         PqStruct address = rowReader.getStruct("address");
///         String city = address.getString("city");
///     }
/// }
/// ```
public interface RowReader extends StructAccessor, AutoCloseable {

    /// Check if there are more rows to read.
    ///
    /// @return true if there are more rows available
    boolean hasNext();

    /// Advance to the next row. Must be called before accessing row data.
    ///
    /// @throws java.util.NoSuchElementException if no more rows are available
    void next();

    @Override
    void close();

    // ==================== Accessors by Index ====================
    // Faster than name-based access as they avoid the name lookup.

    /// Get an INT32 field value by field index.
    ///
    /// @throws NullPointerException if the field is null
    int getInt(int fieldIndex);

    /// Get an INT64 field value by field index.
    ///
    /// @throws NullPointerException if the field is null
    long getLong(int fieldIndex);

    /// Get a FLOAT field value by field index.
    ///
    /// @throws NullPointerException if the field is null
    float getFloat(int fieldIndex);

    /// Get a DOUBLE field value by field index.
    ///
    /// @throws NullPointerException if the field is null
    double getDouble(int fieldIndex);

    /// Get a BOOLEAN field value by field index.
    ///
    /// @throws NullPointerException if the field is null
    boolean getBoolean(int fieldIndex);

    /// Get a STRING field value by field index.
    ///
    /// @return the string value, or null if the field is null
    String getString(int fieldIndex);

    /// Get a BINARY field value by field index.
    ///
    /// @return the binary value, or null if the field is null
    byte[] getBinary(int fieldIndex);

    /// Get a DATE field value by field index.
    ///
    /// @return the date value, or null if the field is null
    LocalDate getDate(int fieldIndex);

    /// Get a TIME field value by field index.
    ///
    /// @return the time value, or null if the field is null
    LocalTime getTime(int fieldIndex);

    /// Get a TIMESTAMP field value by field index.
    ///
    /// @return the timestamp value, or null if the field is null
    Instant getTimestamp(int fieldIndex);

    /// Get a DECIMAL field value by field index.
    ///
    /// @return the decimal value, or null if the field is null
    BigDecimal getDecimal(int fieldIndex);

    /// Get a UUID field value by field index.
    ///
    /// @return the UUID value, or null if the field is null
    UUID getUuid(int fieldIndex);

    /// Get a nested struct field value by field index.
    ///
    /// @return the struct value, or null if the field is null
    PqStruct getStruct(int fieldIndex);

    /// Get an INT32 list field by field index.
    ///
    /// @return the list, or null if the field is null
    PqIntList getListOfInts(int fieldIndex);

    /// Get an INT64 list field by field index.
    ///
    /// @return the list, or null if the field is null
    PqLongList getListOfLongs(int fieldIndex);

    /// Get a DOUBLE list field by field index.
    ///
    /// @return the list, or null if the field is null
    PqDoubleList getListOfDoubles(int fieldIndex);

    /// Get a LIST field value by field index.
    ///
    /// @return the list, or null if the field is null
    PqList getList(int fieldIndex);

    /// Get a MAP field value by field index.
    ///
    /// @return the map, or null if the field is null
    PqMap getMap(int fieldIndex);

    /// Get a field value by field index without type conversion.
    ///
    /// @return the raw value, or null if the field is null
    Object getValue(int fieldIndex);

    /// Check if a field is null by field index.
    boolean isNull(int fieldIndex);
}
