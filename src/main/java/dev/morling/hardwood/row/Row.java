/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.row;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.UUID;

/**
 * Represents a single row in a Parquet file with typed accessor methods.
 * Supports nested structures (structs and lists).
 */
public interface Row {

    // Boolean accessors
    boolean getBoolean(int position);

    boolean getBoolean(String name);

    // INT32 accessors
    int getInt(int position);

    int getInt(String name);

    // INT64 accessors
    long getLong(int position);

    long getLong(String name);

    // FLOAT accessors
    float getFloat(int position);

    float getFloat(String name);

    // DOUBLE accessors
    double getDouble(int position);

    double getDouble(String name);

    // BYTE_ARRAY accessors
    byte[] getByteArray(int position);

    byte[] getByteArray(String name);

    // String convenience (BYTE_ARRAY as UTF-8)
    String getString(int position);

    String getString(String name);

    // Logical type accessors - DATE
    LocalDate getDate(int position);

    LocalDate getDate(String name);

    // Logical type accessors - TIME
    LocalTime getTime(int position);

    LocalTime getTime(String name);

    // Logical type accessors - TIMESTAMP
    Instant getTimestamp(int position);

    Instant getTimestamp(String name);

    // Logical type accessors - DECIMAL
    BigDecimal getDecimal(int position);

    BigDecimal getDecimal(String name);

    // Logical type accessors - UUID
    UUID getUuid(int position);

    UUID getUuid(String name);

    // Generic accessor with automatic logical type conversion
    Object getObject(int position);

    Object getObject(String name);

    // Null checking
    boolean isNull(int position);

    boolean isNull(String name);

    // Metadata
    int getColumnCount();

    String getColumnName(int position);

    // Nested struct access
    Row getStruct(int position);

    Row getStruct(String name);

    // List access - returns List of the appropriate type based on element schema
    List<Integer> getIntList(int position);

    List<Integer> getIntList(String name);

    List<Long> getLongList(int position);

    List<Long> getLongList(String name);

    List<String> getStringList(int position);

    List<String> getStringList(String name);

    // List of structs
    List<Row> getStructList(int position);

    List<Row> getStructList(String name);

    // Generic list access (for nested lists like list<list<int>>)
    List<?> getList(int position);

    List<?> getList(String name);
}
