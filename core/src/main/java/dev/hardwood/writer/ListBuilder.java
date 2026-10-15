/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;
import java.util.function.Consumer;

import dev.hardwood.Experimental;
import dev.hardwood.row.PqFile;
import dev.hardwood.row.PqInterval;

/// Appends the entries of one `LIST` instance.
///
/// The verb matches the list's element type: a list of primitives takes [#addString] and its
/// siblings, a list of structs takes [#addStruct], and a list of lists or of maps takes
/// [#addList] / [#addMap]. Using a verb the element type does not fit throws at the call.
///
/// ```java
/// row.setList("phones", phones -> phones
///         .addString("+49 30 1234567")
///         .addString("+1 415 5550100"));
/// ```
///
/// Every entry the filler appends is one element of the list, in the order appended. A filler
/// that appends nothing writes an empty list, which is distinct from the absent list that
/// leaving the field unset writes.
///
/// The builder is valid only inside the lambda it was passed to. Retaining one and using it
/// afterwards throws [IllegalStateException] rather than appending to a later list.
///
/// **This API is [Experimental]:** the shape may change in future releases.
@Experimental
public interface ListBuilder {

    /// Appends an `INT32` entry.
    ///
    /// @param value the value
    /// @return this builder, for chaining
    /// @throws IllegalArgumentException if the element is not an `INT32` leaf, or the value
    ///         is out of range for the element's annotation
    /// @throws IllegalStateException if this builder's scope has ended
    /// @see StructBuilder#setInt(String, int)
    ListBuilder addInt(int value);

    /// Appends an `INT64` entry.
    ///
    /// @see #addInt(int)
    ListBuilder addLong(long value);

    /// Appends a `FLOAT` entry.
    ///
    /// @see #addInt(int)
    ListBuilder addFloat(float value);

    /// Appends a `DOUBLE` entry.
    ///
    /// @see #addInt(int)
    ListBuilder addDouble(double value);

    /// Appends a `BOOLEAN` entry.
    ///
    /// @see #addInt(int)
    ListBuilder addBoolean(boolean value);

    /// Appends a `STRING`, `ENUM` or `JSON` entry from its UTF-8 bytes. A `null` value
    /// appends a null entry.
    ///
    /// @see #addInt(int)
    /// @see StructBuilder#setString(String, String)
    ListBuilder addString(String value);

    /// Appends a `BYTE_ARRAY` or `FIXED_LEN_BYTE_ARRAY` entry. A `null` value appends a null
    /// entry.
    ///
    /// @see #addInt(int)
    /// @see StructBuilder#setBinary(String, byte[])
    ListBuilder addBinary(byte[] value);

    /// Appends a `DATE` entry. A `null` value appends a null entry.
    ///
    /// @see #addInt(int)
    /// @see StructBuilder#setDate(String, LocalDate)
    ListBuilder addDate(LocalDate value);

    /// Appends a `TIME` entry. A `null` value appends a null entry.
    ///
    /// @see #addInt(int)
    /// @see StructBuilder#setTime(String, LocalTime)
    ListBuilder addTime(LocalTime value);

    /// Appends a UTC-adjusted `TIMESTAMP` entry. A `null` value appends a null entry.
    ///
    /// @see #addInt(int)
    /// @see StructBuilder#setTimestamp(String, Instant)
    ListBuilder addTimestamp(Instant value);

    /// Appends a local-wall-clock `TIMESTAMP` entry. A `null` value appends a null entry.
    ///
    /// @see #addInt(int)
    /// @see StructBuilder#setLocalTimestamp(String, LocalDateTime)
    ListBuilder addLocalTimestamp(LocalDateTime value);

    /// Appends a `DECIMAL` entry. A `null` value appends a null entry.
    ///
    /// @see #addInt(int)
    /// @see StructBuilder#setDecimal(String, BigDecimal)
    ListBuilder addDecimal(BigDecimal value);

    /// Appends a `UUID` entry. A `null` value appends a null entry.
    ///
    /// @see #addInt(int)
    /// @see StructBuilder#setUuid(String, UUID)
    ListBuilder addUuid(UUID value);

    /// Appends an `INTERVAL` entry. A `null` value appends a null entry.
    ///
    /// @see #addInt(int)
    /// @see StructBuilder#setInterval(String, PqInterval)
    ListBuilder addInterval(PqInterval value);

    /// Appends a null entry, which occupies a position in the list — distinct from the list
    /// itself being absent, and from the list being empty.
    ///
    /// @return this builder, for chaining
    /// @throws IllegalArgumentException if the list's element is `REQUIRED`
    /// @throws IllegalStateException if this builder's scope has ended
    ListBuilder addNull();

    /// Appends a struct entry, populating it through `filler`.
    ///
    /// @param filler populates the entry
    /// @return this builder, for chaining
    /// @throws IllegalArgumentException if the list's element is not a struct group (a `FILE`
    ///         element is appended through [#addFile(PqFile)])
    /// @throws IllegalStateException if this builder's scope has ended
    ListBuilder addStruct(Consumer<StructBuilder> filler);

    /// Appends a `FILE` element, the reference `value`. A `null` value appends a null element.
    ///
    /// @param value the reference
    /// @return this builder, for chaining
    /// @throws IllegalArgumentException if the list's element is not a group annotated `FILE`,
    ///         the group has no child for a field `value` sets, `value` is not a valid reference,
    ///         or its `checksum` is not a well-formed token (see [PqFile#withChecksum(String)])
    /// @throws IllegalStateException if this builder's scope has ended
    ListBuilder addFile(PqFile value);

    /// Appends a nested list entry, populating it through `filler`.
    ///
    /// @param filler appends the nested list's entries
    /// @return this builder, for chaining
    /// @throws IllegalArgumentException if the list's element is not a `LIST` group
    /// @throws IllegalStateException if this builder's scope has ended
    ListBuilder addList(Consumer<ListBuilder> filler);

    /// Appends a map entry, populating it through `filler`.
    ///
    /// @param filler appends the map's entries
    /// @return this builder, for chaining
    /// @throws IllegalArgumentException if the list's element is not a `MAP` group
    /// @throws IllegalStateException if this builder's scope has ended
    ListBuilder addMap(Consumer<MapBuilder> filler);
}
