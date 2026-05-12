/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.row;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.UUID;

/// Type-safe list interface for reading Parquet list values.
///
/// Provides type-specific accessor methods for iterating over list elements.
/// Primitive int/long/double element lists surface as the dedicated
/// [PqIntList] / [PqLongList] / [PqDoubleList] types (with `PrimitiveIterator.OfInt`
/// and friends — no boxing). All other typed accessors return [List].
///
/// A `PqList` is a flyweight over the underlying read batch — its `get(int)` and
/// the lazy [List] views returned by the typed accessors resolve to column data
/// on each call. The flyweight is valid for as long as the batch it was obtained
/// from is current; it is not safe to hold across `rowReader.next()` calls.
///
/// ```java
/// // String list
/// PqList tags = rowReader.getList("tags");
/// for (String tag : tags.strings()) {
///     System.out.println(tag);
/// }
///
/// // Nested struct list
/// PqList items = rowReader.getList("items");
/// for (PqStruct item : items.structs()) {
///     String name = item.getString("name");
/// }
///
/// // Primitive int list — no boxing
/// PqList scores = rowReader.getList("scores");
/// PqIntList ints = scores.ints();
/// for (var it = ints.iterator(); it.hasNext(); ) {
///     int v = it.nextInt();
/// }
///
/// // Nested list (2D matrix)
/// PqList matrix = rowReader.getList("matrix");
/// for (PqIntList innerList : matrix.intLists()) {
///     for (var it = innerList.iterator(); it.hasNext(); ) {
///         int value = it.nextInt();
///     }
/// }
///
/// // Triple nested list (3D cube)
/// PqList cube = rowReader.getList("cube");
/// for (PqList plane : cube.lists()) {
///     for (PqIntList innerList : plane.intLists()) {
///         // ...
///     }
/// }
/// ```
public interface PqList {

    /// Get the number of elements in this list.
    ///
    /// @return the element count
    int size();

    /// Check if this list is empty.
    ///
    /// @return true if the list has no elements
    boolean isEmpty();

    /// Get an element by index, decoded to its logical-type representation.
    ///
    /// Returns the value in the same form as the typed accessors below:
    /// `Integer` / `Long` / `Float` / `Double` / `Boolean` for primitives,
    /// `String` for STRING, [LocalDate] for DATE, [LocalTime] for TIME,
    /// [Instant] for TIMESTAMP, [BigDecimal] for DECIMAL, [UUID] for UUID,
    /// [PqInterval] for INTERVAL, and `byte[]` for BYTE_ARRAY / FIXED_LEN_BYTE_ARRAY
    /// with no logical-type annotation. Nested groups surface as
    /// [PqStruct] / [PqList] / [PqMap].
    ///
    /// @param index the element index (0-based)
    /// @return the decoded element value, or null if the element is null
    /// @throws IndexOutOfBoundsException if index is out of range
    Object get(int index);

    /// Check if an element is null by index.
    ///
    /// @param index the element index (0-based)
    /// @return true if the element is null
    boolean isNull(int index);

    /// View the elements as a [List], each decoded to its logical-type representation.
    ///
    /// Element types match [#get]: `Integer` / `String` / [LocalDate] /
    /// [Instant] / [BigDecimal] / [UUID] / [PqInterval] / etc., with
    /// `byte[]` for un-annotated BYTE_ARRAY / FIXED_LEN_BYTE_ARRAY columns and
    /// [PqStruct] / [PqList] / [PqMap] for nested groups. The returned list is
    /// a live view: each `get(int)` decodes lazily on demand.
    List<Object> values();

    /// Get an element by index in its underlying physical form, mirroring
    /// the `getRawValue` accessors on `RowReader` / [PqStruct] / [PqMap.Entry].
    ///
    /// Returns `Integer` / `Long` / `Float` / `Double` / `Boolean` / `byte[]`
    /// for the physical storage type — `Long` micros instead of [Instant]
    /// for TIMESTAMP(MICROS), unscaled `byte[]` instead of [BigDecimal] for
    /// DECIMAL, `byte[]` instead of `String` for STRING / JSON, etc. Nested
    /// groups have no distinct raw form and surface as the same
    /// [PqStruct] / [PqList] / [PqMap] flyweight returned by [#get].
    ///
    /// @param index the element index (0-based)
    /// @return the raw element value, or null if the element is null
    /// @throws IndexOutOfBoundsException if index is out of range
    Object getRaw(int index);

    /// View the elements as a [List] in their underlying physical form, as in [#getRaw].
    List<Object> rawValues();

    // ==================== Primitive Type Accessors ====================

    /// View the elements as a [PqIntList] (no boxing).
    PqIntList ints();

    /// View the elements as a [PqLongList] (no boxing).
    PqLongList longs();

    /// View the elements as a [List] of float values.
    List<Float> floats();

    /// View the elements as a [PqDoubleList] (no boxing).
    PqDoubleList doubles();

    /// View the elements as a [List] of boolean values.
    List<Boolean> booleans();

    // ==================== Object Type Accessors ====================

    /// View the elements as a [List] of String values.
    List<String> strings();

    /// View the elements as a [List] of binary (`byte[]`) values.
    List<byte[]> binaries();

    /// View the elements as a [List] of [LocalDate] values.
    List<LocalDate> dates();

    /// View the elements as a [List] of [LocalTime] values.
    List<LocalTime> times();

    /// View the elements as a [List] of [Instant] (timestamp) values.
    List<Instant> timestamps();

    /// View the elements as a [List] of [BigDecimal] values.
    List<BigDecimal> decimals();

    /// View the elements as a [List] of [UUID] values.
    List<UUID> uuids();

    /// View the elements as a [List] of [PqInterval] values.
    List<PqInterval> intervals();

    // ==================== Nested Type Accessors ====================

    /// View the elements as a [List] of nested structs.
    List<PqStruct> structs();

    /// View the elements as a [List] of nested lists.
    /// Use this for list-of-list structures.
    List<PqList> lists();

    /// View the elements as a [List] of nested int lists.
    /// Use this for list-of-int-list structures (e.g., 2D int matrix).
    List<PqIntList> intLists();

    /// View the elements as a [List] of nested long lists.
    /// Use this for list-of-long-list structures.
    List<PqLongList> longLists();

    /// View the elements as a [List] of nested double lists.
    /// Use this for list-of-double-list structures.
    List<PqDoubleList> doubleLists();

    /// View the elements as a [List] of nested maps.
    List<PqMap> maps();

    /// View the elements as a [List] of [PqVariant] values.
    ///
    /// Only unshredded variants are supported in repeated contexts today;
    /// iterating a list of shredded variants throws
    /// [UnsupportedOperationException] on first access.
    List<PqVariant> variants();
}
