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
import java.util.UUID;

/// Variant value — the top-level accessor for a Parquet column annotated with
/// the `VARIANT` logical type. A Variant carries the canonical two-part binary
/// encoding ([#metadata()], [#value()]) plus a type tag ([#type()]), and exposes
/// tag-specific extraction through the `as*()` methods.
///
/// ```java
/// PqVariant v = row.getVariant("event");
/// if (v.type() == VariantType.OBJECT) {
///     PqVariantObject obj = v.asObject();
///     String userId = obj.getString("user_id");
/// }
/// ```
///
/// Every nested Variant sub-value (array element, object field value) is itself
/// a `PqVariant`, so navigation composes naturally.
///
/// @see <a href="https://github.com/apache/parquet-format/blob/master/VariantEncoding.md">Parquet Variant Encoding</a>
public interface PqVariant {

    // ==================== Raw canonical bytes ====================

    /// The canonical Variant metadata bytes.
    byte[] metadata();

    /// The canonical Variant value bytes.
    byte[] value();

    // ==================== Type introspection ====================

    /// The Variant type tag decoded from the value header.
    VariantType type();

    /// True iff [#type()] is [VariantType#NULL].
    boolean isNull();

    // ==================== Primitive extraction ====================

    /// Extract a boolean. Requires [VariantType#BOOLEAN_TRUE] or [VariantType#BOOLEAN_FALSE].
    ///
    /// @throws VariantTypeException if the type is not a boolean
    boolean asBoolean();

    /// Extract an int. Narrows INT8/INT16/INT32 values.
    ///
    /// @throws VariantTypeException if the type is not INT8/INT16/INT32
    int asInt();

    /// Extract a long. Accepts INT8/INT16/INT32/INT64.
    ///
    /// @throws VariantTypeException if the type is not an integer
    long asLong();

    /// Extract a float.
    ///
    /// @throws VariantTypeException if the type is not [VariantType#FLOAT]
    float asFloat();

    /// Extract a double.
    ///
    /// @throws VariantTypeException if the type is not [VariantType#DOUBLE]
    double asDouble();

    /// Extract a UTF-8 string. Accepts both the short-string encoding and the
    /// primitive STRING encoding.
    ///
    /// @throws VariantTypeException if the type is not a string
    String asString();

    /// Extract opaque binary bytes.
    ///
    /// @throws VariantTypeException if the type is not [VariantType#BINARY]
    byte[] asBinary();

    /// Extract a decimal. Accepts DECIMAL4/DECIMAL8/DECIMAL16.
    ///
    /// @throws VariantTypeException if the type is not a decimal
    BigDecimal asDecimal();

    /// Extract a date (days since Unix epoch).
    ///
    /// @throws VariantTypeException if the type is not [VariantType#DATE]
    LocalDate asDate();

    /// Extract a time of day.
    ///
    /// @throws VariantTypeException if the type is not [VariantType#TIME_NTZ]
    LocalTime asTime();

    /// Extract a timestamp. Accepts TIMESTAMP/TIMESTAMP_NTZ (micros) and
    /// TIMESTAMP_NANOS/TIMESTAMP_NTZ_NANOS (nanos).
    ///
    /// @throws VariantTypeException if the type is not a timestamp
    Instant asTimestamp();

    /// Extract a UUID (16 bytes, big-endian).
    ///
    /// @throws VariantTypeException if the type is not [VariantType#UUID]
    UUID asUuid();

    // ==================== Complex unwrapping ====================

    /// Unwrap as a Variant object view for name-based field navigation.
    ///
    /// @throws VariantTypeException if the type is not [VariantType#OBJECT]
    PqVariantObject asObject();

    /// Unwrap as a Variant array view for indexed element access.
    ///
    /// @throws VariantTypeException if the type is not [VariantType#ARRAY]
    PqVariantArray asArray();
}
