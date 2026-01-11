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
import java.util.UUID;

/**
 * Type-safe representation of Parquet types.
 * Each subtype corresponds to a physical or logical type, with the type parameter
 * representing the corresponding Java type.
 *
 * @param <T> the Java type that values of this Parquet type map to
 */
public sealed

interface PqType<T> {

    // Singleton constants for all supported types
    PqType<Boolean> BOOLEAN = new BooleanType();
    PqType<Integer> INT32 = new Int32Type();
    PqType<Long> INT64 = new Int64Type();
    PqType<Float> FLOAT = new FloatType();
    PqType<Double> DOUBLE = new DoubleType();
    PqType<byte[]> BINARY = new BinaryType();
    PqType<String> STRING = new StringType();
    PqType<LocalDate> DATE = new DateType();
    PqType<LocalTime> TIME = new TimeType();
    PqType<Instant> TIMESTAMP = new TimestampType();
    PqType<BigDecimal> DECIMAL = new DecimalType();
    PqType<UUID> UUID = new UuidType();
    PqType<PqRow> ROW = new RowType();
    PqType<PqList> LIST = new ListType();

    // Physical types
    record BooleanType() implements PqType<Boolean> {}

    record Int32Type() implements PqType<Integer> {}

    record Int64Type() implements PqType<Long> {}

    record FloatType() implements PqType<Float> {}

    record DoubleType() implements PqType<Double> {}

    record BinaryType() implements PqType<byte[]> {}

    // Logical types
    record StringType() implements PqType<String> {}

    record DateType() implements PqType<LocalDate> {}

    record TimeType() implements PqType<LocalTime> {}

    record TimestampType() implements PqType<Instant> {}

    record DecimalType() implements PqType<BigDecimal> {}

    record UuidType() implements PqType<UUID> {}

    // Nested types
    record RowType() implements PqType<PqRow> {}

    record ListType() implements PqType<PqList> {}
}
