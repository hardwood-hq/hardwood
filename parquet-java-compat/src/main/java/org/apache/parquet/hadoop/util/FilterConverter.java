/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.parquet.hadoop.util;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.io.api.Binary;

import dev.hardwood.internal.schema.SchemaPathResolver;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;

/// Converts parquet-java [FilterPredicate] trees to Hardwood
/// [dev.hardwood.reader.FilterPredicate] trees.
final class FilterConverter {

    /// A `FLOAT16` value is two little-endian bytes of an IEEE half.
    private static final int FLOAT16_BYTES = 2;

    private FilterConverter() {
    }

    /// Convert a parquet-java FilterPredicate to a Hardwood FilterPredicate.
    ///
    /// @param predicate the parquet-java predicate
    /// @param schema the schema of the file the predicate filters
    /// @return the equivalent Hardwood predicate
    /// @throws UnsupportedOperationException if the predicate type is not supported
    static dev.hardwood.reader.FilterPredicate convert(FilterPredicate predicate, FileSchema schema) {
        return switch (predicate) {
            case Operators.Eq<?> p -> convertComparison(p.getColumn(), p.getValue(),
                    dev.hardwood.reader.FilterPredicate.Operator.EQ, schema);
            case Operators.NotEq<?> p -> convertComparison(p.getColumn(), p.getValue(),
                    dev.hardwood.reader.FilterPredicate.Operator.NOT_EQ, schema);
            case Operators.Lt<?> p -> convertComparison(p.getColumn(), p.getValue(),
                    dev.hardwood.reader.FilterPredicate.Operator.LT, schema);
            case Operators.LtEq<?> p -> convertComparison(p.getColumn(), p.getValue(),
                    dev.hardwood.reader.FilterPredicate.Operator.LT_EQ, schema);
            case Operators.Gt<?> p -> convertComparison(p.getColumn(), p.getValue(),
                    dev.hardwood.reader.FilterPredicate.Operator.GT, schema);
            case Operators.GtEq<?> p -> convertComparison(p.getColumn(), p.getValue(),
                    dev.hardwood.reader.FilterPredicate.Operator.GT_EQ, schema);
            case Operators.And p -> dev.hardwood.reader.FilterPredicate.and(
                    convert(p.getLeft(), schema), convert(p.getRight(), schema));
            case Operators.Or p -> dev.hardwood.reader.FilterPredicate.or(
                    convert(p.getLeft(), schema), convert(p.getRight(), schema));
            case Operators.Not p -> dev.hardwood.reader.FilterPredicate.not(
                    convert(p.getPredicate(), schema));
            default -> throw new UnsupportedOperationException(
                    "Unsupported filter predicate type: " + predicate.getClass().getName());
        };
    }

    private static dev.hardwood.reader.FilterPredicate convertComparison(
            Operators.Column<?> column, Object value,
            dev.hardwood.reader.FilterPredicate.Operator op, FileSchema schema) {
        String columnName = column.getColumnPath().toDotString();

        return switch (value) {
            case Integer v -> new dev.hardwood.reader.FilterPredicate.IntColumnPredicate(columnName, op, v);
            case Long v -> new dev.hardwood.reader.FilterPredicate.LongColumnPredicate(columnName, op, v);
            case Float v -> new dev.hardwood.reader.FilterPredicate.FloatColumnPredicate(columnName, op, v);
            case Double v -> new dev.hardwood.reader.FilterPredicate.DoubleColumnPredicate(columnName, op, v);
            case Boolean v -> new dev.hardwood.reader.FilterPredicate.BooleanColumnPredicate(columnName, op, v);
            case Binary v -> convertBinary(columnName, op, v.getBytesUnsafe(), schema);
            case null -> throw new IllegalArgumentException(
                    "Null filter values are not supported for column: " + columnName);
            default -> throw new UnsupportedOperationException(
                    "Unsupported filter value type: " + value.getClass().getName()
                            + " for column: " + columnName);
        };
    }

    /// A `Binary` literal, which parquet-java compares through the column's comparator: as the
    /// number on a binary `DECIMAL`, as the half on a `FLOAT16`, and as the bytes elsewhere.
    /// Hardwood's `byte[]` literal is the stored bytes on every column, so the first two convert
    /// to the literal that compares as the value.
    private static dev.hardwood.reader.FilterPredicate convertBinary(String columnName,
            dev.hardwood.reader.FilterPredicate.Operator op, byte[] bytes, FileSchema schema) {
        SchemaNode.PrimitiveNode column = leaf(columnName, schema);
        LogicalType logicalType = column == null ? null : column.logicalType();
        if (logicalType instanceof LogicalType.DecimalType decimal && (column.type() == PhysicalType.BYTE_ARRAY
                || column.type() == PhysicalType.FIXED_LEN_BYTE_ARRAY)) {
            // An empty value is zero, as a DECIMAL column reads one.
            BigInteger unscaled = bytes.length == 0 ? BigInteger.ZERO : new BigInteger(bytes);
            return new dev.hardwood.reader.FilterPredicate.DecimalColumnPredicate(columnName, op,
                    new BigDecimal(unscaled, decimal.scale()));
        }
        if (logicalType instanceof LogicalType.Float16Type && bytes.length == FLOAT16_BYTES) {
            float half = Float.float16ToFloat((short) ((bytes[1] & 0xFF) << 8 | bytes[0] & 0xFF));
            return new dev.hardwood.reader.FilterPredicate.FloatColumnPredicate(columnName, op, half);
        }
        return new dev.hardwood.reader.FilterPredicate.BinaryColumnPredicate(columnName, op, bytes);
    }

    /// The leaf column at the dotted path, or `null` where there is none, which the reader reports
    /// when it resolves the converted predicate.
    private static SchemaNode.PrimitiveNode leaf(String columnName, FileSchema schema) {
        return SchemaPathResolver.resolve(schema, columnName).node() instanceof SchemaNode.PrimitiveNode leaf
                ? leaf
                : null;
    }
}
