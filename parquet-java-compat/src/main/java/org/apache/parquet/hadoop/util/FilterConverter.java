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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
            case Operators.In<?> p -> convertIn(p, schema);
            case Operators.NotIn<?> p -> dev.hardwood.reader.FilterPredicate.not(convertIn(p, schema));
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

        return switch (literal(columnName, value, schema)) {
            case Integer v -> new dev.hardwood.reader.FilterPredicate.IntColumnPredicate(columnName, op, v);
            case Long v -> new dev.hardwood.reader.FilterPredicate.LongColumnPredicate(columnName, op, v);
            case Float v -> new dev.hardwood.reader.FilterPredicate.FloatColumnPredicate(columnName, op, v);
            case Double v -> new dev.hardwood.reader.FilterPredicate.DoubleColumnPredicate(columnName, op, v);
            case Boolean v -> new dev.hardwood.reader.FilterPredicate.BooleanColumnPredicate(columnName, op, v);
            case BigDecimal v -> new dev.hardwood.reader.FilterPredicate.DecimalColumnPredicate(columnName, op, v);
            case byte[] v -> new dev.hardwood.reader.FilterPredicate.BinaryColumnPredicate(columnName, op, v);
            default -> throw unsupportedValue(columnName, value);
        };
    }

    /// An `in` over the values of `predicate`, each converted as a comparison literal is. The
    /// literals of one Java type form one `in`, and literals of several types a disjunction of
    /// them. Only a `FLOAT16` set mixing two-byte and other values converts to two types, and the
    /// reader refuses its `byte[]` part, as it refuses such a comparison literal. Hardwood has no
    /// boolean `in`, so boolean values are a disjunction of equalities.
    private static dev.hardwood.reader.FilterPredicate convertIn(
            Operators.SetColumnFilterPredicate<?> predicate, FileSchema schema) {
        String columnName = predicate.getColumn().getColumnPath().toDotString();
        Map<Class<?>, List<Object>> literalsByType = new LinkedHashMap<>();
        for (Object value : predicate.getValues()) {
            Object literal = literal(columnName, value, schema);
            literalsByType.computeIfAbsent(literal.getClass(), type -> new ArrayList<>()).add(literal);
        }
        List<dev.hardwood.reader.FilterPredicate> parts = new ArrayList<>();
        for (List<Object> literals : literalsByType.values()) {
            parts.add(in(columnName, literals));
        }
        return anyOf(parts);
    }

    /// An `in` over `literals`, which all have the Java type of the first.
    private static dev.hardwood.reader.FilterPredicate in(String columnName, List<Object> literals) {
        return switch (literals.getFirst()) {
            case Integer v -> dev.hardwood.reader.FilterPredicate.in(columnName,
                    literals.stream().mapToInt(Integer.class::cast).toArray());
            case Long v -> dev.hardwood.reader.FilterPredicate.in(columnName,
                    literals.stream().mapToLong(Long.class::cast).toArray());
            case Float v -> dev.hardwood.reader.FilterPredicate.in(columnName, floats(literals));
            case Double v -> dev.hardwood.reader.FilterPredicate.in(columnName,
                    literals.stream().mapToDouble(Double.class::cast).toArray());
            case BigDecimal v -> dev.hardwood.reader.FilterPredicate.in(columnName,
                    literals.toArray(BigDecimal[]::new));
            case byte[] v -> dev.hardwood.reader.FilterPredicate.in(columnName, literals.toArray(byte[][]::new));
            case Boolean v -> anyOf(literals.stream()
                    .map(literal -> dev.hardwood.reader.FilterPredicate.eq(columnName, (boolean) literal))
                    .toList());
            default -> throw unsupportedValue(columnName, literals.getFirst());
        };
    }

    private static float[] floats(List<Object> literals) {
        float[] floats = new float[literals.size()];
        for (int i = 0; i < floats.length; i++) {
            floats[i] = (Float) literals.get(i);
        }
        return floats;
    }

    /// The disjunction of `predicates`, or the one predicate where there is one.
    private static dev.hardwood.reader.FilterPredicate anyOf(List<dev.hardwood.reader.FilterPredicate> predicates) {
        return predicates.size() == 1
                ? predicates.getFirst()
                : dev.hardwood.reader.FilterPredicate.or(predicates.toArray(dev.hardwood.reader.FilterPredicate[]::new));
    }

    /// The Hardwood literal for a parquet-java filter value: an `Integer`, `Long`, `Float`,
    /// `Double` or `Boolean` as it is, and a `Binary` as [#binaryLiteral] converts it.
    private static Object literal(String columnName, Object value, FileSchema schema) {
        return switch (value) {
            case Integer v -> v;
            case Long v -> v;
            case Float v -> v;
            case Double v -> v;
            case Boolean v -> v;
            case Binary v -> binaryLiteral(columnName, v.getBytesUnsafe(), schema);
            case null -> throw new IllegalArgumentException(
                    "Null filter values are not supported for column: " + columnName);
            default -> throw unsupportedValue(columnName, value);
        };
    }

    private static UnsupportedOperationException unsupportedValue(String columnName, Object value) {
        return new UnsupportedOperationException(
                "Unsupported filter value type: " + value.getClass().getName() + " for column: " + columnName);
    }

    /// A `Binary` literal, which parquet-java compares through the column's comparator: as the
    /// number on a binary `DECIMAL`, as the half on a `FLOAT16`, and as the bytes elsewhere.
    /// Hardwood's `byte[]` literal is the stored bytes on every column, so the first two convert
    /// to the `BigDecimal` and the `float` that compare as the value.
    private static Object binaryLiteral(String columnName, byte[] bytes, FileSchema schema) {
        SchemaNode.PrimitiveNode column = leaf(columnName, schema);
        LogicalType logicalType = column == null ? null : column.logicalType();
        if (logicalType instanceof LogicalType.DecimalType decimal && (column.type() == PhysicalType.BYTE_ARRAY
                || column.type() == PhysicalType.FIXED_LEN_BYTE_ARRAY)) {
            // An empty value is zero, as a DECIMAL column reads one.
            BigInteger unscaled = bytes.length == 0 ? BigInteger.ZERO : new BigInteger(bytes);
            return new BigDecimal(unscaled, decimal.scale());
        }
        if (logicalType instanceof LogicalType.Float16Type && bytes.length == FLOAT16_BYTES) {
            return Float.float16ToFloat((short) ((bytes[1] & 0xFF) << 8 | bytes[0] & 0xFF));
        }
        return bytes;
    }

    /// The leaf column at the dotted path, or `null` where there is none, which the reader reports
    /// when it resolves the converted predicate.
    private static SchemaNode.PrimitiveNode leaf(String columnName, FileSchema schema) {
        return SchemaPathResolver.resolve(schema, columnName).node() instanceof SchemaNode.PrimitiveNode leaf
                ? leaf
                : null;
    }
}
