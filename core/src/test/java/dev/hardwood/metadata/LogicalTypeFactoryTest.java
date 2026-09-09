/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Unit tests for the [LogicalType] static factories, the documented way to construct an
/// annotation. The members are taken from the sealed hierarchy itself, so a member added
/// without a factory fails here rather than going unnoticed.
class LogicalTypeFactoryTest {

    static Stream<Class<?>> members() {
        return Stream.of(LogicalType.class.getPermittedSubclasses());
    }

    static Stream<Class<?>> parameterlessMembers() {
        return members().filter(member -> member.getRecordComponents().length == 0);
    }

    @ParameterizedTest
    @MethodSource("members")
    void everyMemberHasAFactoryTakingItsComponents(Class<?> member) {
        assertThat(factoryFor(member))
                .as("static factory on LogicalType returning %s", member.getSimpleName())
                .isNotNull();
    }

    /// A member with no components has exactly one value, so the factory hands out one instance
    /// instead of allocating a fresh record per call.
    @ParameterizedTest
    @MethodSource("parameterlessMembers")
    void parameterlessFactoriesReturnASharedInstance(Class<?> member) throws Exception {
        Method factory = factoryFor(member);

        assertThat(factory.invoke(null)).isSameAs(factory.invoke(null));
    }

    /// The two components read in the order the annotation renders them, which is the reverse
    /// of the order `parquet.thrift` declares the fields in.
    @Test
    void decimalTakesItsPrecisionBeforeItsScale() {
        LogicalType.DecimalType decimal = LogicalType.decimal(9, 2);

        assertThat(decimal.precision()).isEqualTo(9);
        assertThat(decimal.scale()).isEqualTo(2);
    }

    /// A scale above the precision names no decimal, so a transposed pair fails at the call site
    /// rather than several frames later when a schema is built from it.
    @Test
    void decimalRejectsAScaleAboveItsPrecision() {
        assertThatThrownBy(() -> LogicalType.decimal(4, 5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Scale 5 exceeds precision 4");
    }

    @Test
    void timeAndTimestampRequireAUnit() {
        assertThatThrownBy(() -> LogicalType.time(true, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("TIME requires a time unit");
        assertThatThrownBy(() -> LogicalType.timestamp(true, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("TIMESTAMP requires a time unit");
    }

    /// A geospatial annotation naming no CRS carries the format's default, so it matches one
    /// read back from a file that omitted the field.
    @Test
    void geospatialFactoriesSubstituteTheDefaults() {
        assertThat(LogicalType.geometry(null)).isEqualTo(LogicalType.geometry("OGC:CRS84"));
        assertThat(LogicalType.geography(null, null))
                .isEqualTo(LogicalType.geography("OGC:CRS84", LogicalType.EdgeInterpolationAlgorithm.SPHERICAL));
    }

    private static Method factoryFor(Class<?> member) {
        Class<?>[] components = Stream.of(member.getRecordComponents())
                .map(RecordComponent::getType)
                .toArray(Class<?>[]::new);

        return Stream.of(LogicalType.class.getDeclaredMethods())
                .filter(method -> Modifier.isStatic(method.getModifiers()))
                .filter(method -> method.getReturnType() == member)
                .filter(method -> Arrays.equals(method.getParameterTypes(), components))
                .findFirst()
                .orElse(null);
    }
}
