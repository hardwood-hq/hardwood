/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.row;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/// Pins the interface hierarchy that lets Parquet structs and Variant objects
/// share the primitive accessor surface without `PqVariantObject` inheriting
/// Parquet-specific complex getters (`getStruct` / `getList` / `getMap`).
class FieldAccessorSplitTest {

    @Test
    void structAccessorExtendsFieldAccessor() {
        assertThat(FieldAccessor.class.isAssignableFrom(StructAccessor.class)).isTrue();
    }

    @Test
    void pqVariantObjectImplementsFieldAccessorButNotStructAccessor() {
        assertThat(FieldAccessor.class.isAssignableFrom(PqVariantObject.class)).isTrue();
        assertThat(StructAccessor.class.isAssignableFrom(PqVariantObject.class)).isFalse();
    }

    @Test
    void pqStructImplementsStructAccessor() {
        assertThat(StructAccessor.class.isAssignableFrom(PqStruct.class)).isTrue();
    }

    @Test
    void complexParquetGettersLiveOnStructAccessorOnly() throws NoSuchMethodException {
        // getStruct / getList / getMap must be declared on StructAccessor, not
        // FieldAccessor — otherwise PqVariantObject would inherit them.
        StructAccessor.class.getMethod("getStruct", String.class);
        StructAccessor.class.getMethod("getList", String.class);
        StructAccessor.class.getMethod("getMap", String.class);
        assertThat(hasMethod(FieldAccessor.class, "getStruct", String.class)).isFalse();
        assertThat(hasMethod(FieldAccessor.class, "getList", String.class)).isFalse();
        assertThat(hasMethod(FieldAccessor.class, "getMap", String.class)).isFalse();
    }

    @Test
    void getVariantLivesOnFieldAccessor() throws NoSuchMethodException {
        FieldAccessor.class.getMethod("getVariant", String.class);
    }

    private static boolean hasMethod(Class<?> type, String name, Class<?>... args) {
        try {
            type.getMethod(name, args);
            return true;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }
}
