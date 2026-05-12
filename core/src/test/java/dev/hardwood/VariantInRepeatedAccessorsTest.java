/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqVariant;
import dev.hardwood.row.VariantType;

import static org.assertj.core.api.Assertions.assertThat;

/// Coverage for hardwood#464: variant accessors on `PqMap.Entry`,
/// `PqList`, and `RowReader.getVariant(int)`. The fixture carries one
/// top-level variant column plus a `Map<String, Variant>` and a
/// `List<Variant>`, all unshredded. Shredded variants in repeated
/// contexts remain deferred (tracked in #467) and are exercised by an
/// `UnsupportedOperationException` assertion in a sibling test once
/// that work lands.
class VariantInRepeatedAccessorsTest {

    private static final Path FIXTURE =
            Paths.get("src/test/resources/variant_in_repeated_test.parquet");

    @Test
    void rowReaderGetVariantByIndexReturnsTopLevelVariant() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rowReader = fileReader.rowReader()) {
            rowReader.next();
            // Resolve the projected field index for the top_var group; there's
            // no FileSchema.getColumn for non-leaf nodes.
            int idx = -1;
            for (int i = 0; i < rowReader.getFieldCount(); i++) {
                if ("top_var".equals(rowReader.getFieldName(i))) {
                    idx = i;
                    break;
                }
            }
            assertThat(idx).isGreaterThanOrEqualTo(0);
            PqVariant v = rowReader.getVariant(idx);
            assertThat(v).isNotNull();
            assertThat(v.type()).isEqualTo(VariantType.BOOLEAN_TRUE);
            assertThat(v.asBoolean()).isTrue();

            // Sanity: by-name path produces the same value.
            assertThat(rowReader.getVariant("top_var").asBoolean()).isTrue();
        }
    }

    @Test
    void pqMapEntryGetVariantValueDecodesUnshreddedVariant() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rowReader = fileReader.rowReader()) {
            rowReader.next();
            PqMap varMap = rowReader.getMap("var_map");
            List<PqMap.Entry> entries = varMap.getEntries();
            assertThat(entries).hasSize(2);

            // a -> BOOLEAN_TRUE
            PqMap.Entry e0 = entries.get(0);
            assertThat(e0.getStringKey()).isEqualTo("a");
            PqVariant v0 = e0.getVariantValue();
            assertThat(v0).isNotNull();
            assertThat(v0.type()).isEqualTo(VariantType.BOOLEAN_TRUE);

            // b -> INT32(7)
            PqMap.Entry e1 = entries.get(1);
            assertThat(e1.getStringKey()).isEqualTo("b");
            assertThat(e1.getVariantValue().asInt()).isEqualTo(7);

            // getValue() should also surface the variant flyweight, mirroring
            // how getStructValue / getListValue / getMapValue feed groupValueOrNull.
            assertThat(e1.getValue()).isInstanceOf(PqVariant.class);
        }
    }

    @Test
    void pqListVariantsIteratesUnshreddedVariantElements() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rowReader = fileReader.rowReader()) {
            rowReader.next();
            PqList varList = rowReader.getList("var_list");
            assertThat(varList.size()).isEqualTo(3);

            List<VariantType> kinds = new ArrayList<>();
            List<String> stringValues = new ArrayList<>();
            for (PqVariant v : varList.variants()) {
                kinds.add(v.type());
                if (v.type() == VariantType.STRING) {
                    stringValues.add(v.asString());
                }
            }
            assertThat(kinds).containsExactly(
                    VariantType.BOOLEAN_TRUE,
                    VariantType.INT32,
                    VariantType.STRING);
            assertThat(stringValues).containsExactly("hi");

            // Indexed access through PqList.get(int) also returns the variant.
            assertThat(varList.get(1)).isInstanceOf(PqVariant.class);
            assertThat(((PqVariant) varList.get(1)).asInt()).isEqualTo(7);
        }
    }
}
