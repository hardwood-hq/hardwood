/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.LayerKind;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.Validity;

import static org.assertj.core.api.Assertions.assertThat;

/// Pins the public-API contracts of [ColumnReader]'s layer model: layer count
/// / kind, real-items-only offsets and validity, sentinel suffix, varlength
/// leaf buffers and the convenience accessors that read them.
///
/// Each test maps onto one of the design-doc requirements for the Phase 2
/// rework (see _designs/COLUMN_READER_ARROW_LAYOUT.md).
class ColumnReaderLayerModelTest {

    /// `primitive_lists_test.parquet` row 0 = `[1,2,3]`, row 1 = `[4,5]`,
    /// row 2 = `[]` (empty), row 3 = `null` (null list). Verifies that the
    /// layer model encodes empty as `offsets[r+1] - offsets[r] == 0` and null
    /// as a cleared validity bit, with [Validity#NO_NULLS] signalling the
    /// all-present fast path.
    @Test
    void fourStateListEmptyVsNullVsValuesVsNullElement() throws Exception {
        Path file = Paths.get("src/test/resources/primitive_lists_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
             ColumnReader col = reader.columnReader("int_list.list.element")) {

            assertThat(col.nextBatch()).isTrue();
            assertThat(col.getRecordCount()).isEqualTo(4);
            assertThat(col.getLayerCount()).isEqualTo(1);
            assertThat(col.getLayerKind(0)).isEqualTo(LayerKind.REPEATED);

            int[] offsets = col.getLayerOffsets(0);
            assertThat(offsets).as("sentinel-suffixed length").hasSize(5);
            assertThat(offsets[4]).as("trailing sentinel matches valueCount")
                    .isEqualTo(col.getValueCount());

            Validity listValidity = col.getLayerValidity(0);
            assertThat(listValidity.hasNulls()).isTrue();

            // Row 0: present, non-empty
            assertThat(listValidity.isNotNull(0)).isTrue();
            assertThat(offsets[1] - offsets[0]).isGreaterThan(0);
            // Row 1: present, non-empty
            assertThat(listValidity.isNotNull(1)).isTrue();
            assertThat(offsets[2] - offsets[1]).isGreaterThan(0);
            // Row 2: present, empty
            assertThat(listValidity.isNotNull(2)).isTrue();
            assertThat(offsets[3] - offsets[2]).isEqualTo(0);
            // Row 3: null
            assertThat(listValidity.isNull(3)).isTrue();
            assertThat(offsets[4] - offsets[3]).isEqualTo(0);
        }
    }

    /// `nested_struct_test.parquet` projects `address.zip` (int32 under
    /// `optional struct address { ... required int32 zip }`). With a STRUCT
    /// layer above a required leaf: `getLayerCount() == 1`,
    /// `getLayerKind(0) == STRUCT`, `getLayerOffsets(0)` throws (STRUCT layers
    /// have no offsets), and `getLayerValidity(0)` distinguishes
    /// "address is null" from "address is present".
    @Test
    void optionalStructAboveLeafExposesStructLayerValidity() throws Exception {
        Path file = Paths.get("src/test/resources/nested_struct_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
             ColumnReader col = reader.columnReader("address.zip")) {

            assertThat(col.nextBatch()).isTrue();
            assertThat(col.getLayerCount()).isEqualTo(1);
            assertThat(col.getLayerKind(0)).isEqualTo(LayerKind.STRUCT);
            assertThat(col.getValueCount()).isEqualTo(col.getRecordCount());

            Validity structValidity = col.getLayerValidity(0);
            assertThat(structValidity.hasNulls()).isTrue();
            assertThat(structValidity.isNotNull(0)).isTrue();
            assertThat(structValidity.isNotNull(1)).isTrue();
            assertThat(structValidity.isNull(2)).isTrue();   // address null

            // STRUCT layers have no offsets buffer.
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> col.getLayerOffsets(0))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("not REPEATED");
        }
    }

    /// `deep_nested_struct_test.parquet` projects the leaf `zip` four levels
    /// deep under three nested optional structs. Verifies that `STRUCT`
    /// validity at every layer is reported independently — Bob has
    /// `account.organization.address == null`, Charlie has
    /// `account.organization == null`, Diana has `account == null`.
    /// This is the multi-layer extension of #436.
    @Test
    void threeDeepStructChainExposesPerLayerValidity() throws Exception {
        Path file = Paths.get("src/test/resources/deep_nested_struct_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
             ColumnReader col = reader.columnReader("account.organization.address.zip")) {

            assertThat(col.nextBatch()).isTrue();
            assertThat(col.getLayerCount()).isEqualTo(3);
            assertThat(col.getLayerKind(0)).isEqualTo(LayerKind.STRUCT);
            assertThat(col.getLayerKind(1)).isEqualTo(LayerKind.STRUCT);
            assertThat(col.getLayerKind(2)).isEqualTo(LayerKind.STRUCT);

            Validity accountValidity = col.getLayerValidity(0);
            Validity orgValidity = col.getLayerValidity(1);
            Validity addressValidity = col.getLayerValidity(2);

            // Row 0 (Alice): full chain present
            assertThat(accountValidity.isNotNull(0)).isTrue();
            assertThat(orgValidity.isNotNull(0)).isTrue();
            assertThat(addressValidity.isNotNull(0)).isTrue();

            // Row 1 (Bob): account/org present, address null
            assertThat(accountValidity.isNotNull(1)).isTrue();
            assertThat(orgValidity.isNotNull(1)).isTrue();
            assertThat(addressValidity.isNull(1)).isTrue();

            // Row 2 (Charlie): account present, org null (cascades to address)
            assertThat(accountValidity.isNotNull(2)).isTrue();
            assertThat(orgValidity.isNull(2)).isTrue();
            assertThat(addressValidity.isNull(2)).isTrue();

            // Row 3 (Diana): account null (cascades to org and address)
            assertThat(accountValidity.isNull(3)).isTrue();
            assertThat(orgValidity.isNull(3)).isTrue();
            assertThat(addressValidity.isNull(3)).isTrue();
        }
    }

    /// `nested_list_test.parquet`'s `matrix` column is `list<list<int32>>`.
    /// Verifies the chained-offsets walk: `getLayerOffsets(0)` indexes into
    /// `getLayerOffsets(1)`, which in turn indexes into the leaf. Sentinels
    /// at every layer make the inner-loop bounds uniform across records.
    @Test
    void listOfListIntChainedOffsets() throws Exception {
        Path file = Paths.get("src/test/resources/nested_list_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
             ColumnReader col = reader.columnReader("matrix.list.element.list.element")) {

            assertThat(col.nextBatch()).isTrue();
            assertThat(col.getLayerCount()).isEqualTo(2);
            assertThat(col.getLayerKind(0)).isEqualTo(LayerKind.REPEATED);
            assertThat(col.getLayerKind(1)).isEqualTo(LayerKind.REPEATED);

            int recordCount = col.getRecordCount();
            int[] outerOffsets = col.getLayerOffsets(0);
            int[] innerOffsets = col.getLayerOffsets(1);
            assertThat(outerOffsets).hasSize(recordCount + 1);

            // Outer sentinel matches inner-layer item count
            assertThat(outerOffsets[recordCount]).isEqualTo(innerOffsets.length - 1);
            // Inner sentinel matches leaf valueCount
            assertThat(innerOffsets[innerOffsets.length - 1]).isEqualTo(col.getValueCount());

            // Row 0: matrix=[[1,2],[3,4,5],[6]] — 3 inner lists, 6 leaf values
            int innerCountRow0 = outerOffsets[1] - outerOffsets[0];
            assertThat(innerCountRow0).isEqualTo(3);
            int leafCountRow0 = innerOffsets[outerOffsets[1]] - innerOffsets[outerOffsets[0]];
            assertThat(leafCountRow0).isEqualTo(6);

            int[] values = col.getInts();
            // First inner list of row 0 = [1,2]
            int firstInnerStart = innerOffsets[outerOffsets[0]];
            int firstInnerEnd = innerOffsets[outerOffsets[0] + 1];
            assertThat(values[firstInnerStart]).isEqualTo(1);
            assertThat(values[firstInnerEnd - 1]).isEqualTo(2);
        }
    }

    /// `plain_uncompressed_with_nulls.parquet` has a flat `optional binary`
    /// column. `getBinaryValues()` and `getBinaryOffsets()` round-trip every
    /// non-null value — no STRUCT or REPEATED layers; `getLayerCount() == 0`.
    @Test
    void flatBinaryRoundTrip() throws Exception {
        Path file = Paths.get("src/test/resources/plain_uncompressed_with_nulls.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
             ColumnReader col = reader.columnReader("name")) {

            assertThat(col.nextBatch()).isTrue();
            assertThat(col.getLayerCount()).isEqualTo(0);

            byte[] bytes = col.getBinaryValues();
            int[] offsets = col.getBinaryOffsets();
            Validity validity = col.getLeafValidity();

            assertThat(offsets).hasSize(col.getValueCount() + 1);

            // Negative-space test: bytes buffer is capacity-sized, may be
            // larger than the prefix in use.
            assertThat(bytes.length).isGreaterThanOrEqualTo(offsets[col.getValueCount()]);

            // Row 0 = "alice", row 1 = null, row 2 = "charlie"
            assertThat(validity.isNotNull(0)).isTrue();
            String s0 = new String(bytes, offsets[0], offsets[1] - offsets[0], StandardCharsets.UTF_8);
            assertThat(s0).isEqualTo("alice");

            assertThat(validity.isNull(1)).isTrue();   // null

            assertThat(validity.isNotNull(2)).isTrue();
            String s2 = new String(bytes, offsets[2], offsets[3] - offsets[2], StandardCharsets.UTF_8);
            assertThat(s2).isEqualTo("charlie");

            // Convenience-accessor parity
            String[] strings = col.getStrings();
            assertThat(strings[0]).isEqualTo("alice");
            assertThat(strings[1]).isNull();
            assertThat(strings[2]).isEqualTo("charlie");
        }
    }

    /// `list_basic_test.parquet`'s `tags` is `list<string>` with rows
    /// `["a","b","c"]`, `[]`, `null`, `["single"]`. Cross-product of layer
    /// offsets (records → string positions) and binary offsets (string
    /// position → byte spans).
    @Test
    void listOfStringCrossProductOfOffsets() throws Exception {
        Path file = Paths.get("src/test/resources/list_basic_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
             ColumnReader col = reader.columnReader("tags.list.element")) {

            assertThat(col.nextBatch()).isTrue();
            assertThat(col.getLayerCount()).isEqualTo(1);
            assertThat(col.getLayerKind(0)).isEqualTo(LayerKind.REPEATED);

            int[] layerOffsets = col.getLayerOffsets(0);
            Validity listValidity = col.getLayerValidity(0);
            byte[] bytes = col.getBinaryValues();
            int[] binaryOffsets = col.getBinaryOffsets();

            // 4 records, sentinel suffix
            assertThat(layerOffsets).hasSize(5);
            // Row 0: ["a","b","c"]
            assertThat(layerOffsets[1] - layerOffsets[0]).isEqualTo(3);
            String row0First = new String(bytes,
                    binaryOffsets[layerOffsets[0]],
                    binaryOffsets[layerOffsets[0] + 1] - binaryOffsets[layerOffsets[0]],
                    StandardCharsets.UTF_8);
            assertThat(row0First).isEqualTo("a");

            // Row 1: empty list (present, zero items)
            assertThat(listValidity.hasNulls()).isTrue();
            assertThat(listValidity.isNotNull(1)).isTrue();
            assertThat(layerOffsets[2] - layerOffsets[1]).isEqualTo(0);

            // Row 2: null list (cleared validity, zero-span at offsets)
            assertThat(listValidity.isNull(2)).isTrue();
            assertThat(layerOffsets[3] - layerOffsets[2]).isEqualTo(0);

            // Row 3: ["single"]
            assertThat(layerOffsets[4] - layerOffsets[3]).isEqualTo(1);
            String row3 = new String(bytes,
                    binaryOffsets[layerOffsets[3]],
                    binaryOffsets[layerOffsets[3] + 1] - binaryOffsets[layerOffsets[3]],
                    StandardCharsets.UTF_8);
            assertThat(row3).isEqualTo("single");

            // Sentinel matches leaf valueCount
            assertThat(binaryOffsets[col.getValueCount()])
                    .isLessThanOrEqualTo(bytes.length);

            // Convenience-accessor parity
            String[] flat = col.getStrings();
            assertThat(flat).hasSize(col.getValueCount());
            assertThat(flat[layerOffsets[0]]).isEqualTo("a");
            assertThat(flat[layerOffsets[3]]).isEqualTo("single");
        }
    }

    /// `address_book_test.parquet`'s `contacts: list<Contact(name,
    /// phoneNumber)>` projects a STRUCT-inside-LIST shape. Verifies that the
    /// element-wrapper rule contributes a `STRUCT` layer (the design's
    /// correctness gate against re-folding list-element struct nullability
    /// into the leaf), and that the leaf validity at the inner-most layer
    /// independently reports `phoneNumber == null` for record 0's second
    /// contact (Chris Aniszczyk).
    @Test
    void listOfStructExposesElementWrapperLayer() throws Exception {
        Path file = Paths.get("src/test/resources/address_book_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
             ColumnReader col = reader.columnReader("contacts.list.element.phoneNumber")) {

            assertThat(col.nextBatch()).isTrue();
            assertThat(col.getLayerCount()).isEqualTo(2);
            assertThat(col.getLayerKind(0)).isEqualTo(LayerKind.REPEATED);
            assertThat(col.getLayerKind(1)).isEqualTo(LayerKind.STRUCT);

            int[] listOffsets = col.getLayerOffsets(0);
            assertThat(listOffsets).hasSize(col.getRecordCount() + 1);
            // Record 0 has 2 contacts; record 1 has 0.
            assertThat(listOffsets[1] - listOffsets[0]).isEqualTo(2);
            assertThat(listOffsets[2] - listOffsets[1]).isEqualTo(0);

            Validity leafValidity = col.getLeafValidity();
            assertThat(leafValidity.hasNulls()).isTrue();
            // Contact 0 (Dmitriy): phoneNumber set.
            assertThat(leafValidity.isNotNull(listOffsets[0])).isTrue();
            // Contact 1 (Chris): phoneNumber null — this is the element-wrapper
            // correctness gate. The bit must report leaf-null independently of
            // any list-level encoding.
            assertThat(leafValidity.isNull(listOffsets[0] + 1)).isTrue();
        }
    }

    /// `optional_struct_optional_leaf_test.parquet` is the canonical depth-1
    /// #436 shape: `optional struct { optional int32 x }`. Three rows pin
    /// the disambiguation a single-bit `getElementNulls` could not provide:
    /// `getLayerValidity(0)` separates "struct null" from "struct present"
    /// while `getLeafValidity()` separates "leaf null" from "leaf present"
    /// inside a present struct.
    @Test
    void depth1OptionalStructDisambiguatesStructNullFromLeafNull() throws Exception {
        Path file = Paths.get("src/test/resources/optional_struct_optional_leaf_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
             ColumnReader col = reader.columnReader("point.x")) {

            assertThat(col.nextBatch()).isTrue();
            assertThat(col.getRecordCount()).isEqualTo(3);
            assertThat(col.getLayerCount()).isEqualTo(1);
            assertThat(col.getLayerKind(0)).isEqualTo(LayerKind.STRUCT);

            Validity structValidity = col.getLayerValidity(0);
            Validity leafValidity = col.getLeafValidity();
            assertThat(structValidity.hasNulls()).isTrue();
            assertThat(leafValidity.hasNulls()).isTrue();

            // Row 0: struct null. Both bits clear; struct-null reading takes
            // precedence — consumer checks struct first.
            assertThat(structValidity.isNull(0)).isTrue();
            assertThat(leafValidity.isNull(0)).isTrue();

            // Row 1: struct present, x null. Struct bit set, leaf bit clear.
            // This is the disambiguation #436 calls out: distinct from row 0.
            assertThat(structValidity.isNotNull(1)).isTrue();
            assertThat(leafValidity.isNull(1)).isTrue();

            // Row 2: both present.
            assertThat(structValidity.isNotNull(2)).isTrue();
            assertThat(leafValidity.isNotNull(2)).isTrue();
            assertThat(col.getInts()[2]).isEqualTo(42);
        }
    }

    /// `list_of_optional_struct_test.parquet` is the full element-wrapper
    /// gate: row 0 contains a list with three entries — one with a present
    /// leaf (`age=10`), one with a present struct but null leaf
    /// (`{age=null}`), and one with a null struct (`null`). The struct's
    /// per-item validity (`getLayerValidity(structLayer)`) and the leaf's
    /// validity (`getLeafValidity()`) must independently report the three
    /// states. An implementation that re-folded list-element struct
    /// nullability into the leaf would only fail here.
    @Test
    void listOfOptionalStructFullElementWrapperGate() throws Exception {
        Path file = Paths.get("src/test/resources/list_of_optional_struct_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
             ColumnReader col = reader.columnReader("items.list.element.age")) {

            assertThat(col.nextBatch()).isTrue();
            assertThat(col.getRecordCount()).isEqualTo(4);
            assertThat(col.getLayerCount()).isEqualTo(2);
            assertThat(col.getLayerKind(0)).isEqualTo(LayerKind.REPEATED);
            assertThat(col.getLayerKind(1)).isEqualTo(LayerKind.STRUCT);

            int[] listOffsets = col.getLayerOffsets(0);
            assertThat(listOffsets).hasSize(5);
            // Row 0: 3 items, Row 1: empty, Row 2: null, Row 3: 1 item.
            assertThat(listOffsets[1] - listOffsets[0]).isEqualTo(3);
            assertThat(listOffsets[2] - listOffsets[1]).isEqualTo(0);
            assertThat(listOffsets[3] - listOffsets[2]).isEqualTo(0);
            assertThat(listOffsets[4] - listOffsets[3]).isEqualTo(1);

            Validity listValidity = col.getLayerValidity(0);
            Validity structValidity = col.getLayerValidity(1);
            Validity leafValidity = col.getLeafValidity();
            assertThat(listValidity.hasNulls()).isTrue();
            assertThat(structValidity.hasNulls()).isTrue();
            assertThat(leafValidity.hasNulls()).isTrue();

            // Row 2 list is null; the others are present.
            assertThat(listValidity.isNotNull(0)).isTrue();
            assertThat(listValidity.isNotNull(1)).isTrue();
            assertThat(listValidity.isNull(2)).isTrue();
            assertThat(listValidity.isNotNull(3)).isTrue();

            // Row 0's three list items at layer-1 indices [0,1,2]:
            // - 0: leaf-present  → struct=present, leaf=present
            // - 1: leaf-null     → struct=present, leaf=clear
            // - 2: struct-null   → struct=clear,   leaf=clear
            assertThat(structValidity.isNotNull(0)).isTrue();
            assertThat(leafValidity.isNotNull(0)).isTrue();
            assertThat(structValidity.isNotNull(1)).isTrue();
            assertThat(leafValidity.isNull(1)).isTrue();
            assertThat(structValidity.isNull(2)).isTrue();
            assertThat(leafValidity.isNull(2)).isTrue();

            // Row 3's single item at layer-1 index [3]: leaf-present.
            assertThat(structValidity.isNotNull(3)).isTrue();
            assertThat(leafValidity.isNotNull(3)).isTrue();

            int[] ages = col.getInts();
            assertThat(ages[listOffsets[0]]).isEqualTo(10);
            assertThat(ages[listOffsets[3]]).isEqualTo(99);
        }
    }

    /// Negative-space check: the bytes buffer is documented as
    /// **capacity-sized**, not exact-sized — only `[0, offsets[valueCount])`
    /// is meaningful and any tail beyond is unspecified scratch. A test that
    /// silently tightens the contract back to exact-sized would break
    /// zero-copy downstream consumers; this asserts strict `>` is acceptable.
    @Test
    void capacitySizedBinaryBufferAllowsTrailingScratch() throws Exception {
        Path file = Paths.get("src/test/resources/list_basic_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
             ColumnReader col = reader.columnReader("tags.list.element")) {

            assertThat(col.nextBatch()).isTrue();
            byte[] bytes = col.getBinaryValues();
            int[] offsets = col.getBinaryOffsets();
            int valueCount = col.getValueCount();

            // Allowed: bytes.length may strictly exceed offsets[valueCount].
            assertThat(bytes.length).isGreaterThanOrEqualTo(offsets[valueCount]);
        }
    }

    /// `simple_map_test.parquet` has `attributes: map<string, int32>` with
    /// five rows: 3 entries / 2 entries / empty / null / 1 entry. Pins the
    /// key/value lockstep walk over shared parent offsets: both leaves
    /// report the same `getLayerOffsets(0)` and the same `getLayerValidity(0)`,
    /// so a consumer can iterate keys and values in step without coordinating
    /// state across the two readers.
    @Test
    void mapKeysAndValuesShareParentOffsetsAndValidity() throws Exception {
        Path file = Paths.get("src/test/resources/simple_map_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
             ColumnReader keys = reader.columnReader("attributes.key_value.key");
             ColumnReader values = reader.columnReader("attributes.key_value.value")) {

            assertThat(keys.nextBatch()).isTrue();
            assertThat(values.nextBatch()).isTrue();

            assertThat(keys.getRecordCount()).isEqualTo(5);
            assertThat(values.getRecordCount()).isEqualTo(5);
            assertThat(keys.getLayerCount()).isEqualTo(1);
            assertThat(keys.getLayerKind(0)).isEqualTo(LayerKind.REPEATED);
            assertThat(values.getLayerCount()).isEqualTo(1);
            assertThat(values.getLayerKind(0)).isEqualTo(LayerKind.REPEATED);

            int[] keyOffsets = keys.getLayerOffsets(0);
            int[] valueOffsets = values.getLayerOffsets(0);
            // The two readers walk the same parent map; their layer offsets
            // and the totals they imply must match.
            assertThat(keyOffsets).containsExactly(valueOffsets);
            assertThat(keys.getValueCount()).isEqualTo(values.getValueCount());

            // Row counts per state: 3 / 2 / 0 (empty) / 0 (null) / 1.
            assertThat(keyOffsets[1] - keyOffsets[0]).isEqualTo(3);
            assertThat(keyOffsets[2] - keyOffsets[1]).isEqualTo(2);
            assertThat(keyOffsets[3] - keyOffsets[2]).isEqualTo(0);
            assertThat(keyOffsets[4] - keyOffsets[3]).isEqualTo(0);
            assertThat(keyOffsets[5] - keyOffsets[4]).isEqualTo(1);

            Validity mapValidity = keys.getLayerValidity(0);
            assertThat(mapValidity.hasNulls()).isTrue();
            assertThat(mapValidity.isNotNull(0)).isTrue();
            assertThat(mapValidity.isNotNull(1)).isTrue();
            assertThat(mapValidity.isNotNull(2)).isTrue();   // empty, present
            assertThat(mapValidity.isNull(3)).isTrue();      // null map
            assertThat(mapValidity.isNotNull(4)).isTrue();

            // Lockstep read of (key, value) pairs for row 4 ("Eve" → single_key=42).
            byte[] keyBytes = keys.getBinaryValues();
            int[] keyBinOffsets = keys.getBinaryOffsets();
            int[] valueInts = values.getInts();

            int entry = keyOffsets[4];
            int kStart = keyBinOffsets[entry];
            int kLen = keyBinOffsets[entry + 1] - kStart;
            String key = new String(keyBytes, kStart, kLen, StandardCharsets.UTF_8);
            assertThat(key).isEqualTo("single_key");
            assertThat(valueInts[entry]).isEqualTo(42);
        }
    }
}
