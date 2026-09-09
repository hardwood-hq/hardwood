/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.reader.ParquetReadException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A parse failure should say which field of which struct it was standing on.
/// The struct is the reader's knowledge and the field id the raw reader's, and
/// neither is much use without the other.
class ThriftFieldNamingTest {

    /// `0x1f` is a field header for field 1 — delta 1 from the struct's start —
    /// declaring wire type 15, which is not a type. Field 1 of `PageHeader` is
    /// `type`, so the message can name it.
    @Test
    void namesAFieldTheStructDefines() {
        assertThatThrownBy(() -> PageHeaderReader.read(
                new ThriftCompactReader(ByteBuffer.wrap(new byte[] { 0x1f }))))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("PageHeader.type — Unknown field type: 15");
    }

    /// `0xff` is delta 15, which no struct in the format has room for. Saying
    /// so is stronger than naming a type: it means the bytes did not decode to
    /// anything that belongs there at all.
    @Test
    void saysWhenAFieldIdIsNotOneTheStructHas() {
        assertThatThrownBy(() -> PageHeaderReader.read(
                new ThriftCompactReader(ByteBuffer.wrap(new byte[] { (byte) 0xff }))))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("PageHeader field 15 — Unknown field type: 15");
    }

    /// The innermost reader is the one standing on the field; an outer struct's
    /// name against it would point at a field that is not the one that failed.
    @Test
    void theInnermostStructWins() {
        // field 5 of PageHeader is data_page_header, a struct; inside it, 0x1f
        // is field 1 — num_values — with an invalid type.
        assertThatThrownBy(() -> PageHeaderReader.read(
                new ThriftCompactReader(ByteBuffer.wrap(new byte[] { 0x5c, 0x1f }))))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("DataPageHeader.num_values — Unknown field type: 15");
    }

    /// A field id means one thing in the struct it was read from and something
    /// else in the struct enclosing it, so a reader may only name its own
    /// fields. `FileMetaData.key_value_metadata` holds `KeyValue` structs whose
    /// field 1 is `key`; field 1 of `FileMetaData` is `version`, and naming
    /// that would point at a field nowhere near the damage.
    @Test
    void anEnclosingStructDoesNotNameANestedStructsField() {
        // FileMetaData field 5, key_value_metadata: a one-element list of
        // structs whose first field header declares an invalid wire type.
        byte[] bytes = { 0x59, 0x1c, 0x1f };

        assertThatThrownBy(() -> FileMetaDataReader.read(
                new ThriftCompactReader(ByteBuffer.wrap(bytes))))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("KeyValue.key — Unknown field type: 15");
    }

    /// A struct nobody annotates gets no name rather than its container's. The
    /// message is then exactly what it was before struct naming existed, which
    /// is worse than a right name and far better than a wrong one.
    ///
    /// `DataPageHeaderV2.definition_levels_byte_length` is an `i32`; declared as a
    /// struct it fails [ThriftCompactReader#acceptField] and is walked past by
    /// [ThriftCompactReader#skipStruct], which does not know what it is walking.
    @Test
    void aStructNoReaderAnnotatesIsNotNamedAtAll() {
        // DataPageHeaderV2 field 5 declared a struct, so it is skipped wholesale;
        // inside it, a field header declaring an invalid wire type.
        byte[] bytes = { 0x5c, 0x1f };

        assertThatThrownBy(() -> DataPageHeaderV2Reader.read(
                new ThriftCompactReader(ByteBuffer.wrap(bytes))))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Unknown field type: 15");
    }

    /// Leaving a nested struct puts the enclosing one back, so a failure after it is
    /// named against the struct the reader has returned to and not the one it has
    /// just left. Field 13 is `encoding_stats` on `ColumnMetaData` and nothing at all
    /// on `Statistics`, so the two readings are told apart by the message.
    @Test
    void leavingANamedNestedStructRestoresTheEnclosingOne() {
        // ColumnMetaData field 12 (statistics) holding only a STOP, then field 13
        // with an invalid wire type.
        byte[] bytes = { (byte) 0xcc, 0x00, 0x1f };

        assertThatThrownBy(() -> ColumnMetaDataReader.read(
                new ThriftCompactReader(ByteBuffer.wrap(bytes))))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("ColumnMetaData.encoding_stats — Unknown field type: 15");
    }

    /// The same on the way out of an anonymous struct, which restores the enclosing
    /// struct rather than leaving the reader with none.
    @Test
    void leavingAnAnonymousNestedStructRestoresTheEnclosingOne() {
        // PageHeader field 6 (index_page_header) is a struct this reader skips
        // wholesale, entering it anonymously; field 7 then carries an invalid
        // wire type, and must be named against PageHeader rather than against
        // the struct just left.
        byte[] bytes = { 0x6c, 0x00, 0x1f };

        assertThatThrownBy(() -> PageHeaderReader.read(
                new ThriftCompactReader(ByteBuffer.wrap(bytes))))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("PageHeader.dictionary_page_header — Unknown field type: 15");
    }

    /// The structs a `LogicalType` variant holds are named too. `SchemaElement`'s
    /// `logicalType` is in nearly every modern footer, so a `DECIMAL` or `TIMESTAMP`
    /// annotation is where an ordinary corrupt footer lands.
    @Test
    void aVariantsOwnStructIsNamed() {
        // LogicalType field 5, DECIMAL, holding a DecimalType whose field 1 —
        // scale — declares an invalid wire type.
        byte[] bytes = { 0x5c, 0x1f };

        assertThatThrownBy(() -> LogicalTypeReader.read(
                new ThriftCompactReader(ByteBuffer.wrap(bytes))))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("DecimalType.scale — Unknown field type: 15");
    }

    /// A declared length that is not a length is as ordinary a corruption as an
    /// invalid wire type, and is named the same way.
    @Test
    void aBinaryLengthThatIsNotALengthNamesItsField() {
        // SchemaElement field 4, name, a binary whose length varint is the ten-byte
        // form of an all-ones 64-bit value — which is -1, and so not a length.
        byte[] bytes = { 0x48,
                (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
                (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x01 };

        assertThatThrownBy(() -> SchemaElementReader.read(
                new ThriftCompactReader(ByteBuffer.wrap(bytes))))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("SchemaElement.name — binary value declares -1 bytes,"
                        + " which is not a length");
    }

    /// `LogicalType` is a union, so its ids name variants. Field 1 is `STRING`;
    /// `scale` is a field of the `DecimalType` the union's field 5 holds, and
    /// resolving one against the other named a field of a type the file never
    /// claimed to be.
    @Test
    void aUnionsFieldsAreItsVariants() {
        assertThatThrownBy(() -> LogicalTypeReader.read(
                new ThriftCompactReader(ByteBuffer.wrap(new byte[] { 0x1f }))))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("LogicalType.STRING — Unknown field type: 15");
    }

    /// The second form of the name asserts the format has no such field, so it
    /// must not appear for one the format defines. `RowGroup.sorting_columns`
    /// is field 4 and this reader skips it, which is not the same as the format
    /// not having it.
    @Test
    void aFieldTheFormatDefinesIsNamedEvenWhenTheReaderSkipsIt() {
        assertThatThrownBy(() -> RowGroupReader.read(
                new ThriftCompactReader(ByteBuffer.wrap(new byte[] { 0x4f }))))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("RowGroup.sorting_columns — Unknown field type: 15");
    }

    /// A field id past everything the format defines keeps the second form,
    /// which is the stronger statement it was always meant to be.
    @Test
    void aFieldIdTheFormatDoesNotDefineKeepsTheBareForm() {
        assertThatThrownBy(() -> RowGroupReader.read(
                new ThriftCompactReader(ByteBuffer.wrap(new byte[] { (byte) 0x8f }))))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("RowGroup field 8 — Unknown field type: 15");
    }

    /// A reader's own validation is named the same way a parse failure is: the
    /// reader is standing on the field it rejected, so nothing states the name.
    @Test
    void namesTheFieldAReadersOwnValidationRejects() {
        // numBytes = -5, which is an i32 the format allows nowhere.
        assertThatThrownBy(() -> BloomFilterHeaderReader.read(
                new ThriftCompactReader(ByteBuffer.wrap(new byte[] { 0x15, 0x09, 0x00 }))))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("BloomFilterHeader.numBytes — must be non-negative but was -5");
    }

    /// An *optional* field whose wire type disagrees is skipped, and nothing
    /// names it because nothing failed: the file loses that field and the rest
    /// of the struct reads. A required one fails where it is instead, so
    /// "missing" is never said of a field that arrived.
    @Test
    void saysNothingAboutAnOptionalFieldItSkipped() {
        // A valid PageHeader whose optional crc declares binary, not i32.
        byte[] bytes = { 0x15, 0x00, 0x15, 0x14, 0x15, 0x10, 0x18, 0x00, 0x00 };

        PageHeader header = PageHeaderReader.read(new ThriftCompactReader(ByteBuffer.wrap(bytes)));
        assertThat(header.crc()).isNull();
        assertThat(header.uncompressedPageSize()).isEqualTo(10);
    }

    /// A required field whose wire type disagrees fails at the field, naming it
    /// and both types — never at the STOP as a field that never arrived.
    @Test
    void namesARequiredFieldWhoseWireTypeDisagrees() {
        // numBytes carrying a bool where the format gives it an i32.
        assertThatThrownBy(() -> BloomFilterHeaderReader.read(
                new ThriftCompactReader(ByteBuffer.wrap(new byte[] { 0x11, 0x00 }))))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("BloomFilterHeader.numBytes — wrong Thrift wire type 0x1 (expected 0x5)");
    }

    /// A complaint about the struct rather than about a field of it names no
    /// field, and names every field that is genuinely absent in one sentence.
    @Test
    void namesNoFieldForAComplaintAboutTheStruct() {
        assertThatThrownBy(() -> BloomFilterHeaderReader.read(
                new ThriftCompactReader(ByteBuffer.wrap(new byte[] { 0x00 }))))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("BloomFilterHeader is missing required fields: numBytes, algorithm,"
                        + " hash, compression");
    }

    /// A union variant's value is the empty struct the format gives it, and that
    /// value is required, so a wire type that disagrees is rejected where it is
    /// rather than skipped by a type the bytes do not hold.
    @Test
    void rejectsAUnionVariantWhoseValueIsNotAStruct() {
        // BloomFilterHeader.algorithm holding a union whose one variant declares i32.
        byte[] bytes = { 0x15, 0x40, 0x1c, 0x15, 0x00, 0x00, 0x00 };

        assertThatThrownBy(() -> BloomFilterHeaderReader.read(
                new ThriftCompactReader(ByteBuffer.wrap(bytes))))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("BloomFilterAlgorithm.BLOCK — wrong Thrift wire type 0x5 (expected 0xc)");
    }

    /// A variant id the union does not define names a file this version of Hardwood
    /// cannot evaluate rather than an invalid one — the same file, read by a version
    /// that defines the variant, is correct — so it fails as unsupported rather than
    /// as unparseable, still named against the union rather than as an out-of-range
    /// argument. The type is [UnsupportedOperationException] itself: a caller acts no
    /// differently for this than for any other file this version cannot read, and the
    /// message already names the variant.
    @Test
    void rejectsAVariantIdTheUnionDoesNotDefine() {
        // BloomFilterHeader.algorithm holding a union whose variant is id 2.
        byte[] bytes = { 0x15, 0x40, 0x1c, 0x2c, 0x00, 0x00, 0x00 };

        assertThatThrownBy(() -> BloomFilterHeaderReader.read(
                new ThriftCompactReader(ByteBuffer.wrap(bytes))))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("BloomFilterAlgorithm field 2 is not a bloom filter algorithm "
                        + "this version implements");
    }

    /// The union names itself from [ThriftStruct] rather than from a string a
    /// caller passes, so it cannot disagree with the name its ids resolve against.
    @Test
    void namesTheUnionWhenNoVariantIsSet() {
        assertThatThrownBy(() -> BloomFilterHeaderReader.read(new ThriftCompactReader(
                ByteBuffer.wrap(new byte[] { 0x15, 0x40, 0x1c, 0x00, 0x00 }))))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("BloomFilterAlgorithm has no variant set");
    }
}
