/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import dev.hardwood.internal.EncryptedFileException;
import dev.hardwood.internal.thrift.ThriftCompactConstants.FieldType.Codes;
import dev.hardwood.metadata.ColumnOrder;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.metadata.SchemaElement;

/// Reader for FileMetaData from Thrift Compact Protocol.
public class FileMetaDataReader {

    /// A footer as read, together with what [FileMetaData] cannot record: which leaf columns the
    /// footer annotated with a logical type this reader does not decode.
    public static final class ReadFooter {

        private final FileMetaData metaData;
        private final BitSet logicalTypeUnread;

        ReadFooter(FileMetaData metaData, BitSet logicalTypeUnread) {
            this.metaData = metaData;
            this.logicalTypeUnread = (BitSet) logicalTypeUnread.clone();
        }

        public FileMetaData metaData() {
            return metaData;
        }

        /// Whether the footer annotated the leaf column at `leafOrdinal` with a logical type this
        /// reader does not decode, which leaves the column read as its physical type.
        public boolean logicalTypeUnread(int leafOrdinal) {
            return logicalTypeUnread.get(leafOrdinal);
        }
    }

    /// Ids of the fields the format requires of a `FileMetaData`.
    private static final int[] REQUIRED_FIELDS = { 1, 2, 3, 4 };

    public static FileMetaData read(ThriftCompactReader reader) {
        return readFooter(reader).metaData();
    }

    public static ReadFooter readFooter(ThriftCompactReader reader) {
        int saved = reader.pushFieldIdContext(ThriftStruct.FILE_META_DATA);
        try {
            return readInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static ReadFooter readInternal(ThriftCompactReader reader) {
        int version = 0;
        List<SchemaElement> schema = Collections.emptyList();
        BitSet logicalTypeUnread = new BitSet();
        long numRows = 0;
        List<RowGroup> rowGroups = Collections.emptyList();
        Map<String, String> keyValueMetadata = Collections.emptyMap();
        String createdBy = null;
        List<ColumnOrder> columnOrders = Collections.emptyList();
        long seen = 0;

        while (true) {
            int header = reader.readFieldHeader();
            if (header == ThriftCompactReader.STOP_FIELD) {
                break;
            }

            int fieldId = ThriftCompactReader.fieldId(header);
            switch (fieldId) {
                // Fields 1-4 are required, so a wrong wire type fails here rather than being
                // reported as a field that never arrived.
                case 1: // version (required i32)
                    reader.requireField(header, Codes.I32);
                    version = reader.readI32();
                    seen |= 1L << fieldId;
                    break;
                case 2: { // schema (required list<SchemaElement>)
                    reader.requireField(header, Codes.LIST);
                    List<SchemaElementReader.ReadElement> elements =
                            reader.readStructList(SchemaElementReader::readElement);
                    schema = elements(elements);
                    logicalTypeUnread = logicalTypeUnread(elements);
                    seen |= 1L << fieldId;
                    break;
                }
                case 3: // num_rows (required i64)
                    reader.requireField(header, Codes.I64);
                    numRows = reader.readNonNegativeI64();
                    seen |= 1L << fieldId;
                    break;
                case 4: // row_groups (required list<RowGroup>)
                    reader.requireField(header, Codes.LIST);
                    rowGroups = reader.readStructList(RowGroupReader::read);
                    seen |= 1L << fieldId;
                    break;
                case 5: // key_value_metadata (optional list<KeyValue>)
                    if (reader.acceptField(header, Codes.LIST)) {
                        keyValueMetadata = KeyValueMetadataReader.read(reader);
                    }
                    break;
                case 6: // created_by (optional)
                    if (reader.acceptField(header, Codes.BINARY)) {
                        createdBy = reader.readString();
                    }
                    break;
                case 7: // column_orders (optional list<ColumnOrder>)
                    if (reader.acceptField(header, Codes.LIST)) {
                        columnOrders = readColumnOrders(reader);
                    }
                    break;
                case 8: // encryption_algorithm (present only with a plaintext footer)
                    // The footer parses, but the column data is encrypted and
                    // Hardwood cannot decrypt it. Fail fast rather than letting a
                    // later page scan crash with an unattributable error.
                    throw new EncryptedFileException();
                default:
                    reader.skipField(ThriftCompactReader.fieldType(header));
                    break;
            }
        }

        ThriftCompactReader.requireFields(ThriftStruct.FILE_META_DATA, seen, REQUIRED_FIELDS);

        return new ReadFooter(new FileMetaData(version, schema, numRows, rowGroups, keyValueMetadata,
                createdBy, columnOrders), logicalTypeUnread);
    }

    private static List<SchemaElement> elements(List<SchemaElementReader.ReadElement> elements) {
        List<SchemaElement> schema = new ArrayList<>(elements.size());
        for (SchemaElementReader.ReadElement element : elements) {
            schema.add(element.element());
        }
        return Collections.unmodifiableList(schema);
    }

    /// The leaf ordinals of the elements whose logical type went unread, counting primitive
    /// elements in schema order as `FileSchema` numbers its columns.
    private static BitSet logicalTypeUnread(List<SchemaElementReader.ReadElement> elements) {
        BitSet unread = new BitSet();
        int leafOrdinal = 0;
        for (SchemaElementReader.ReadElement element : elements) {
            if (!element.element().isPrimitive()) {
                continue;
            }
            if (element.logicalTypeUnread()) {
                unread.set(leafOrdinal);
            }
            leafOrdinal++;
        }
        return unread;
    }

    /// The column orders are optional and only refine how statistics are compared, so a list
    /// this reader will not decode leaves them empty — the same shape as a writer that omits
    /// the field.
    private static List<ColumnOrder> readColumnOrders(ThriftCompactReader reader) {
        long listHeader =
                reader.acceptListHeader(Codes.STRUCT);
        if (listHeader == ThriftCompactReader.ABSENT_LIST) {
            return List.of();
        }
        List<ColumnOrder> orders = new ArrayList<>(ThriftCompactReader.listSize(listHeader));
        for (int i = 0; i < ThriftCompactReader.listSize(listHeader); i++) {
            orders.add(ColumnOrderReader.read(reader));
        }
        return Collections.unmodifiableList(orders);
    }
}
