/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.thrift;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import dev.morling.hardwood.metadata.FileMetaData;
import dev.morling.hardwood.metadata.RowGroup;
import dev.morling.hardwood.metadata.SchemaElement;

/**
 * Reader for FileMetaData from Thrift Compact Protocol.
 */
public class FileMetaDataReader {

    public static FileMetaData read(ThriftCompactReader reader) throws IOException {
        reader.resetLastFieldId();

        int version = 0;
        List<SchemaElement> schema = new ArrayList<>();
        long numRows = 0;
        List<RowGroup> rowGroups = new ArrayList<>();
        String createdBy = null;

        while (true) {
            ThriftCompactReader.FieldHeader header = reader.readFieldHeader();
            if (header == null) {
                break;
            }

            switch (header.fieldId()) {
                case 1: // version
                    if (header.type() == 0x05) {
                        version = reader.readI32();
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 2: // schema
                    if (header.type() == 0x09) { // LIST
                        ThriftCompactReader.CollectionHeader listHeader = reader.readListHeader();
                        for (int i = 0; i < listHeader.size(); i++) {
                            schema.add(SchemaElementReader.read(reader));
                        }
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 3: // num_rows
                    if (header.type() == 0x06) {
                        numRows = reader.readI64();
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 4: // row_groups
                    if (header.type() == 0x09) {
                        ThriftCompactReader.CollectionHeader listHeader = reader.readListHeader();
                        for (int i = 0; i < listHeader.size(); i++) {
                            rowGroups.add(RowGroupReader.read(reader));
                        }
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 6: // created_by (optional)
                    if (header.type() == 0x08) {
                        createdBy = reader.readString();
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                default:
                    reader.skipField(header.type());
                    break;
            }
        }

        return new FileMetaData(version, schema, numRows, rowGroups, createdBy);
    }
}
