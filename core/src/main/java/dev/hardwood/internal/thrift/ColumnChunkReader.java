/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.io.IOException;
import java.nio.ByteBuffer;

import dev.hardwood.DecryptionKeyProvider;
import dev.hardwood.internal.reader.ParquetCryptoHelper;
import dev.hardwood.internal.reader.ParquetModuleType;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnCryptoMetaData;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.EncryptionAlgorithm;
import dev.hardwood.metadata.EncryptionStrategy;

/// Reader for ColumnChunk from Thrift Compact Protocol.
public class ColumnChunkReader {

    public static ColumnChunk read(ThriftCompactReader reader,
                                   DecryptionKeyProvider keyProvider,
                                   EncryptionAlgorithm encryptionAlgorithm,
                                   int rowGroupOrdinal,
                                   int columnOrdinal) throws IOException {
        short saved = reader.pushFieldIdContext();
        try {
            return readInternal(reader, keyProvider, encryptionAlgorithm, rowGroupOrdinal, columnOrdinal);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static ColumnChunk readInternal(ThriftCompactReader reader,
                                            DecryptionKeyProvider keyProvider,
                                            EncryptionAlgorithm encryptionAlgorithm,
                                            int rowGroupOrdinal,
                                            int columnOrdinal) throws IOException {
        ColumnMetaData metaData = null;
        Long offsetIndexOffset = null;
        Integer offsetIndexLength = null;
        Long columnIndexOffset = null;
        Integer columnIndexLength = null;
        ColumnCryptoMetaData columnCryptoMetaData = null;

        while (true) {
            ThriftCompactReader.FieldHeader header = reader.readFieldHeader();
            if (header == null) {
                break;
            }

            switch (header.fieldId()) {
                case 1: // file_path (optional string - deprecated)
                    reader.skipField(header.type());
                    break;
                case 2: // file_offset (required i64)
                    reader.skipField(header.type());
                    break;
                case 3: // meta_data (required)
                    if (header.type() == 0x0C) { // STRUCT
                        metaData = ColumnMetaDataReader.read(reader);
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 4: // offset_index_offset (optional i64)
                    if (header.type() == 0x06) {
                        offsetIndexOffset = reader.readI64();
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 5: // offset_index_length (optional i32)
                    if (header.type() == 0x05) {
                        offsetIndexLength = reader.readI32();
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 6: // column_index_offset (optional i64)
                    if (header.type() == 0x06) {
                        columnIndexOffset = reader.readI64();
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 7: // column_index_length (optional i32)
                    if (header.type() == 0x05) {
                        columnIndexLength = reader.readI32();
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 8: // column_crypto_metadata (optional struct)
                    if (header.type() == 0x0C) {
                        columnCryptoMetaData = ColumnCryptoMetaDataReader.read(reader);
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 9: // encrypted_column_metadata
                    // In PAR1 (plaintext footer) mode, the spec states that field 3 (meta_data)
                    // is always present for ALL columns, including encrypted ones, it's just
                    // stripped of stats for sensitive columns. Field 9 (encrypted_column_metadata)
                    // is optional addition containing full stats, available only to readers
                    // that hold the column key. As field 3 already provides everything required
                    // to read column data, we skip field 9 for PAR1.
                    // For PARE files, encryptionAlgorithm is passed in from
                    // FileCryptoMetaData (parsed before FileMetaData), so it is available here.
                    // In that case we decrypt field 9 to get the full ColumnMetaData.
                    // encryptionAlgorithm is null for PAR1 because FileMetaData field 8
                    // (encryption_algorithm) comes after field 4 (row_groups) in the Thrift stream
                    // and has not been parsed yet when ColumnChunkReader runs.
                    if (header.type() == 0x08 && encryptionAlgorithm != null) { // binary blob
                        byte[] encryptedMetaData = reader.readBinary();
                        byte[] keyMetadata = columnCryptoMetaData.columnKey().keyMetadata();
                        byte[] key = keyProvider.provideKey(keyMetadata);
                        EncryptionStrategy strategy = encryptionAlgorithm.activeEncryptionStrategy();
                        byte[] aad = ParquetCryptoHelper.buildPageAad(
                                strategy.aadPrefix(), strategy.aadFileUnique(),
                                ParquetModuleType.COLUMN_META, rowGroupOrdinal, columnOrdinal, -1);
                        ByteBuffer decrypted = ParquetCryptoHelper.decryptGcm(
                                ByteBuffer.wrap(encryptedMetaData), key, aad);
                        metaData = ColumnMetaDataReader.read(new ThriftCompactReader(decrypted));
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
        return new ColumnChunk(metaData, offsetIndexOffset, offsetIndexLength, columnIndexOffset, columnIndexLength, columnCryptoMetaData);
    }
}
