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
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.function.UnaryOperator;

import dev.hardwood.InputFile;
import dev.hardwood.internal.reader.ParquetMetadataReader;
import dev.hardwood.metadata.FileMetaData;

/// Rewrites the footer of a well-formed file held in memory, for a test that needs a file
/// claiming something no writer here produces. The data pages and the page index are
/// untouched, so what changes is only what the footer says about them.
public final class FooterRewriter {

    private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.US_ASCII);

    private FooterRewriter() {
    }

    /// `file` with its footer replaced by `patch` applied to the footer it has.
    public static byte[] rewrite(byte[] file, UnaryOperator<FileMetaData> patch) throws IOException {
        FileMetaData metaData = ParquetMetadataReader.readMetadata(InputFile.of(ByteBuffer.wrap(file)));

        ThriftCompactWriter footer = new ThriftCompactWriter();
        FileMetaDataWriter.write(footer, patch.apply(metaData));
        byte[] footerBytes = footer.toByteArray();

        int dataLength = file.length - MAGIC.length - Integer.BYTES - footerLength(file);
        ByteBuffer rewritten = ByteBuffer
                .allocate(dataLength + footerBytes.length + Integer.BYTES + MAGIC.length)
                .order(ByteOrder.LITTLE_ENDIAN);
        rewritten.put(file, 0, dataLength);
        rewritten.put(footerBytes);
        rewritten.putInt(footerBytes.length);
        rewritten.put(MAGIC);
        return rewritten.array();
    }

    private static int footerLength(byte[] file) {
        return ByteBuffer.wrap(file, file.length - MAGIC.length - Integer.BYTES, Integer.BYTES)
                .order(ByteOrder.LITTLE_ENDIAN)
                .getInt();
    }
}
