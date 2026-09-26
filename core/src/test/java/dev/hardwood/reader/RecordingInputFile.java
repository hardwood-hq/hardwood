/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import dev.hardwood.InputFile;

/// Serves fixed bytes, records whether it was closed, and optionally fails to
/// open or to close.
final class RecordingInputFile implements InputFile {

    private static final byte[] NOT_PARQUET = "this is not a parquet file".getBytes(StandardCharsets.US_ASCII);

    private final String name;
    private final ByteBuffer data;
    private final IOException openFailure;
    private final Exception closeFailure;
    boolean closed;

    RecordingInputFile(String name, byte[] data, IOException openFailure, Exception closeFailure) {
        if (closeFailure != null && !(closeFailure instanceof IOException) && !(closeFailure instanceof RuntimeException)) {
            throw new IllegalArgumentException("closeFailure must be an IOException or a RuntimeException");
        }
        this.name = name;
        this.data = ByteBuffer.wrap(data);
        this.openFailure = openFailure;
        this.closeFailure = closeFailure;
    }

    /// A file whose bytes are not Parquet, so reading its footer fails.
    RecordingInputFile(String name, IOException openFailure, IOException closeFailure) {
        this(name, NOT_PARQUET, openFailure, closeFailure);
    }

    static RecordingInputFile notParquet(String name) {
        return new RecordingInputFile(name, null, null);
    }

    @Override
    public void open() throws IOException {
        if (openFailure != null) {
            throw openFailure;
        }
    }

    @Override
    public ByteBuffer readRange(long offset, int length) {
        return data.slice(Math.toIntExact(offset), length);
    }

    @Override
    public long length() {
        return data.capacity();
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void close() throws IOException {
        closed = true;
        if (closeFailure instanceof IOException e) {
            throw e;
        }
        if (closeFailure instanceof RuntimeException e) {
            throw e;
        }
    }
}
