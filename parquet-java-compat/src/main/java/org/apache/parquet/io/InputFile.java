/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.parquet.io;

import java.io.IOException;

/**
 * Minimal shim for parquet-java's {@code org.apache.parquet.io.InputFile}.
 * <p>
 * In real parquet-java this interface also declares {@code newStream()}, but
 * Hardwood does not use seekable input streams — it uses range reads via
 * {@link dev.hardwood.InputFile}. This shim exists purely so that
 * {@code HadoopInputFile.fromPath()} returns the correct upstream type and
 * {@code ParquetReader.builder(ReadSupport, InputFile)} compiles.
 * </p>
 */
public interface InputFile {

    /**
     * Returns the total length of the file in bytes.
     *
     * @return the file length
     * @throws IOException if obtaining the length fails
     */
    long getLength() throws IOException;
}
