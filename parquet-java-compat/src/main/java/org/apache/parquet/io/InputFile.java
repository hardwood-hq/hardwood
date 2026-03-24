/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.parquet.io;

import java.io.IOException;

/// Minimal shim for parquet-java's `org.apache.parquet.io.InputFile`.
///
/// In real parquet-java this interface also declares `newStream()`, but
/// Hardwood does not use seekable input streams — it uses range reads via
/// [dev.hardwood.InputFile]. This shim exists purely so that
/// `HadoopInputFile.fromPath()` returns the correct upstream type and
/// `ParquetReader.builder(ReadSupport, InputFile)` compiles.
public interface InputFile {

    /// Returns the total length of the file in bytes.
    ///
    /// @return the file length
    /// @throws IOException if obtaining the length fails
    long getLength() throws IOException;
}
