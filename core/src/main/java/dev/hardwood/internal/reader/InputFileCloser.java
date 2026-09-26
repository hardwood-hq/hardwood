/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.util.List;

import dev.hardwood.InputFile;

/// Closes the input files a reader or iterator owns, attempting every file even
/// when an earlier one fails to close.
public final class InputFileCloser {

    private InputFileCloser() {
    }

    /// Closes every file in `files`, throws the first close failure and attaches
    /// the later ones to it as suppressed.
    public static void closeAll(List<? extends InputFile> files) throws IOException {
        Exception failure = closeAll(files, null);
        if (failure instanceof IOException e) {
            throw e;
        }
        if (failure != null) {
            throw (RuntimeException) failure;
        }
    }

    /// Attempts to close every file in `files`, skipping `null` entries. Each close
    /// failure is attached as suppressed to `failure`, or becomes the failure when
    /// `failure` is `null`.
    ///
    /// @return `failure`, or the first close failure when `failure` is `null`
    public static Exception closeAll(List<? extends InputFile> files, Exception failure) {
        Exception first = failure;
        for (InputFile file : files) {
            if (file == null) {
                continue;
            }
            try {
                file.close();
            }
            catch (IOException | RuntimeException closeException) {
                if (first == null) {
                    first = closeException;
                }
                else {
                    first.addSuppressed(closeException);
                }
            }
        }
        return first;
    }
}
