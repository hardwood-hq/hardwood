/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.parquet.hadoop.util;

import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.InputFile;

/**
 * Bridge between the compat-layer shim types and Hardwood internals.
 * <p>
 * This class lives in the same package as {@link HadoopInputFile} and
 * {@link FilterConverter} so it can access package-private members without
 * exposing Hardwood types on the public API.
 * </p>
 */
public final class InputFiles {

    private InputFiles() {
    }

    /**
     * Unwraps a compat-layer {@link InputFile} to the underlying
     * Hardwood {@link dev.hardwood.InputFile}.
     *
     * @param inputFile the compat-layer InputFile (must be a {@link HadoopInputFile})
     * @return the Hardwood InputFile
     * @throws UnsupportedOperationException if the InputFile is not a HadoopInputFile
     */
    public static dev.hardwood.InputFile unwrap(InputFile inputFile) {
        if (inputFile instanceof HadoopInputFile hadoopFile) {
            return hadoopFile.delegate();
        }
        throw new UnsupportedOperationException(
                "Unsupported InputFile implementation: " + inputFile.getClass().getName()
                        + ". Use HadoopInputFile.fromPath() to create InputFile instances.");
    }

    /**
     * Converts a compat-layer {@link FilterPredicate} to a Hardwood
     * {@link dev.hardwood.reader.FilterPredicate}.
     *
     * @param predicate the compat-layer filter predicate
     * @return the Hardwood filter predicate
     */
    public static dev.hardwood.reader.FilterPredicate convertFilter(FilterPredicate predicate) {
        return FilterConverter.convert(predicate);
    }
}
