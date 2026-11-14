/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.io.IOException;
import java.util.function.Predicate;

import dev.hardwood.internal.reader.batch.Dictionary;

/// Where an evaluation of a row group finds each column's dictionary, to prove a literal absent
/// from the column's chunk ([RowGroupFilterEvaluator#refineWithDictionaries]).
public interface DictionarySource {

    /// Whether `check` proves a literal absent from the column's dictionary; `false` where the
    /// column has none this source can use.
    boolean absent(int columnIndex, Predicate<Dictionary> check) throws IOException;
}
