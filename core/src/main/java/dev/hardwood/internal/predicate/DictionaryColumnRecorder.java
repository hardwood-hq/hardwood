/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.util.BitSet;
import java.util.function.Predicate;

import dev.hardwood.internal.reader.batch.Dictionary;

/// A [DictionarySource] that reads nothing: it proves no literal absent and records which columns
/// the evaluator asked for.
///
/// Evaluating a row group with it through [RowGroupFilterEvaluator#refineWithDictionaries] yields
/// the columns whose dictionary a leaf may consult: since no dictionary decides a leaf here, no
/// `AND` or `OR` stops short of one a real evaluation could reach, so the recorded columns cover
/// every dictionary that evaluation reads.
public final class DictionaryColumnRecorder implements DictionarySource {

    private final BitSet columns = new BitSet();

    /// Records `columnIndex` and proves nothing absent.
    @Override
    public boolean absent(int columnIndex, Predicate<Dictionary> check) {
        if (columnIndex >= 0) {
            columns.set(columnIndex);
        }
        return false;
    }

    /// The file ordinals of the columns asked for.
    public BitSet columns() {
        return (BitSet) columns.clone();
    }
}
