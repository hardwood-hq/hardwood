/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import dev.hardwood.row.StructAccessor;

/// Compiled, immutable representation of a [ResolvedPredicate] that evaluates
/// a single row in one virtual call.
///
/// A `RowMatcher` is built once per [dev.hardwood.reader.RowReader] by
/// [RecordFilterCompiler] and reused for every row. By holding all
/// invariant state (column indices, leaf names, struct paths, literal
/// operands, child arrays) in fields rather than recomputing per row, the
/// compiler eliminates the type and operator switches a generic
/// tree-walking evaluator would perform.
///
/// Each leaf type (one for every combination of value type and operator)
/// is its own concrete class — or factory-produced lambda — so that JIT
/// call sites see a single receiver type and can inline aggressively.
@FunctionalInterface
public interface RowMatcher {

    /// Returns true if the given row matches this predicate.
    boolean test(StructAccessor row);
}