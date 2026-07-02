/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

/// Outcome of inspecting a data page's repetition/definition level streams for
/// the fixed-size-list fast path.
///
/// A page is [FixedWidth] only when every row it carries is a present list of
/// exactly the same length — the shape produced by writing an
/// `Arrow FixedSizeList` / fixed-shape tensor through the standard 3-level
/// `LIST` encoding with no null rows and no null elements. Any deviation (nulls,
/// empty lists, varying lengths, an encoding the detector does not recognise)
/// yields [#NOT_APPLICABLE] and the caller must fall back to the regular
/// record-reconstruction path.
///
/// [#UNDETERMINED] is a third, internal-only outcome: it is the deferral signal
/// passed between [FixedSizeListDetector]'s repetition-level probes and never
/// escapes [FixedSizeListDetector#detect], which always resolves to [FixedWidth]
/// or [#NOT_APPLICABLE].
public sealed interface FixedSizeListShape
        permits FixedSizeListShape.FixedWidth, FixedSizeListShape.NotApplicable,
                FixedSizeListShape.Undetermined {

    /// Every present row has exactly `k` elements, verified from the level
    /// streams without reading vector interiors.
    ///
    /// @param k the fixed number of elements per row; always `>= 1`
    record FixedWidth(int k) implements FixedSizeListShape {
        public FixedWidth {
            if (k < 1) {
                throw new IllegalArgumentException(
                        "Fixed list length must be >= 1, but was " + k);
            }
        }
    }

    /// The page does not match the fixed-size-list fast-path invariants.
    record NotApplicable() implements FixedSizeListShape {}

    /// Shared singleton for the negative outcome; the result carries no state.
    FixedSizeListShape NOT_APPLICABLE = new NotApplicable();

    /// A repetition-level probe abstained: it does not recognise the stream, but a
    /// later probe might. Distinct from [#NOT_APPLICABLE], which stops the dispatch
    /// with a definitive rejection. Internal to [FixedSizeListDetector]; never
    /// returned by [FixedSizeListDetector#detect].
    record Undetermined() implements FixedSizeListShape {}

    /// Shared singleton for the deferral signal; the result carries no state.
    FixedSizeListShape UNDETERMINED = new Undetermined();
}
