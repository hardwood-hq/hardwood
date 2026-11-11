/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive.internal;

import java.util.List;

import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Constraint;
import dev.tamboui.layout.Layout;
import dev.tamboui.layout.Rect;
import dev.tamboui.text.Line;
import dev.tamboui.text.Span;
import dev.tamboui.text.Text;
import dev.tamboui.widgets.paragraph.Paragraph;

/// The `⚠` line a screen shows where it uses an offset index its chunk or row group contradicts,
/// carrying the message a read fails with on it. A screen lists the index as the file stores it,
/// so the line is all that tells the reader a read would refuse it.
final class OffsetIndexFlag {

    private OffsetIndexFlag() {
    }

    /// The flag as a line, for a pane that lays out its own rows.
    static Line line(String violation) {
        return Line.from(new Span(" ⚠ " + violation, Theme.error()));
    }

    /// Renders the flag on the first line of `area`, above the pane it concerns, and returns the
    /// area left for that pane; returns `area` whole when `violation` is `null`. The flag sits
    /// outside the pane, so it is no row the pane's cursor stops on.
    static Rect above(Buffer buffer, Rect area, String violation) {
        if (violation == null) {
            return area;
        }
        List<Rect> split = Layout.vertical()
                .constraints(new Constraint.Length(1), new Constraint.Fill(1))
                .split(area);
        Paragraph.builder()
                .text(Text.from(line(violation)))
                .left()
                .build()
                .render(split.get(0), buffer);
        return split.get(1);
    }
}
