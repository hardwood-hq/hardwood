/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive.internal;

import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

import dev.hardwood.cli.internal.Strings;
import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Rect;
import dev.tamboui.text.Line;
import dev.tamboui.text.Span;

/// What a screen shows instead of itself when the file will not give up what it
/// was asked for.
///
/// The message is the reader's, which by the time it reaches here names the
/// file, the row group, the column and the page. This adds no wording of its own
/// beyond the hint: a sentence invented here would be a second, vaguer account
/// of something already described precisely.
public final class ReadFailureOverlay {

    private static final int WIDTH = 60;

    private ReadFailureOverlay() {
    }

    /// The text for `e`, or its simple class name when it has none so the box
    /// is never blank.
    ///
    /// `new UncheckedIOException(cause)` reports the cause's `toString()` as its
    /// own message, which would put a fully qualified class name in front of the
    /// reader; that bare form is unwrapped. A wrapper built with a message of
    /// its own is kept — that is the one carrying the read context.
    public static String messageOf(Throwable e) {
        Throwable reported = e;
        if (e instanceof UncheckedIOException && e.getCause() != null
                && e.getCause().toString().equals(e.getMessage())) {
            reported = e.getCause();
        }
        String message = reported.getMessage();
        return message == null || message.isBlank()
                ? reported.getClass().getSimpleName()
                : message;
    }

    /// Lines the overlay would show at the width the last frame wrapped to, so
    /// the key handler and the renderer agree on how far it can scroll.
    public static int lineCount(String message) {
        return lines(message, Keys.modalWidth()).size();
    }

    public static void render(Buffer buffer, Rect screenArea, String message, int scroll) {
        Rect widthProbe = ScrollPane.modalArea(screenArea, WIDTH, screenArea.height());
        List<Line> lines = lines(message, ScrollPane.modalWidth(widthProbe));
        Rect area = ScrollPane.modalArea(screenArea, WIDTH, lines.size() + 4);
        ScrollPane.renderModal(buffer, area, "Read failed", lines, scroll, "[Esc] back");
    }

    private static List<Line> lines(String message, int width) {
        List<Line> lines = new ArrayList<>();
        // One column narrower than the box allows, so the inset matches on both
        // sides — wrapping to the full width insets the left and lets the text
        // touch the right border.
        for (String chunk : Strings.wordWrap(message, Math.max(1, width - 1))) {
            lines.add(Line.from(Span.raw(" " + chunk)));
        }
        return lines;
    }
}
