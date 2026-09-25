/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive.internal;

import java.util.ArrayList;
import java.util.List;

import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Rect;
import dev.tamboui.text.Line;
import dev.tamboui.text.Span;
import dev.tamboui.text.Text;
import dev.tamboui.tui.event.KeyEvent;
import dev.tamboui.widgets.Clear;
import dev.tamboui.widgets.block.Block;
import dev.tamboui.widgets.block.BorderType;
import dev.tamboui.widgets.block.Borders;
import dev.tamboui.widgets.paragraph.Paragraph;

/// Navigation for a pane whose content is a document rather than a list of
/// rows: the value modals and the help and read-failure overlays. A facts
/// pane has a cursor and goes through [Document] instead. There is no cursor
/// here, so the three strides all move the viewport itself — `↑`/`↓` by a
/// line, `PgUp`/`PgDn` by a viewport, `g`/`G` to the ends.
///
/// Screens hold the scroll offset in their [ScreenState] and pass it back
/// here on every keypress; nothing is retained between calls. The offset is
/// clamped against the *current* content before it is adjusted, so content
/// that shrinks underneath a scrolled pane — collapsing a section, say —
/// cannot leave a stale offset that swallows the next few keypresses.
public final class ScrollPane {

    /// Returned by [#scroll(KeyEvent,int,int,int)] when the event is not one
    /// of the navigation keys, so the caller can let it fall through to the
    /// rest of its handler.
    public static final int UNHANDLED = -1;

    private ScrollPane() {
    }

    /// The new scroll offset after `event`, clamped to the content, or
    /// [#UNHANDLED] if `event` does not scroll. A navigation key that is
    /// already against its end returns the unchanged offset rather than
    /// [#UNHANDLED]: it is handled, it simply has nowhere to go.
    public static int scroll(KeyEvent event, int scroll, int totalLines, int viewport) {
        int max = maxScroll(totalLines, viewport);
        int current = Math.max(0, Math.min(scroll, max));
        if (Keys.isStepUp(event)) {
            return Math.max(0, current - 1);
        }
        if (Keys.isStepDown(event)) {
            return Math.min(max, current + 1);
        }
        if (Keys.isPageUp(event)) {
            return Math.max(0, current - viewport);
        }
        if (Keys.isPageDown(event)) {
            return Math.min(max, current + viewport);
        }
        if (Keys.isJumpTop(event)) {
            return 0;
        }
        if (Keys.isJumpBottom(event)) {
            return max;
        }
        return UNHANDLED;
    }

    /// The largest offset that still fills the viewport, or zero when the
    /// content fits.
    public static int maxScroll(int totalLines, int viewport) {
        return Math.max(0, totalLines - Math.max(1, viewport));
    }

    /// True when the content is taller than the viewport, and therefore when
    /// the navigation keys do anything at all. Keybars gate their scroll
    /// hints on this.
    public static boolean overflows(int totalLines, int viewport) {
        return totalLines > Math.max(1, viewport);
    }

    /// The slice of `lines` visible at `scroll`, with the offset clamped the
    /// same way [#scroll(KeyEvent,int,int,int)] clamps it so a stale offset
    /// renders the last full page instead of an empty pane.
    public static List<Line> window(List<Line> lines, int scroll, int viewport) {
        int max = maxScroll(lines.size(), viewport);
        int start = Math.max(0, Math.min(scroll, max));
        int end = Math.min(lines.size(), start + Math.max(1, viewport));
        return lines.subList(start, end);
    }

    /// A centred modal rect, capped at `maxWidth` by `maxHeight` and inset
    /// from the screen so the pane behind it stays visible at the edges.
    public static Rect modalArea(Rect screenArea, int maxWidth, int maxHeight) {
        int width = Math.min(maxWidth, Math.max(1, screenArea.width() - 4));
        int height = Math.min(maxHeight, Math.max(1, screenArea.height() - 2));
        return new Rect(screenArea.left() + (screenArea.width() - width) / 2,
                screenArea.top() + (screenArea.height() - height) / 2,
                width, height);
    }

    /// Content lines a modal of this size can show: its height less the two
    /// borders, the blank separator and the hint row.
    public static int modalViewport(Rect area) {
        return Math.max(1, area.height() - 4);
    }

    /// Columns a modal of this size can show, less its two borders and the
    /// one-cell inset every row is rendered with. Callers wrap their content
    /// to this before handing it over.
    public static int modalWidth(Rect area) {
        return Math.max(1, area.width() - 3);
    }

    /// Renders a scrollable modal: the visible slice of `content`, then a
    /// hint row stating how much is out of view and which keys move it.
    ///
    /// The modal is the only thing the navigation keys can address while it
    /// is open, so this also records its viewport as the key stride.
    public static void renderModal(Buffer buffer, Rect area, String title,
                                   List<Line> content, int scroll, String closeHint) {
        Clear.INSTANCE.render(area, buffer);
        int viewport = modalViewport(area);
        Keys.observeViewport(viewport);
        Keys.observeModalWidth(modalWidth(area));
        int max = maxScroll(content.size(), viewport);
        int start = Math.max(0, Math.min(scroll, max));
        List<Line> lines = new ArrayList<>(window(content, start, viewport));
        lines.add(Line.empty());
        lines.add(Line.from(new Span(hintRow(content.size(), viewport, start, closeHint), Theme.dim())));
        Paragraph.builder()
                .block(Block.builder()
                        .title(" " + title + " ")
                        .borders(Borders.ALL)
                        .borderType(BorderType.ROUNDED)
                        .build())
                .text(Text.from(lines))
                .left()
                .build()
                .render(area, buffer);
    }

    private static String hintRow(int totalLines, int viewport, int scroll, String closeHint) {
        if (!overflows(totalLines, viewport)) {
            return " " + closeHint;
        }
        int below = totalLines - Math.min(totalLines, scroll + viewport);
        String position = below > 0
                ? " ↓ " + below + " more lines"
                : " ↑ " + scroll + " lines above";
        return position + " · " + closeHint + " · " + hints(totalLines, viewport);
    }

    /// The keybar fragment for a scrollable pane, empty when the content
    /// fits. Screens append it to their own hints so the wording of the
    /// scroll keys is stated in one place.
    public static String hints(int totalLines, int viewport) {
        return new Keys.Hints()
                .add(overflows(totalLines, viewport), "[↑↓] scroll")
                .add(overflows(totalLines, viewport), "[PgDn/PgUp or Shift+↓↑] page")
                .add(overflows(totalLines, viewport), "[g/G] top/bottom")
                .build();
    }
}
