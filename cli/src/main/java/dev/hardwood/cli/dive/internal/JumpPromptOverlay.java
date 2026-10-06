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
import dev.tamboui.widgets.Clear;
import dev.tamboui.widgets.block.Block;
import dev.tamboui.widgets.block.BorderType;
import dev.tamboui.widgets.block.Borders;
import dev.tamboui.widgets.paragraph.Paragraph;

/// The `:` prompt: a centred bordered box over the active screen, the same
/// shape [HelpOverlay] and [ReadFailureOverlay] use. `unit` names what the
/// typed number counts on the screen the prompt was opened from — "row" on
/// the Data preview, "row group" on Row groups — and appears in the hint
/// shown before anything has been typed.
public final class JumpPromptOverlay {

    /// Wide enough for the longest refusal it prints, tall enough for the
    /// input, the hint and the keys, plus two borders.
    private static final int WIDTH = 48;
    private static final int HEIGHT = 6;

    private JumpPromptOverlay() {
    }

    public static void render(Buffer buffer, Rect screenArea, JumpPrompt prompt, String unit) {
        int width = Math.min(WIDTH, Math.max(1, screenArea.width() - 4));
        int height = Math.min(HEIGHT, Math.max(1, screenArea.height()));
        Rect area = new Rect(
                screenArea.left() + (screenArea.width() - width) / 2,
                screenArea.top() + (screenArea.height() - height) / 2,
                width, height);
        Clear.INSTANCE.render(area, buffer);

        List<Line> lines = new ArrayList<>();
        lines.add(Line.from(
                new Span(" : ", Theme.primary()),
                new Span(prompt.input() + "█", Theme.primary())));
        lines.add(Line.empty());
        lines.add(prompt.error() == null
                ? Line.from(new Span(" a " + unit, Theme.dim()))
                : Line.from(new Span(" " + prompt.error(), Theme.accent())));
        lines.add(Line.from(new Span(" Enter go · Esc cancel", Theme.dim())));
        Paragraph.builder()
                .block(Block.builder()
                        .title(" Jump to ")
                        .borders(Borders.ALL)
                        .borderType(BorderType.ROUNDED)
                        .build())
                .text(Text.from(lines))
                .left()
                .build()
                .render(area, buffer);
    }
}
