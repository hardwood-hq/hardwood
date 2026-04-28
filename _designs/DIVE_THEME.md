# Design: `dive` theme and visual hierarchy (#355)

**Status: Implemented.**

## Goal

Visual rules for the `hardwood dive` TUI that are consistent across
screens and render correctly on the terminal palettes dive ships
against — Solarized Dark, Solarized Light, macOS Terminal, and any
terminal that honours either truecolor escapes or named ANSI. One
theme; no per-mode palette; no startup probe.

The rest of this document describes the rule set in five tiers and
the decision tree authors should follow when adding new content.

## Tiers

| # | Tier | Method | Visual |
|---|---|---|---|
| 1 | Selection | `Theme.selection()` | bold yellow |
| 2 | Structural caption | `Theme.accent().bold()` | bold blue |
| 3 | Label / "you-are-here" | `Theme.primary()` | bold default fg |
| 4 | Body content | `Style.EMPTY` (no method call) | terminal default fg |
| 5 | Persistent chrome | `Theme.dim()` | faint default fg |

Concrete styling on the four `Theme` methods:

| Method | Truecolor terminals | Named-ANSI terminals |
|---|---|---|
| `primary()` | `Style.EMPTY.bold()` | (same) |
| `accent()` | `Style.EMPTY.fg(rgb(38, 139, 210))` | `Style.EMPTY.fg(Color.BLUE)` |
| `selection()` | `Style.EMPTY.bold().fg(rgb(181, 137, 0))` | `Style.EMPTY.bold().fg(Color.YELLOW)` |
| `dim()` | `Style.EMPTY.dim()` | (same — uses ANSI faint attribute) |

Truecolor support is detected once at class-load via `$COLORTERM`
(`truecolor` or `24bit`). The boolean is fixed for the JVM lifetime;
`Theme` is otherwise stateless.

`accent()` and `selection()` use truecolor RGB pinned to Solarized's
accent slots when available so that iTerm2's "Use bright colors for
bold text" setting cannot remap them. `Color.BLUE` and `Color.YELLOW`
serve as the fallback when truecolor is unavailable; on those
terminals the bold-bright remap either does not occur or maps to
something readable.

`dim()` uses the ANSI faint attribute rather than a specific grey
colour. This avoids Solarized's named-grey traps (`Color.GRAY` →
`base2` cream, brighter than fg; `Color.DARK_GRAY` → `base03`, the
Solarized Dark background). Terminals that do not honour the faint
attribute render the text at default fg unchanged.

## Decision tree

When adding new content to a dive screen, walk these in order. The
first match wins.

1. **Is it the active row in a navigable list?** (table-row highlight,
   selected drill-into menu row, selected schema name, footer active
   anchor, modal cursor.) → `Theme.selection()`.

2. **Is it a structural caption?** (section heading inside a pane,
   table column header, app brand " hardwood dive ".) →
   `Theme.accent().bold()`.

3. **Is it a label, breadcrumb leaf, drill-into menu label (enabled,
   not selected), or interactive prompt?** (`/ ` search prompt, kv
   pane label, "you-are-here" indicator.) → `Theme.primary()`.

4. **Is it persistent chrome?** (keybar, modal hint bar, breadcrumb
   non-head segment, ` › ` separator, empty-state message,
   search-result count, parenthetical advisory, unfocused pane
   border.) → `Theme.dim()`.

5. **Else** — body content the user came to read (kv values, schema
   row name / type / logical / repetition / col-suffix, top-bar facts
   like filename and size, menu hint values, table data rows). →
   `Style.EMPTY` (no `Theme` call).

The decision tree is exhaustive. Any new dive content falls into
exactly one tier.

## Pane borders

Borders follow the same active / ambient distinction as text content
but use `borderStyle` rather than `borderColor`:

- **Active pane** (focused half of a multi-pane screen, or the only
  pane on a single-pane screen): no `borderStyle` set on the `Block`
  builder. Border lines render in the terminal's default fg.
- **Unfocused pane** in a multi-pane focus-tracking screen
  (`OverviewScreen`, `RowGroupDetailScreen`, `ColumnChunkDetailScreen`):
  `Block.builder()....borderStyle(Theme.dim())`. Faint default fg.

Pane borders deliberately do not use `accent()`. Bold blue is
reserved for textual structural anchors (section titles, table
headers, brand) so the eye reads it as "this is a heading-like
thing" rather than "this is decoration."

## Modal dimming

When a modal is open (help overlay, kv-meta detail, page-header
detail, record detail, dictionary value, min/max), the body behind
the modal is faded so the modal stands out and to signal that the
background is non-interactive.

Implementation: each modal-render path calls
`buffer.setStyle(area, Style.EMPTY.dim())` on the body area
immediately before the modal renders. `Buffer.setStyle` invokes
`Cell.patchStyle` per cell, which adds the faint modifier on top of
existing styling without overwriting the cell content or its other
modifiers. The modal's own `Clear.INSTANCE.render(modalArea, buffer)`
then fully replaces the cells inside the modal area, restoring full
intensity for the modal contents.

Chrome (top bar, breadcrumb, keybar) stays at full intensity. Only
the body region — the screen's main pane area passed to `render` —
is dimmed.

```java
// in a screen's render method, after the body is painted:
if (state.modalOpen()) {
    buffer.setStyle(area, Style.EMPTY.dim());
    renderXxxModal(buffer, area, ...);
}
```

A new modal added to dive should follow this pattern: paint the body
first, dim the body area, render the modal last. Skipping the dim
call leaves the background at full intensity behind the modal —
visually inconsistent with every other modal in the app.

## Convention recipes

### KV panes (label / value rows)

```java
new Span(padRight(label, width), Theme.primary()),
new Span(value, Style.EMPTY)
```

### Tables (Tamboui `Table` widget)

```java
Row header = Row.from(...).style(Theme.accent().bold());
Table.builder()
    .header(header)
    .highlightStyle(Theme.selection())
    ...
```

### Drill-into menu rows

| Row state | Cursor | Label | Hint |
|---|---|---|---|
| Enabled, not selected | `  ` | `Theme.primary()` | per screen |
| Enabled, selected | `▶ ` | `Theme.selection()` | per screen |
| Disabled | `  ` | `Theme.primary()` | hint shows `n/a` / `—` |

Hint styling depends on what the hint says:
- If the hint carries a fact (e.g. `4 pages`, `present`, `n/a` in
  `ColumnChunkDetailScreen`'s menu): default fg, treated as body
  content.
- If the hint has both a fact value and an annotation (e.g. `4 columns
  · browse by column` in `OverviewScreen`'s menu): split — value at
  default fg, ` · browse by …` suffix at `Theme.dim()`.

Disabled rows are signalled by the hint content (`n/a` / `—`), not by
a separate visual style. The cursor skips them on selection.

### Schema tree

- Header row (`Name`, `Type`, `Logical`, `Repetition`):
  `Theme.accent().bold()`.
- Body rows: name / type / logical / repetition / `[col N]` columns
  all at default fg.
- Selected name: `Theme.selection()`.
- Expand / collapse marker (`▼` / `▶`): `Theme.accent()` (no bold).

### Section delimiters within a pane

```java
new Span(" Compression ", Theme.accent().bold())
```

Used by `RowGroupDetailScreen` (Compression / Encoding mix / Page
indexes), `FooterScreen` (File layout / Encodings / Codecs / Page
indexes / Dictionary / Aggregate), `PagesScreen` modal (Inline
statistics), `OverviewScreen` ("key/value meta (N)"), and the help
overlay's section labels.

## `Theme` API

```java
public final class Theme {
    public static Style primary();
    public static Style accent();
    public static Style selection();
    public static Style dim();
}
```

All four return `Style`. Selection sites compose `.bold()` onto
`accent()` directly. The rare site that needs a `Color` (currently
none) extracts it via `.fg().orElseThrow()`.

There is no `init()` and no `--theme` flag. The truecolor probe runs
in a static initialiser; the values are immutable for the JVM's
lifetime.

## Testing

- `ThemeTest` pins each tone's structure (modifier presence, fg
  colour identity) so an accidental retargeting shows up in review.
- Manual verification on at least: iTerm2 + Solarized Dark, iTerm2
  + Solarized Light, macOS Terminal, one generic xterm-style
  terminal. Truecolor / named-ANSI behaviour is exercised by
  setting / unsetting `$COLORTERM`.

## Maintaining consistency

When adding new dive content:

- Style spans through one of the four `Theme` methods or leave them
  unstyled (`Style.EMPTY`). Direct use of `Color.*` constants or
  literal `Style.EMPTY.bold()` / `Style.EMPTY.fg(...)` etc. outside
  `Theme.java` is a smell — review against the decision tree before
  introducing any.
- Walk the [decision tree](#decision-tree) in order; the first match
  is the correct tier.
- New table widgets follow the [tables recipe](#tables-tamboui-table-widget).
  New kv blocks follow the [kv recipe](#kv-panes-label--value-rows).
  New panes that participate in focus tracking follow the [pane
  borders](#pane-borders) rule.
- `dim()` is for chrome; do not use it for content the user came to
  read. Conversely, body content (values, schema rows, top-bar
  facts) must not use `Theme.primary()` — primary is for labels and
  "you-are-here" markers, not for emphasising data.
