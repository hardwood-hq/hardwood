# Design: dive navigation and cursor model

**Status: Implemented.** Tracking issue: #1008.

## Goal

One navigation model across every pane of `hardwood dive`, so that a key
pressed on any screen does what it did on the previous screen, and so that
a reviewer has a rule to check new screens against.

Two rules cover it: one for what the keys move, one for what the markers
mean.

## Rule 1 — keys

`↑`/`↓`, `PgUp`/`PgDn` (aliased `Shift+↑`/`Shift+↓`) and `g`/`G` are **one
axis at three strides**: one unit, one viewport, all the way. They never act
on different things within the same pane.

What a "unit" is depends on the pane, and a pane is exactly one kind:

- **Cursor pane** — the unit is a row. State holds an index into the
  content; the scroll offset exists only to keep that index on screen. Every
  pane a reader can focus is one of these, whether or not `Enter` does
  anything in it: a cursor is where you are, not what you can do, and a pane
  worth reading is worth walking through. The list screens, the menus, the
  facts panes, the record modal.
- **Scroll pane** — the unit is a line. State holds an offset into the
  content; there is no cursor and `Enter` means nothing. The modals and the
  help overlay: an overlay is dismissed rather than navigated, so a position
  in it would not survive to be worth keeping.

The split is what a reader does with the pane, not what the content happens
to contain. A facts pane with nothing actionable still gets a cursor,
because `↑` should mean the same thing on it as on the pane beside it.

Where a screen has both kinds, `Tab` switches focus between them, so `↑`/`↓`
changes meaning only after an explicit move.

**The cursor stops on every row.** It never skips rows that `Enter` cannot
act on: a row that is not actionable is still worth reading, and skipping it
puts content out of reach of the only keys that reach content.

The cursor is a line index wherever a pane's rows are not uniform — the
footer body, the facts panes, the record modal — so the lines between the
ones `Enter` acts on are visited like any other, headings and blanks
included. Which lines are inert is a rendering detail, and a cursor that
knew about it would need the rule to carry an exception again.

> Every key moves the cursor. Only the size of the step varies.

## Rule 2 — markers

- **Colour marks the cursor.** The cursor row is styled `Theme.selection()`.
  That is the only signal of where the cursor is.
- **`▶` marks actionability.** It is drawn on the cursor row whenever that
  row is actionable, and on other rows only where it distinguishes them —
  that is, where the pane holds some row `Enter` cannot act on.

The cursor row's marker is orange like the rest of that row, since the
selection style covers the whole row. A pane whose rows are uniformly
actionable therefore shows a single caret travelling with the cursor rather
than a column of identical ones. Absence is only required to be unambiguous
on the cursor row, which is the only row `Enter` applies to, and there it
always is.

The marker occupies two cells, `"▶ "` or `"  "`, so text starts at the same
column whether or not a row is actionable.

The keybar stays consistent with this: `Keys.Hints` already lists `Enter`
only when it is live on the current row.

Together with Rule 1 this makes a non-actionable row legible rather than
merely reachable — the cursor can rest on it, and the missing `▶` says why
`Enter` does nothing.

`Schema` carries two markers in two columns, and they stay separate: the
actionability caret first, per the rule above, then the tree's `▶`/`▼`
marking whether a group is collapsed or expanded. Leaf rows leave the tree
column blank. Every schema row is actionable, so the first column holds a
single caret that travels with the cursor.

## Conformance

Panes already conforming: Row groups, Column chunks, Pages, Offset index,
Column index, Dictionary, File indexes, Row-group indexes,
Column-across-row-groups, Data preview table, Column chunk detail facts
pane, and the Overview and Row group detail menus.

The remaining work, in the order it should land:

### 1. Cursor panes whose viewport does not follow the cursor

Both move the cursor correctly and then render from row zero, so the cursor
leaves the screen and never comes back.

- **Schema** — `ScreenState.Schema` has no scroll offset. Add one and adopt
  `RowWindow.bottomPinned`, per
  [DIVE_LIST_VIEWPORT_VIRTUALIZATION.md](DIVE_LIST_VIEWPORT_VIRTUALIZATION.md);
  this is the only list screen still building rows for the whole collection.
- **Overview facts pane** — `ScreenState.Overview` has no scroll offset for
  the key/value list. Add one, window the pane, then add `PgUp`/`PgDn` and
  `g`/`G`, which only become meaningful once the offset exists.

### 2. Scroll panes missing strides

- **Pages header, Dictionary value and Column index min/max modals** —
  accept only `Esc` / `Enter`, so content past the modal viewport cannot be
  reached. The Dictionary modal additionally places the whole value in a
  single unwrapped line inside a fixed 16-row box, clipping anything wider
  than the modal; it exists to show values too long for the table row.

- **Row group detail facts pane** — no scrolling at all; content below the
  fold is unreachable on a short terminal. Add a scroll offset and mirror
  `ColumnChunkDetailScreen.scrollFacts`.
- **Key/value modal** — recognises only `Shift+↑`/`Shift+↓`, with a
  hard-coded stride of 10 and no `g`/`G`. Route through `Keys.isPageUp` /
  `Keys.isPageDown`, take the stride from `Keys.viewportStride()`, add
  `g`/`G`.
- **Help overlay** — silently truncated on short terminals. Either make it
  a scroll pane or guarantee it fits.

### 3. Cursor panes that skip rows

- **Record modal** — the cursor stops only on expandable fields and the body
  scrolls on a second axis to compensate. Restore a cursor that stops on
  every line, put `PgUp`/`PgDn` back on the cursor, add `g`/`G`. The modal
  geometry stays; the independent scroll offset becomes a slave of the
  cursor.
- **Column chunk detail "Drill into" menu** — the cursor skips disabled
  items. Let it rest on them; the absent `▶` tells the reader why `Enter`
  does nothing.

### 4. Footer

Replace the anchor cursor with a line cursor that stops on every line, so
the lines above the topmost anchor can be reached at all. The separate
scroll offset and the render-time reconciliation that fights it both go
away; opening the screen still puts the cursor on the first anchor.

### 5. Markers

Replace `.highlightSymbol("▶ ")` on the ten list screens, and the
hand-rolled equivalent in the Overview facts pane and the two "Drill into"
menus, with a marker driven by per-row actionability.

`Schema` replaces its `▸` cursor prefix with the standard `▶` caret in its
own column, ahead of the tree marker, and styles the whole cursor row
`Theme.selection()` rather than the name span alone.

The record modal reserves one cell for its marker where every other screen
reserves two, so its caret abuts the field name. Widening it to `"▶ "` /
`"  "` also shifts `continuationIndent` and `valueBudget`, which both encode
the one-cell width.

### 6. Shared mechanisms

Every pane that renders through the shared table path obeys both rules, and
every pane that deviates hand-rolls its own paragraph. The two rules should
therefore be two pieces of shared code, with no third path:

- **`CursorPane`** — the three strides over rows, the `RowWindow` slice, the
  actionability marker column and the three standard keybar hints. A screen
  supplies a row count, an actionability predicate, cell content, and the
  effect of `Enter`. `Footer` overrides the finest stride; nothing else
  needs to.
- **`ScrollPane`** — the three strides over lines, plus wrapping. Promoted
  out of `ColumnChunkDetailScreen.scrollFacts`, which is currently the only
  correct implementation of it and is private to one screen.

This is what the existing virtualization rule already demonstrates:
`RowWindow` is shared, every screen that uses it is correct, and `Schema`,
which does not, is the one that is broken.

### 7. Documentation

CLAUDE.md's Dive TUI section gains a pointer to this document, next to the
existing theme and virtualization rules.

The help overlay lists `PgDn`/`PgUp` under "Data preview", implying it is
specific to that screen when it works on every pane that scrolls. Move it to
"Navigation", document Footer's second axis, and list the key/value modal's
keys.

## Open decisions

None outstanding.
