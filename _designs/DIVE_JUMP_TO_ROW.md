# Design: jumping to a row or row group in dive

**Status: Implemented.** Tracking issue: #1101. Related: #1100.

## Goal

A reader looking at row group 17 on the Row groups screen — suspicious
statistics, an odd encoding — should be able to see the values behind it, and a
reader who knows the record they want should be able to name it. Navigation in
the Data preview is relative only, so today the route to a specific record is
holding `PgDn`, and the route to a row group's first record is adding up
`numRows()` by hand.

The same vocabulary reaches the non-interactive side, so that what a reader
finds in dive is reproducible on the command line.

## Row ↔ row group mapping

`ParquetModel` owns the mapping in both directions, over one prefix array of
cumulative `RowGroup.numRows()` built when the model opens:

- `long firstRowOf(int rowGroupIndex)` — the absolute index of a row group's
  first row. Drives the jump.
- `int rowGroupOf(long row)` — the row group an absolute row falls in, by binary
  search over the same array. Drives the Data preview's row-group indicator
  (#1100), which asks once per keystroke.

Both reject an out-of-range argument rather than clamping: a jump that silently
lands somewhere else is worse than one that does not happen.

## Jumping within the Data preview

`:` opens a prompt on the Data preview. It accepts either a record — `12345` —
or a row group — `rg 17`. `Enter` seeks so that the target is the selected row,
`Esc` closes the prompt and leaves the view where it was.

`:` rather than `/`, because `/` is inline search on the Schema, Column index
and Dictionary screens, and a Data preview that answers `/` with a jump prompt
would make the key mean two different things one screen apart. `/` stays
reserved for a value search over the preview.

Both numbers are counted from zero, as the rest of the tool counts them:
`hardwood inspect columns --row-group 0` names the first row group, and
`hardwood print --row-index` numbers the first record 0. A row typed into the
prompt is therefore the row `--skip` takes, which is what makes a position
found in dive reproducible on the command line.

The Data preview's title counts from one — `rows 1-20 of 300` — so a reader
copying a number off the title lands one row past the record they pointed at.
Aligning the title on zero is a change to what every dive reader already sees
and is left to #1100, which revisits that line to name the current row group.

Input out of range for the file — a row past the last row, a row group past the
last row group — is reported in the prompt, which stays open with the text
intact. Nothing is clamped and no seek happens.

The prompt is a centred bordered box over the table, the shape `HelpOverlay`,
`ReadFailureOverlay` and the Data preview's own record modal use. It carries
its own keys, so the keybar stands down while it is open, and it costs the page
behind it no rows.

The seek itself goes through the Data preview's existing absolute-position move,
so a jump is the same operation as `G` with a different target, and the
navigation model's two rules (`_designs/DIVE_NAVIGATION_MODEL.md`) hold
unchanged: the prompt is a mode, not a new kind of cursor.

`PreviewWindow` needs no invalidation path. Its buffer covers a range of
absolute row indices and is fully replaced whenever the requested page falls
outside that range, so an arbitrary seek either lands in a buffer that is still
correct for it or triggers a refill.

## Jumping from the Row groups screens

`d` on `RowGroupsScreen` and `RowGroupDetailScreen` opens the Data preview at
the first row of the selected row group. This is the common case and takes no
typing.

`Enter` on those screens keeps its meaning — drilling into the row group's
detail and its column chunks — so the `▶` marker continues to mark what `Enter`
acts on.

## Command-line parity

`print` and `convert` gain `--skip <n>` and `--row-group <i>`:

- `--skip <n>` starts at row `n` instead of row 0, and composes with the
  existing `--rows/-n`.
- `--row-group <i>` prints exactly the rows of row group `i`, resolved through
  `firstRowOf`.

The two are mutually exclusive, and both are rejected before the read when out
of range or negative — the CLI fails before it reads, as the interactive prompt
refuses before it seeks. Neither combines with a negative `-n`, which counts
from the end of the file and so has no starting point to move. `print
--row-index` numbers rows by their position in the file, so a row reached with
`--skip` carries the number it had in `dive`.

Documented under `docs/content/reference/cli.md` alongside the existing `print`
options, and in `skills/hardwood-cli/`.

## Open decisions

None outstanding.
