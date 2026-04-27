<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# CLI

The `hardwood` CLI lets you inspect and convert Parquet files from the command line — useful for exploring datasets, debugging file structure, and quick format conversions without writing Java code. It reads local files and S3 URIs, and ships as a GraalVM native binary with instant startup.

Pre-built native binaries for Linux, macOS, and Windows are available from the [early-access release](https://github.com/hardwood-hq/hardwood/releases/tag/1.0-early-access).

!!! note "macOS"
    The binary is not notarized. On first run, macOS Gatekeeper will block it. Remove the quarantine flag after extracting:

    ```shell
    xattr -r -d com.apple.quarantine hardwood-cli-*/
    ```

## Available Commands

| Command | Description |
|---------|-------------|
| `hardwood info` | Display high-level file information |
| `hardwood schema` | Print the file schema, including logical-type annotations such as `VARIANT(1)` on Variant groups |
| `hardwood print` | Print rows as an ASCII table (head, tail, or all); Variant columns are decoded to JSON-like text |
| `hardwood convert` | Convert a Parquet file to CSV or JSON; Variant columns are emitted as a JSON string in CSV and as a native JSON subtree in JSON |
| `hardwood footer` | Print decoded footer length, offset, and file structure |
| `hardwood inspect pages` | List data and dictionary pages per column chunk; includes per-page min/max when the file has a page index |
| `hardwood inspect dictionary` | Print dictionary entries for a column |
| `hardwood inspect columns` | Show compressed and uncompressed byte sizes per column, ranked |
| `hardwood inspect rowgroups` | Display per-row-group column chunk metadata (sizes, codec) |
| `hardwood dive` | Interactively explore a file's structure in a TUI |
| `hardwood help` | Display help information about a command |

## Examples

```shell
# Show file overview
hardwood info -f data.parquet

# Print schema
hardwood schema -f data.parquet

# Show first 20 rows
hardwood print -n 20 -f data.parquet

# Show last 5 rows
hardwood print -n -5 -f data.parquet

# Show all rows
hardwood print -f data.parquet

# Convert to CSV
hardwood convert --format csv -f data.parquet

# Show dictionary entries for a column (first 50 entries per row group by default)
hardwood inspect dictionary -f data.parquet -c category

# Show all dictionary entries for a column
hardwood inspect dictionary -f data.parquet -c category --limit 0
```

## Interactive exploration (`dive`)

`hardwood dive` launches a terminal UI for navigating a Parquet file's structure
without re-invoking the CLI for each slice:

```shell
hardwood dive -f data.parquet
```

From the Overview landing screen, drill into **Schema** or **Row groups**,
then into column chunks and per-chunk metadata.

### What you can do with it

`dive` composes the slices that the batch subcommands (`info`, `schema`,
`footer`, `inspect`, `print`) each surface separately into a single
navigable session. Typical things to reach for it for:

- **Find a column quickly** in a wide schema — Schema screen, `/` to
  filter the tree to leaves matching a substring.
- **Spot the heavy column chunks** in a row group — Row groups → Column
  chunks ranks by compressed size with the codec and dictionary flag
  alongside.
- **Check page-level statistics and indexes** — drill from a chunk into
  Pages, Column index, or Offset index; `Enter` on a page opens the
  full thrift header, including inline statistics when no Column Index
  is present.
- **Inspect dictionary entries** for a column — Dictionary screen with
  `/` substring filter; `Enter` reveals the full untruncated value of
  the highlighted entry.
- **Preview a few rows** without exporting — Data preview paginates with
  `PgDn`/`PgUp` (`g`/`G` for first/last); `Enter` opens a per-row modal
  where each field can be expanded inline.
- **Decode key/value metadata** — Spark JSON schemas pretty-print, Arrow
  IPC schemas decode to a hex dump.
- **Compare a column across row groups** — from Schema, `Enter` on a
  leaf jumps to a one-row-per-RG view of that column's sizes,
  encodings, and stats.
- **Read raw file layout** — Footer & indexes shows file size, footer
  offset, encoding/codec histograms, page-index coverage, and aggregate
  byte breakdowns; from there you can drill into a file-wide list of
  every chunk's column index, offset index, or dictionary region.

### Keys

| Key | Action |
|-----|--------|
| `↑` / `↓` | Move selection |
| `PgDn` / `PgUp` (or `Shift-↓` / `Shift-↑`) | Page down / up |
| `g` / `G` | Jump to first / last row |
| `Enter` | Drill into the selected item |
| `Esc` / `Backspace` | Go back one level |
| `Tab` / `Shift-Tab` | Switch focused pane |
| `/` | Inline search (Schema, Column index, Dictionary) |
| `t` | Toggle logical / physical value rendering (screen-specific: Pages, Column index, Dictionary, Data preview, Column chunk detail) |
| `e` / `c` | Expand / collapse all (Schema tree; Data preview row modal) |
| `o` | Jump back to Overview |
| `?` | Toggle help overlay |
| `q` / `Ctrl-C` | Quit |

The keybar at the bottom of every screen lists the keys that are
actually meaningful in the current context — so the menus above show the
full vocabulary, but the keybar tells you which subset is live right
now.

Available screens: Overview, Schema (expandable tree of groups + leaves),
Row groups, Column chunks, Column chunk detail (facts + drill menu), Pages
(with a page-header modal on Enter), Column index, Offset index, Footer &
indexes (which also drills into a file-wide list of every chunk's column
index, offset index, or dictionary region),
Column-across-row-groups (from the Schema screen), Dictionary (with
full-value modal on Enter and `/` inline search), and Data preview (row
values via `RowReader`; `←/→` scrolls the visible column window,
`PgDn/PgUp` flips pages).

The `--rows N` flag sets the Data preview page size (default: 20).

### Layout

Every screen shares a four-region layout — a top bar with file identity, a
breadcrumb showing the navigation stack, the active screen body, and a keybar:

```
 hardwood dive │ /data/lineitem.parquet │ 1.4 GB │ 3 RGs │ 12.4 M rows
 Overview › Row groups › RG #1 › Column chunks › l_address.street
┌────────────────────────────────────────────────────────────────────────┐
│                                                                        │
│                         (active screen body)                           │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
 [↑↓] move  [Enter] drill  [Esc] back                [?] help   [q] quit
```

### Typical drill path

1. **Overview** → pick *Row groups* from the drill menu.
2. **Row groups** → select a row, *Enter* — opens its column chunks.
3. **Column chunks** → select a column, *Enter* — opens the chunk detail.
4. **Column chunk detail** (facts pane + drill menu) → pick *Pages*, *Column
   index*, *Offset index*, or *Dictionary*.
5. Drill sub-screens (*Pages*, *Column index*, etc.) support *Esc* back up to
   the previous level; in *Pages* and *Dictionary*, *Enter* opens a modal with
   the full header / value.

Alternative entry: from **Overview → Schema**, navigate the tree of group and
primitive nodes with `→` / `←`; `Enter` on a leaf drills into a
*Column-across-row-groups* view — one row per row group showing that column's
sizes, encoding, stats — and from there into the chunk detail.

### Inline search

The **Schema**, **Column index**, and **Dictionary** screens support inline
search. Press `/` to enter search-edit mode:

- **Schema** — filters leaf columns whose field path contains the query.
  While the filter is active, the tree collapses to a flat list of matches.
- **Column index** — filters pages whose formatted min or max value
  contains the query.
- **Dictionary** — filters entries whose value contains the query.

In all three cases: typed characters extend the filter; *Backspace* trims;
*Esc* clears the filter and exits edit mode; *Enter* commits (keeps the
filter applied but exits edit mode). The table re-filters live as you type.

## Reading Files from S3

All commands accept `s3://` URIs via the `-f` flag:

```shell
hardwood schema -f s3://my-bucket/data.parquet
hardwood print -n 10 -f s3://my-bucket/data.parquet
```

The CLI resolves credentials via the standard AWS credential chain (environment variables, `~/.aws/credentials`, SSO, instance profiles, etc.).

| Environment Variable | Description |
|----------------------|-------------|
| `AWS_REGION` | AWS region (also read from `~/.aws/config` if not set) |
| `AWS_ENDPOINT_URL` | Custom endpoint for S3-compatible services (MinIO, LocalStack, R2, etc.) |
| `AWS_PATH_STYLE` | Set to `true` to use path-style access (required by some S3-compatible services) |

## Shell Completion

The distribution includes a Bash completion script at `bin/hardwood_completion`. Source it in your shell to enable tab completion for commands, options, and arguments:

```shell
source hardwood_completion
```

To make it permanent, add the line above to your `~/.bashrc` or `~/.bash_profile`.
