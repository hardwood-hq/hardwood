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
```

## Interactive exploration (`dive`)

`hardwood dive` launches a terminal UI for navigating a Parquet file's structure
without re-invoking the CLI for each slice:

```shell
hardwood dive -f data.parquet
```

From the Overview landing screen, drill into **Schema** or **Row groups**,
then into column chunks and per-chunk metadata.

| Key | Action |
|-----|--------|
| `↑` / `↓` | Move selection |
| `Enter` | Drill into the selected item |
| `Esc` / `Backspace` | Go back one level |
| `Tab` / `Shift-Tab` | Switch focused pane |
| `g` | Jump back to Overview |
| `?` | Toggle help overlay |
| `q` / `Ctrl-C` | Quit |

Available screens: Overview, Schema (expandable tree of groups + leaves),
Row groups, Column chunks, Column chunk detail (facts + drill menu), Pages
(with a page-header modal on Enter), Column index, Offset index, Footer &
indexes, Column-across-row-groups (from the Schema screen), Dictionary (with
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

### Searching dictionaries

Open the **Dictionary** screen, then press `/` to enter inline search mode.
Typed characters extend the filter; *Backspace* trims it; *Esc* clears the
filter; *Enter* commits it (keeps the filter applied but exits edit mode).
The table re-filters live as you type.

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
