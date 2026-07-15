---
name: hardwood-cli
description: 'Inspect, debug, and convert Apache Parquet files with the `hardwood` CLI. Use whenever a user is debugging code that reads or writes Parquet (Arrow / Spark / Flink / Java / Python) and the data file itself may be the cause — to check the schema, physical and logical types, repetition, or a row sample. Use it to diagnose query-optimizer behavior: why row-group pruning, min/max predicate pushdown, or page-index filtering isn''t skipping data. Use it to read per-column and per-row-group compressed sizes and codecs, page-level min/max statistics and the page index, dictionary-encoded values, the footer / PAR1 header, or to convert rows to CSV or JSON. Trigger on `.parquet` files, "is this column dictionary encoded", "why are all row groups scanned", "why isn''t pushdown working", "what types does this parquet have", "convert parquet to csv/json", and any question about row groups, column chunks, data pages, the page index, statistics, or encodings.'
license: Apache-2.0. LICENSE.txt has complete terms
---

# Inspecting Parquet files with `hardwood`

`hardwood` is a command-line tool for looking _inside_ Apache Parquet files without
writing any code. It is purpose-built for the moments when you are debugging
Parquet read/write code and start to suspect the file itself — a type that does
not match, statistics that are missing, an encoding that defeats your optimizer,
a dictionary that is not what you expect.

This skill teaches you which subcommand answers which question, how to read the
output, and how to chain a few commands into a diagnosis.

## Before you start

1. **Confirm the binary is available.** The CLI ships as a GraalVM native binary
   with instant startup. Check for it before assuming a flag will work:

   ```shell
   command -v hardwood && hardwood --version
   ```

2. **Every command takes `-f`/`--file`.** This is the only required flag for the
   read-only commands. It accepts a local path **or** an `s3://` URI (no extra
   config beyond the standard AWS environment variables `AWS_REGION`,
   `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, etc.):

   ```shell
   hardwood schema -f s3://my-bucket/path/to/data.parquet
   ```

3. **`-h`/`--help` on any command lists its real options** — run it when a flag
   below does not match what you see, e.g. `hardwood inspect pages --help`.

4. **`dive` needs a TTY; you usually do not.** `hardwood dive` is an interactive
   terminal UI. It exits with an error when stdin/stdout is not a real terminal,
   which is the normal case for an agent. Use the headless subcommands below for
   your own investigation, and _suggest_ `dive` to the human user when they want
   to poke around a file interactively.

## Command quick reference

| You want to know…                                                    | Run                                             |
| -------------------------------------------------------------------- | ----------------------------------------------- |
| The high-level shape of a file (row groups, rows, sizes, **writer**) | `hardwood info -f FILE`                         |
| The schema: fields, physical types, logical-type annotations         | `hardwood schema -f FILE`                       |
| The same schema as Avro or Protobuf                                  | `hardwood schema -f FILE -F AVRO` / `-F PROTO`  |
| A few actual rows, formatted                                         | `hardwood print -n 20 -f FILE`                  |
| The raw file layout: size, footer offset/length, PAR1 magic          | `hardwood footer -f FILE`                       |
| Per-column compressed/uncompressed size, ranked                      | `hardwood inspect columns -f FILE`              |
| Per-row-group column chunks: type, codec, sizes                      | `hardwood inspect rowgroups -f FILE`            |
| Pages (data + dictionary) and their min/max stats                    | `hardwood inspect pages -f FILE [-c COLUMN]`    |
| The dictionary entries of a column                                   | `hardwood inspect dictionary -f FILE -c COLUMN` |
| Rows out as CSV or JSON                                              | `hardwood convert -f FILE -F csv\|json`         |
| Interactive exploration (hand off to the user)                       | `hardwood dive -f FILE`                         |

## Key output signals to read for

A lot of the diagnostic value is in small labels the commands print. Learn to
spot these:

- **`info` → `Created By:`** reveals the writer library and version
  (`parquet-cpp-arrow version 24.0.0`, `parquet-mr`, `spark`, …). Writers differ
  in whether they emit statistics, page indexes, and which encodings they use —
  this line is often the first clue in a "pushdown doesn't work" mystery.
- **`inspect pages` → the `(stats: …)` suffix** after the column name tells you
  whether page-level statistics exist and where they come from:
  - `(stats: ColumnIndex)` — a Page Index (ColumnIndex + OffsetIndex sidecars)
    is present. This is what engines need for **page skipping**.
  - `(stats: inline)` — only legacy statistics embedded in each data-page header.
    No page index, so no page-level skipping.
  - `(no page-level stats)` — nothing at page level. Nothing can be skipped at
    page granularity.
  - `(stats: mixed)` — different row groups disagree; some skipping only.
- **`inspect pages` → the `Min` / `Max` / `Nulls` columns** show the actual
  bounds per page. If the predicate value falls _inside_ every page's min/max,
  skipping nothing is correct behavior, not a bug.
- **`inspect columns` → `Ratio`** (compressed ÷ uncompressed, as a percent) and
  **`# Pages`**: the row at the top (Rank 1) is the dominant scan cost; `100.0%`
  means the column is stored uncompressed. A `# Pages` count (vs `-`) confirms a
  page index exists for that column.
- **`inspect dictionary` → `no dictionary (column is not dictionary-encoded)`**
  versus a table of entries: tells you whether a column is dict-encoded and what
  its distinct values are.
- **`footer` → magic bytes** `PAR1` at both ends = a plain Parquet file;
  `PARE` = Parquet Modular Encryption (encrypted footer), which most readers
  cannot open without keys.

## Playbook: "the source Parquet file looks wrong"

The user is debugging read/write code and has narrowed it down to a suspect
file. Establish ground truth in three commands:

```shell
hardwood info    -f FILE        # how big, how many row groups, who wrote it
hardwood schema  -f FILE        # exact fields, physical + logical types
hardwood print   -n 10 -f FILE  # the first 10 actual rows
```

What to compare against the code:

- **Physical vs. logical type mismatches.** A field the code treats as a
  `String` may physically be `BYTE_ARRAY` with a `STRING` annotation, or just
  bare `BYTE_ARRAY`. `schema` prints both, e.g.:
  ```
  message schema {
    required int64 id;
    optional binary category (STRING);
    optional int32 ts (TIMESTAMP_MILLIS);
  }
  ```
- **Repetition / nullability.** `required` vs `optional` changes null handling.
- **`Created By`** — if it names a library version known to emit odd encodings,
  that is a lead.

To export a sample for a downstream tool, project just the columns you care
about (nested fields use dot notation) and pick head or tail:

```shell
hardwood print   -n 5  -c "id,account.organization.name" -f FILE  # ASCII
hardwood convert -n 50 --format csv -c "id,value" -f FILE         # CSV to stdout
hardwood convert -n 5  --format json -o sample.json -f FILE       # JSON to a file
```

Row-limit semantics for `-n`/`--rows` (used by both `print` and `convert`):
positive = first _N_ rows (head), negative = last _N_ rows (tail), `ALL` =
every row, and `0` is rejected.

## Playbook: "why isn't predicate pushdown / file skipping working?"

A query is scanning far more data than expected. The engine skips at two
granularities — **row group** (using each column chunk's footer `Statistics`)
and **page** (using the Page Index). Walk down the file to see what skipping is
even possible:

```shell
hardwood info              -f FILE                              # 1. row-group count + writer
hardwood inspect rowgroups -f FILE                              # 2. row-group sizes / codecs
hardwood inspect columns   -f FILE                              # 3. which columns dominate
hardwood inspect pages     -f FILE -c <predicateColumn>         # 4. stats source + min/max bounds
```

Interpretation checklist:

1. **Only one row group?** `info` shows `Row Groups: 1`. Row-group pruning has
   nothing to skip — the fix is to rewrite the file with more, smaller row
   groups, not to tune the query.
2. **`Created By` a writer that omits statistics?** Some writers/spark versions
   disable statistics by default. `inspect pages` will then show
   `(no page-level stats)` and the `Min`/`Max`/`Nulls` columns are `-`.
3. **Stats present but everything overlaps?** If every page's `[Min, Max]`
   contains the predicate value, skipping zero pages is correct — the data really
   is unselectable by that column. Suggest sorting/clustering the file by the
   predicate column before rewriting.
4. **`INT96` timestamps?** Legacy 96-bit timestamps have no usable min/max for
   pushdown in most engines. `schema` will show `INT96`; advise rewriting to
   `TIMESTAMP(MICROS|NANOS)` on `INT64`.
5. **No page index?** `(stats: inline)` or `(no page-level stats)` means
   page-level skipping is off the table; only row-group pruning (if those chunk
   stats exist) can help. To _enable_ page skipping at write time, the writer
   must emit a Page Index — point the user at their writer's
   "page index" / "column index" option.

For **row-group-level** min/max (the chunk statistics an engine uses for
row-group pruning), there is no dedicated headless dump — `inspect rowgroups`
shows sizes/codec only. Use `inspect pages -c <col>` to confirm statistics are
emitted at all and to see the per-page bounds, and recommend the interactive
`hardwood dive -f FILE` (Schema → leaf → _Column-across-row-groups_) to the user
when they need the row-group-by-row-group comparison.

## Playbook: "why is this file so big / so slow to scan?"

```shell
hardwood inspect columns   -f FILE   # columns ranked by compressed size
hardwood inspect rowgroups -f FILE   # per-row-group breakdown
hardwood inspect pages     -f FILE   # encodings + page counts
```

`inspect columns` ranks columns by compressed bytes and prints the compression
`Ratio` and `# Pages`:

```
+------+-----------+-----------+------------+--------------+--------+---------+
| Rank | Column    | Type      | Compressed | Uncompressed | Ratio  | # Pages |
+------+-----------+-----------+------------+--------------+--------+---------+
|    1 | payload   | BYTE_ARRAY|    78.4 KB |      120.0 KB|  65.3% |      10 |
|    2 | id        | INT64     |     2.1 KB |        2.1 KB| 100.0% |       - |
+------+-----------+-----------+------------+--------------+--------+---------+
```

The rank-1 column is your scan cost. Then check _why_ it is large: a `PLAIN`
encoding where `RLE_DICT` would dominate, or no compression (`UNCOMPRESSED` in
`inspect rowgroups`).

## Playbook: "is this column dictionary encoded, and what are its values?"

```shell
hardwood inspect dictionary -f FILE -c <column>           # first 50 entries per RG
hardwood inspect dictionary -f FILE -c <column> --limit 0 # all entries
hardwood inspect pages      -f FILE -c <column>           # confirms a 'dict' page + RLE_DICT data pages
```

- If the column is **not** dict-encoded you get:
  `Row Group 0: no dictionary (column is not dictionary-encoded)`.
- If it is, you get a table of `(RG, Index, Length, Value)`; for `BYTE_ARRAY`/
  `FIXED_LEN_BYTE_ARRAY` the `Length` column is included. `--limit 0` means
  unlimited (default is 50 entries per row group).
- `inspect pages` corroborates: a `dict` page row followed by data pages using
  the `RLE_DICT` (or `PLAIN_DICT`) encoding.

## Playbook: "is this a valid / encrypted Parquet file, and how is it laid out?"

```shell
hardwood footer -f FILE
```

```
File Size:     722 bytes
Footer Offset: 178 bytes
Footer Length: 536 bytes
Leading Magic:  PAR1
Trailing Magic: PAR1
```

- `PAR1` at both ends = a normal file. `PARE` (trailing) means **Parquet
  Modular Encryption** with an encrypted footer — report that the reader needs
  decryption keys; this is why a file "won't open".
- A `Footer Length` that is implausibly large or a missing trailing magic means
  the file is truncated/corrupt.

## Flags at a glance

| Command                                | Useful flags                                                                                                                          |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `info`, `footer`                       | `-f FILE`                                                                                                                             |
| `schema`                               | `-f FILE`, `-F NATIVE\|AVRO\|PROTO`                                                                                                   |
| `print`                                | `-f FILE`, `-n N\|ALL`, `-c a,b`, `-w MAX_WIDTH`, `-s SAMPLE`, `--no-truncate`, `--transpose`, `-i` (row index), `-d` (row delimiter) |
| `convert`                              | `-f FILE`, `-F csv\|json`, `-o OUT`, `-c a,b`, `-n N\|ALL`                                                                            |
| `inspect pages`                        | `-f FILE`, `-c COLUMN`, `--no-stats`                                                                                                  |
| `inspect dictionary`                   | `-f FILE`, `-c COLUMN` (required), `--limit N` (`0` = unlimited)                                                                      |
| `inspect columns`, `inspect rowgroups` | `-f FILE`                                                                                                                             |
| `dive`                                 | `-f FILE` (interactive TTY only)                                                                                                      |

Notes and edge cases:

- `print`/`convert` accept **nested fields by dot path** in `-c`, e.g.
  `-c "account.organization.name"`. An unknown column makes the command exit
  non-zero — check the exit code, not just stdout.
- `-n` rejects `0`; use `ALL` to mean "no limit". Positive = head, negative =
  tail. `tail` works correctly across multiple row groups.
- All commands work over `s3://` URIs; `dive` additionally caches fetched byte
  ranges locally so repeat navigation over S3 stays fast.
- When you only need to confirm one column, always pass `-c` to `inspect pages`
  — it keeps the output small for files with hundreds of columns.

## When to hand off to `dive`

For interactive, multi-screen exploration (schema tree with `/` filtering, the
column-across-row-groups stats comparison, per-page header modals, dictionary
search, row preview), suggest the user run it themselves in a real terminal:

```shell
hardwood dive -f FILE
# over S3:
docker run --rm -it -v "$(pwd)":/data ghcr.io/hardwood-hq/hardwood:latest dive -f /data/FILE
```

**DO NOT try to run `dive` yourself in a non-interactive context** — it will refuse
to start. Build your diagnosis from the headless commands above instead.
