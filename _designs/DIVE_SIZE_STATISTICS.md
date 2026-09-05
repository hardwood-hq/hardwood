# Design: size statistics and level histograms in the CLI

**Status: Implemented.** Tracking issue: #870. Builds on the parse landed
under #607.

## Goal

Surface the `SizeStatistics` a file already carries — the unencoded
`BYTE_ARRAY` size and the repetition and definition-level histograms —
on the two CLI surfaces that answer "why is this column this big" and
"where did the nulls come from": the `dive` column-chunk detail screen
and `hardwood inspect columns`.

The level histograms are the only place in the metadata where "the
field was absent" and "the list was empty" are distinguishable.
`ColumnIndex.null_counts` lumps them together, and a raw
`definition_level_histogram` of `[52428, 104857, 39321, 6291456]` says
nothing without the schema to name each bucket. Naming the buckets is
what makes the data readable, and it is derivable from the column's
path through the schema alone.

Both surfaces read the chunk-level statistics in `ColumnMetaData` field
16. The per-page copies in `ColumnIndex` and `OffsetIndex` are read only
to report their presence; displaying them per page is out of scope.

## Screen

Example column is `websites.list.element`, a `LIST<optional STRING>`
inside an optional group (`max def 3`, `max rep 1`).

The pane is grouped rather than a flat list of label/value rows. Four
captions — Identity, Storage, Content, Layout — let the eye find the
group holding the fact it wants instead of reading two dozen uniform
rows, and the offsets go last because almost no reader needs them.
A derived figure that qualifies a count rides on that count's row
rather than standing as a row of its own: fan-out is a property of
`Values`, not an independent fact, and a bare `6.19` on its own line
would read as one with no scale.

```
╭ websites.list.element (RG #0) ───────────────────────────╮╭ Drill into ──────────────────────────╮
│ Identity                                                 ││▶ Pages           96 pages            │
│   Path                websites.list.element              ││  Column index    present · levels    │
│   Physical            BYTE_ARRAY                         ││  Offset index    present · unencoded │
│   Logical             STRING                             ││  Dictionary      present             │
│                                                          ││                                      │
│ Storage                                                  ││                                      │
│   Compressed          12.4 MiB  (39.0% of uncompressed)  ││                                      │
│   Uncompressed        31.8 MiB                           ││                                      │
│   Codec               ZSTD                               ││                                      │
│   Encoding            PLAIN+DICT 92%  (966k of 1.05M)    ││                                      │
│   Chunk encodings     PLAIN, RLE, RLE_DICTIONARY         ││                                      │
│   Size statistics     chunk + 96 pages                   ││                                      │
│   Unencoded           66.0 MiB  (5.3× compressed, …)     ││                                      │
│   Avg value size      10 B                               ││                                      │
│                                                          ││                                      │
│ Content                                                  ││                                      │
│   Records             1,048,576                          ││                                      │
│   Values              6,488,062  (6.19 per record)       ││                                      │
│   Present             6,291,456  (97.0%)                 ││                                      │
│   Nulls               196,606  (3.0%)                    ││                                      │
│   Avg list length     7.10  (non-empty)                  ││                                      │
│                                                          ││                                      │
│ Def levels (max 3)                                       ││                                      │
│   0  websites null        52,428   0.8% ▏                ││                                      │
│   1  websites empty      104,857   1.6% ▏                ││                                      │
│   2  element null         39,321   0.6% ▏                ││                                      │
│   3  element present   6,291,456  97.0% ███████████▋     ││                                      │
│                                                          ││                                      │
│ Rep levels (max 1)                                       ││                                      │
│   0  new record        1,048,576  16.2% █▉               ││                                      │
│   1  websites.list     5,439,486  83.8% ██████████▏      ││                                      │
│                                                          ││                                      │
│ Layout                                                   ││                                      │
│   Data offset         109,970                            ││                                      │
│   …                                                      ││                                      │
╰──────────────────────────────────────────────────────────╯╰──────────────────────────────────────╯
 [Tab] pane  [↑↓] move  [Enter] open  [l] levels  [t] logical types  [Esc] back  │  [?] help  [q] quit
```

The two level blocks add roughly fourteen lines to a pane that already
runs to twenty. `l` toggles them, in the same shape as the existing `t`:
handled before the MENU-only early return so it works from either pane,
carried as a flag on `ScreenState.ColumnChunkDetail`, and advertised in
the key bar only when the chunk has a histogram to show. It defaults
off, so the derived rows — which are the summary a reader wants at a
glance — stay visible and the raw buckets become a deliberate step:

```
│ Avg list length       7.10  (non-empty)                  │
│ Levels                [l] to show                        │
```

The level blocks are bounded by `maxDefinitionLevel + 1` and
`maxRepetitionLevel + 1`, so the viewport-virtualization rule in
`CLAUDE.md` does not apply — there is no collection here whose size
tracks the data.

The pane scrolls. Even collapsed it runs past the bottom of a short
terminal, and a `Paragraph` drops what does not fit without saying so,
which is indistinguishable from a complete pane. `scrollTop` rides on
`ScreenState.ColumnChunkDetail`; ↑/↓, PgUp/PgDn and g/G drive it while
the facts pane has focus, where they otherwise mean nothing. The title
carries a `1-35/39` range whenever anything is hidden, so a clipped
pane is never silent. `FooterScreen` already works this way.

A chunk with no usable histogram at all has nothing to collapse: its two
blocks are one `—` row each, so they are shown outright and neither the
`Levels` row nor the `[l]` hint appears. The toggle and its key exist
only where there is a block worth the keystroke.

### Menu hints

`Column index` and `Offset index` gain a suffix when the page-level
fields are present, so a reader knows whether drilling in will show
per-page histograms before spending the keystroke:

| Item | Hint |
|---|---|
| `Column index` | `present · levels` when `ColumnIndex` fields 6/7 are set, else `present` |
| `Offset index` | `present · unencoded` when `OffsetIndex` field 2 is set, else `present` |

Per the drill-into recipe in [DIVE_THEME.md](DIVE_THEME.md), a hint
carrying both a fact and an annotation splits: `present` at default fg,
` · levels` at `Theme.dim()`.

## Level labels

Walk the column's `FieldPath` from `FileSchema.getRootNode()` and
collect the nodes whose repetition type is `OPTIONAL` or `REPEATED`, in
order. There are exactly `maxDefinitionLevel` of them; call them
`d₁…d_maxDef`. Definition level `i` names the node the value failed to
reach:

| Condition | Label |
|---|---|
| `i < maxDef`, `d(i+1)` is `REPEATED` | `<parent name> empty` |
| `i < maxDef`, `d(i+1)` is `OPTIONAL` | `<node name> null` |
| `i == maxDef` | `<leaf name> present` |

A `REPEATED` node is named for its parent because the empty collection
is a fact about the field the user knows — `websites empty`, not the
synthetic `websites.list` node the LIST annotation introduces. The same
rule gives a `MAP` the right name, since `key_value` sits under the map
field. An unannotated repeated field has no such wrapper, so its parent
is whatever group encloses it; there the repeated node's own name is
used instead.

Repetition level 0 is always `new record`. Level `i` is the dotted path
of the `i`-th repeated node, relative to the root.

## Derived rows

| Row | Definition |
|---|---|
| `Size statistics` | `chunk + N pages` / `chunk only` / `— (not written)` |
| `Compressed` | with the codec's effect as a parenthetical, stated on the row it applies to |
| `Unencoded` | what the values occupy with no encoding, and what that is as a multiple of the compressed size. Recorded in `unencoded_byte_array_data_bytes` for `BYTE_ARRAY`; for a fixed width it is `present values × width`, so the figure exists on every column. For `BYTE_ARRAY` the parenthetical also carries `4 × present values`, the per-value length prefixes the field excludes |
| `Records` | `rep[0]` |
| `Present` | `def[maxDef]`, with its share of `Values` |
| `Nulls` | `null_count` where the writer recorded one, otherwise `Values − Present`, with its share of `Values`. For a required column that is zero, which the schema settles even where no statistics were written — reporting `—` would contradict the `Present` row beside it |
| `Encoding` | what the *data* pages use, from `encoding_stats`, abbreviated and `+`-joined, followed by the dictionary's cardinality where it has one. `Chunk encodings` follows only where `encoding_stats` exists and so made the two differ: the declared list carries the dictionary page and the RLE level streams as well, and cannot express a dictionary abandoned partway |
| dictionary cardinality | dictionary `num_values` ÷ the values it could hold an entry for, as a whole percent. The denominator is the present-value count where known, since nulls never reach a dictionary. `<1%` rather than `0%` for a non-zero count, which beside a `DICT` label would read as "no dictionary" |
| fan-out | `sum(def) / rep[0]`, as a `per record` qualifier on the `Values` row; `1.0` by definition where the column cannot repeat |
| `Avg list length` | non-empty list elements ÷ records holding a non-empty list |
| `Avg value size` | `unencoded / present values`. `BYTE_ARRAY` only — for a fixed width it would restate the width |
| level rows | count, share of `sum(def)` or `sum(rep)`, bar |

The unencoded size is the one figure here that predicts read-side cost:
compressed and uncompressed both measure the encoded form, so a
dictionary-encoded column looks cheap beside what it costs to
materialise. What it does *not* support is a verdict on the encoding
itself. Comparing it against `uncompressedBytes` yields a difference in
uncompressed bytes, and every question a reader actually has is about
compressed ones — a column whose dictionary looks worth 170 KiB
uncompressed can be worth nothing at all on disk, because the codec
finds the same redundancy either way. Answering that needs a re-encode,
which a metadata reader cannot do, so no such figure is offered.

`Avg list length` needs the level at which the first repeated node
sits. Let `e` be its index in `d₁…d_maxDef`; definition levels below
`e` are the buckets where no list element exists at all. Then

```
elements = sum(def) − Σ(def[i] for i < e)
records  = rep[0]   − Σ(def[i] for i < e)
```

and the row is `elements / records`. It is emitted only when
`maxRepetitionLevel == 1`. With nested repetition a single average has
no unambiguous referent, so the row is omitted rather than computed
against an arbitrary level.

A row is dropped rather than shown as `—` when it would add nothing,
which keeps the flat-column form short instead of scaffolding it with
placeholders. Two cases are distinct:

- **Redundant.** When `maxRepetitionLevel == 0` every value is its own
  record, and when `maxDefinitionLevel == 0` every value is present. The
  quantities are known — both fall back to `num_values` — but `Records`,
  `Present` and the fan-out would restate the `Values` row. In `dive`
  they are dropped, where a redundant fact costs a line in a pane that
  overflows. In the `inspect` table they are printed: the column takes
  its width either way, and a blank cell makes a reader — or a parser —
  reconstruct a number already known.
- **Unknown.** Where the histogram a quantity needs is absent or empty
  and no such fallback applies, the quantity does not exist and its row
  is dropped. `records()` and `presentValues()` throw rather than fall
  back in that case, so a caller that skips the guard fails loudly
  instead of reporting level slots as records or nulls as values.

`Size statistics` reads `chunk + N pages` when *either* page-level field
is present: the histograms live in `ColumnIndex` and the unencoded sizes
in `OffsetIndex`, and a required `BYTE_ARRAY` column has only the
latter to write. A page index that is present but unreadable is `-` on
`inspect` rather than `chunk only`, which would be a claim the reader
cannot check.

## Consistency check

The screen is self-checking. `num_values` must equal both `sum(def)`
and `sum(rep)`, and the chunk's `null_count` must equal
`num_values − def[maxDef]`. A writer that disagrees is painted as an
error rather than silently rendered, since that is the class of bug a
reader opens `dive` to find:

```
│ ⚠ Declared vs actual  values 6,488,062, sum(def) 6,488,050        │
```

The check runs on both surfaces. `inspect` emits no colour, so there it
is the `⚠` prefix and the wording alone.

## Degraded forms

A file written before parquet-format 2.10 collapses to one row, with no
empty scaffolding for the rows that cannot be filled:

```
│ Size statistics       — (not written)                    │
```

A flat `BYTE_ARRAY` column has both histograms legitimately omitted by
the spec, and the unencoded size is still the interesting number:

```
│ Size statistics       chunk + 240 pages                  │
│ Unencoded             420.1 MiB  (+48.0 MiB lengths)     │
│ Avg value size        35 B                               │
│ Def levels            — (max 1, redundant with Nulls)    │
│ Rep levels            — (not repeated)                   │
```

A present but empty histogram is distinct from an absent one — a writer
emits `definition_level_histogram = []` for a required, non-repeated
column — and is reported as the `—` form rather than indexed into.
Non-`BYTE_ARRAY` columns keep the `Unencoded` row, computed from the
value count, and omit only `Avg value size`, which for a fixed width
would restate the width. A writer omits `SizeStatistics` altogether for
a required, non-repeated, fixed-width column — the shape least able to
spare the figure, since nothing else in the footer reports its size —
so the summary forms regardless and `hasSizeStatistics()` reports
whether one was recorded.

A histogram whose length is neither zero nor `maxLevel + 1` is neither
of those: the writer meant to record one and got it wrong. Pairing its
counts with the level names would misreport them, so it is dropped from
the block and named by the consistency check instead — `def histogram
has 3 buckets, max def 3 needs 4`.

## Narrow terminals

The level rows degrade by dropping their least load-bearing column
first. Thresholds are on the pane's inner width:

| Inner width | Rendered |
|---|---|
| ≥ 56 | level, label, count, percentage, bar |
| 44–55 | level, label, count, percentage |
| < 44 | level, label, count |

Bars are drawn from the eighth-block characters `▏▎▍▌▋▊▉█` so a bar
carries sub-cell resolution at small shares.

## `hardwood inspect columns`

The ranked table answers "which column is this file, and what is the
lever". `Share` states the first question rather than leaving it as
arithmetic over `Compressed`; `Codec` and `Compression` sit together so
the percentage names what it divides; `Unencoded` is summed per column
path across row groups, `-` only where the present-value count is
unknown. `Uncompressed` is not shown — it existed only as the
percentage's other operand and follows from the two figures that are:

```
+------+-------------------------+------------+-------+------------+-------+-------------+-----------------+-----------+---------+
| Rank | Column                  | Type       | Codec | Compressed | Share | Compression | Encoding        | Unencoded | # Pages |
+------+-------------------------+------------+-------+------------+-------+-------------+-----------------+-----------+---------+
|    1 |       order.description | BYTE_ARRAY |  ZSTD |   61.7 MiB | 33.5% |       33.5% | PLAIN+DICT 100% | 420.1 MiB |     240 |
|    2 | order.tags.list.element | BYTE_ARRAY |  ZSTD |   12.4 MiB |  6.7% |       39.0% |  PLAIN+DICT 92% |  66.0 MiB |      96 |
|    3 |       order.total_cents |      INT64 |  ZSTD |   38.4 MiB | 20.8% |       40.0% |        DICT <1% |  96.0 MiB |     120 |
+------+-------------------------+------------+-------+------------+-------+-------------+-----------------+-----------+---------+
```

`Encoding` is here and not only under `--column` because the reason to
scan this table is to pick the column worth looking at more closely, and
a dictionary the writer abandoned is one of the few things that decides
it. The cell is the union across row groups: a fallback in any one of
them is a property of the column as the file stores it.

The cardinality is the other. `DICT` reads identically for a dictionary
that pays for itself and one that holds an entry per value, and the
second is a copy of the column: the values are stored once verbatim in
the dictionary page and once more as a stream of distinct indices, which
is high-entropy by construction and survives the codec. It goes
unnoticed because writers dictionary-encode by default and reconsider
only when the dictionary outgrows its size limit — a high-cardinality
column that stays under the limit keeps a useless dictionary silently.

The figure is deliberately a number and not a verdict. What a column
*should* be encoded as is not derivable from the footer, as the removed
PLAIN comparison showed; `100%` says the dictionary is a second copy,
and re-encoding is what says what to do about it.

Both figures cost reads the footer cannot serve. The dictionary's
`num_values` lives on its page header, so the ranked table takes one
short seek per column chunk on top of the offset-index read it already
does. That is a real cost on a wide file and it buys the only view from
which the reader can *find* the column: a figure reachable solely by
drilling into columns one at a time answers a question nobody knew to
ask. `dive` pays it only on the column chunk detail screen, and caches
per chunk — the list screens re-render on every keystroke, and one read
per visible row would turn footer arithmetic into N round trips.

A `--column <path>` option prints the per-chunk detail — the same facts
as the dive pane, one row per row group, followed by the histograms.
The column set is fixed whatever the column's shape, so two runs are
comparable and anything parsing the output sees a stable table; a cell
is `-` only where the value genuinely is not known:

```
$ hardwood inspect columns -f orders.parquet --column order.tags.list.element

order.tags.list.element  BYTE_ARRAY / String  max def 3  max rep 1

+----+-----------+---------+-----------+-----------+---------+-------+------------+-------------+------------+-----------+
| RG | Values    | Nulls   | Records   | Present   | Fan-out | Codec | Compressed | Compression | Encoding   | Unencoded |
+----+-----------+---------+-----------+-----------+---------+-------+------------+-------------+------------+-----------+
|  0 | 6,488,062 | 196,606 | 1,048,576 | 6,291,456 |    6.19 |  ZSTD |   61.7 MiB |       33.5% | PLAIN+DICT |  66.0 MiB |
|  1 | 6,502,110 | 201,004 | 1,048,576 | 6,301,106 |    6.20 |  ZSTD |   61.8 MiB |       33.6% | PLAIN+DICT |  66.1 MiB |
+----+-----------+---------+-----------+-----------+---------+-----------+------------------+

Definition levels (all row groups, max 3)
 0  tags null           104,857   0.8%  ▏
 1  tags empty          209,714   1.6%  ▏
 2  element null         78,642   0.6%  ▏
 3  element present  12,592,562  97.0%  ███████████▋

Repetition levels (all row groups, max 1)
 0  new record        2,097,152  16.2%  █▉
 1  tags.list        10,878,972  83.8%  ██████████▏
```

Level histograms sum element-wise, so the file-wide block is exact
rather than a sample of one row group. `--row-group <n>` narrows both
the table and the histograms to a single row group.

Bars are the same plain characters used by `dive`, so the two surfaces
render identically. The `inspect` commands emit no colour and this does
not change that; the consistency check is the `⚠` prefix and the
wording alone, one line per offending row group under the table.

Output is written as UTF-8 whatever the platform encoding says. The
bars, the `—` placeholders and the `⚠` are all non-ASCII, and a default
stream encodes them as `?` on a host with no UTF-8 locale — a native
image worst of all, since its default charset is fixed when the image
is built and no runtime `LANG` reaches it.

## `LevelSummary`

Both surfaces share one helper,
`dev.hardwood.cli.internal.LevelSummary`, beside the existing `Sizes`
and `Fmt`. It is a record built by a static factory from the column's
schema and metadata, holding the derived scalars, the labelled level
rows, and the consistency verdict. It performs no I/O and no rendering:
`dive` paints it through its own `fact()` helper so labels keep
`Theme.primary()`, and `inspect` paints it through `RowTable`.

The bar rendering and the schema walk live in the same class. Their only
consumer is `LevelSummary` itself, and splitting them out would add two
files with one call site each.

The factory always returns a summary: a chunk with no `SizeStatistics`
still has a shape, and for a fixed-width column the unencoded size
follows from the value count alone. `hasSizeStatistics()` reports
whether the file recorded one. Quantities that cannot be established
throw rather than fall back — `records()` and `presentValues()` are
right to return `num_values` only for a column that cannot repeat or
cannot be null, and a caller that skips the guard would otherwise read
level slots as records and nulls as present values. The two page-index predicates — whether the column index carries
level histograms, whether the offset index carries unencoded sizes — are
static on the same class, since both surfaces ask them and each answers
half of "does the page index describe these pages".

## One figure, one rendering

A reader moves between the two surfaces on the same file, so a quantity
that appears on both must appear the same way. Three shared helpers in
`dev.hardwood.cli.internal` are what hold that:

- `Sizes.compression(compressed, uncompressed)` renders
  compression as the percentage of the uncompressed size that survived
  the codec — everywhere. When `uncompressed <= 0`, it renders the shared
  absent-value marker `Strings.ABSENT_VALUE` (`—`). `dive` previously showed
  a `×` factor on its overview, row-group and chunk tables while `inspect`
  showed a percentage, which describes the same quantity two ways and inverts
  between them. The row-group detail pane's group caption is `Storage`,
  not `Compression`, since it now holds a `Compression` row of its own
  and `Storage` is what the column chunk detail already calls the same
  three figures.
- `Encodings.dataPages(metaData)` decides what a chunk's values are
  encoded with, and `Encodings.label(...)` renders it — ordered by the
  enum rather than by the caller's iteration, since a `Set.of` iterates
  in an order that varies between JVM runs and this string is what a
  reader compares between two invocations. Reading the flat `encodings`
  list instead gives a different and less informative answer for the
  same chunk, and both labels would sit one screen apart.
- `LevelSummary.nullCount(statistics)` decides which of the two sources
  answers "how many nulls". That is not a per-surface choice: a required
  column holds none whether or not `null_count` was written, and a
  surface that reports `—` there contradicts the present-value count it
  prints from the same schema fact.

The cross-row-group screen — `Schema` → a leaf → the per-row-group
table — is the interactive twin of `inspect columns --column`: the same
shape, one row per row group for one column. It carries `Unencoded`
for the same reason that table does, since it is what says whether a
chunk is large because of its values or because of its encoding.

## `Theme.error()`

The consistency mismatch is an error state, and `Theme` has no tier for
one. It gains a fifth role — red, with the same truecolor-with-named-
ANSI-fallback structure as `accent()` and `selection()`:

| Method | Truecolor terminals | Named-ANSI terminals |
|---|---|---|
| `error()` | `Style.EMPTY.fg(rgb(220, 50, 47))` | `Style.EMPTY.fg(Color.RED)` |

The RGB is Solarized's red slot, chosen on the same grounds as the
existing two: it survives iTerm2's bold-to-bright remapping and reads
against both Solarized variants.

[DIVE_THEME.md](DIVE_THEME.md) declares its decision tree exhaustive, so
the new tier is added there rather than bolted on: an error state is
checked first, ahead of selection, because a mismatch row must stay
legible even when it is the row under the cursor.

Nothing else in this change introduces colour. Bar length already
encodes magnitude, so colouring the bars would encode the same variable
twice, and the semantic split between present and absent levels is
already carried by the label column. Level rows and counts are body
content — tier 4, `Style.EMPTY`. The `— (not written)` and
`— (not repeated)` strings are parenthetical advisories — tier 5,
`Theme.dim()`.

## Testing

`LevelSummaryTest` covers the label walk against each schema shape that
changes its outcome: a LIST, a MAP, a struct member, an unannotated
repeated field, a flat required column, a flat optional one, a column
with absent statistics, and one with a present-but-empty histogram. It
pins the derived scalars — including the buckets subtracted from both
sides of `Avg list length`, which needs a column whose lists are not all
present — all three arms of the consistency check, the throw that
replaces a fallback where the count is unknown, and the element-wise
combination `inspect` sums with.

`DiveRenderTest` gains cases for the pane with levels toggled off, with
them on, for the not-written form, for the degraded rows that are shown
without a toggle, for the key bar that offers `[l]` only where a
histogram is behind it, and for the scroll: that the tail is
unreachable at offset zero on a short pane and reachable when scrolled,
and that a pane which fits carries no range marker.
`InspectColumnsCommand` gains tests for `--column`, for the `Unencoded`
column on both recorded and computed shapes, for the fixed column set,
and for the file-wide sum against a four-row-group file, where sampling
one chunk would read a quarter of the true count.

`EncodingsTest` drives the encoding label directly with hand-built
`PageEncodingStats`, since no checked-in file is large enough for a
writer to reach for a dictionary at all, let alone abandon one. It
covers the dictionary page being excluded from the data-page answer, the
mid-chunk fallback, a zero-count page type, the fall back to the
declared list, the enum ordering, and the cardinality: the 100% and
`<1%` readings, the rounding that must not render `0%`, and the three
cases where the figure is dropped rather than fabricated.
`dive_screenshots_fixture.parquet` carries both readings end to end —
`names.primary` holds an entry per value, `category` a handful for all
of them — so the ranked table and the facts pane are pinned against a
real file rather than only against assembled metadata.

`SizesTest` pins the percentage form of `compression` and its
placeholder, and `DiveRenderTest` asserts that no screen rendering it
still shows a `×`.

`ThemeTest` pins `error()` alongside the existing four.

`cli/src/test/resources/dive_screenshots_fixture.parquet` already
carries chunk-level `SizeStatistics` and needs no regeneration. It
covers every branch on its own: `websites.list.element` is the LIST
shape above, `names.common.key_value.value` the MAP shape, `id` a flat
required `BYTE_ARRAY`, `confidence` a `max def 1` column with a real
histogram, and the `metric_*` columns have no `size_statistics` at all.
Its columns carry no page index, so the `chunk only` form is what the
screenshots show.

## Documentation

- `docs/content/reference/cli.md` — the `--column` and `--row-group`
  options.
- The `dive` documentation — the new rows and the `l` key.
- `skills/hardwood-cli/SKILL.md` — the `inspect columns` entries, its
  derived-metric glossary, and the option table, per the agent-skills
  rule in [CONTRIBUTING.md](../CONTRIBUTING.md).
- `_designs/DIVE_THEME.md` — the `error()` tier.
- The `dive` screenshots regenerated via the `screenshots` profile: the
  chunk detail and a new capture for the levels view, plus the overview,
  row-group and chunk-list captures that carried the `×` factor.
- `FORMAT_COVERAGE.md` — size statistics have a functional consumer
  once this lands.
- `ROADMAP.md` — the 9.1 Statistics entries.
