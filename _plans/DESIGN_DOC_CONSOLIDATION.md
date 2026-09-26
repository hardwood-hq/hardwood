# Design doc consolidation

**Status: In progress.** Tracking issue: #1290. Replaces the 93 historical design docs with 24 design docs kept current with the code; implementation plans live under `_plans/` and are deleted when their work lands. The docs not yet consolidated sit in `_designs-legacy/`, and each area's PR moves its sources out of there.

## Target design docs

| # | Target | Scope | Fed by |
|---|---|---|---|
| 1 | `READ_PIPELINE.md` | Row group to published batch: `RowGroupIterator` → `PageSource` → `ColumnWorker` → `BatchExchange`, threading, back-pressure, batch sizing, multi-file planning, `ReaderConfig` | PARSING_PIPELINE_V2, COLUMN_READER_ADAPTIVE_BATCH_SIZING, MULTI_FILE_INCREMENTAL_PLANNING, READER_CONFIG, invariant from LAZY_ROW_GROUP_INITIALIZATION, [facts from deleted sources](#facts-from-deleted-sources) |
| 2 | `COLUMN_READER.md` | `ColumnReader` data model: layers, offsets, `Validity`, varlength leaves, real view, cursor/scan | COLUMN_READER_ARROW_LAYOUT, VALIDITY_WORD_BITMAP, COLUMN_READ_PIPELINE |
| 3 | `ROW_READER.md` | Flat vs nested row reader, accessor addressing, `head`/`tail`/`skip` | ROW_BASED_SEEK, reader section of PARSING_PIPELINE_V2, [facts from deleted sources](#facts-from-deleted-sources) |
| 4 | `NESTED_DECODE.md` | Levels to nested batches: schema→layers, `IndexMode`, real view on drain, bulk copy, fixed-size-list fast path, level scratch | NESTED_REALVIEW_ON_DRAIN, NESTED_ALL_PRESENT_PAGE_BULK_COPY, FIXED_SIZE_LIST_FASTPATH, POOLED_LEVEL_DECODE_SCRATCH, UNANNOTATED_REPEATED_LISTS |
| 5 | `VALUE_DECODE.md` | Page values to Java values: decoders, SIMD, dictionary string interning, `LeafKind` | DICTIONARY_STRING_REUSE, NESTED_PRIMITIVE_LEAF_DECODE, ~5 lines of SIMD_VECTOR_API_PLAN |
| 6 | `PREDICATE_MODEL.md` | `FilterPredicate` semantics, literal rules, resolution, parquet-java compat translation | PREDICATE_LITERALS, PREDICATE_PUSHDOWN (API), COMPAT_FILTER_SUPPORT |
| 7 | `STATISTICS_PRUNING.md` | CANNOT/MIGHT/ALWAYS from metadata: `UnitStats`, bounds readability, page index, inline stats, bloom, dictionary | UNIT_STATISTICS_CONVERGENCE, ALWAYS_MATCH_STATISTICS, PREDICATE_PUSHDOWN, COLUMN_INDEX_PUSHDOWN, INLINE_PAGE_STATS_FALLBACK, BLOOM_FILTER_PUSHDOWN, BLOOM_FILTER_SUPPORT, DICTIONARY_PUSHDOWN, COLUMN_ORDER_HANDLING, UNREADABLE_SORT_ORDER_BOUNDS, SIZE_STATISTICS_AND_NAN_COUNTS |
| 8 | `RECORD_FILTERING.md` | Exact row filtering on both readers: augmented projection, compiled matcher, drain-side matchers, selection/compaction, filter-only skip, row selection over the filtered relation | RECORD_FILTER_COMPILATION, DRAIN_SIDE_RECORD_FILTERING, EXACT_COLUMN_READER_FILTERING, FILTER_ONLY_COLUMN_SKIP, ROW_READER_AUGMENTED_PROJECTION, ALWAYS_MATCH_STATISTICS (consumption), ROW_SELECTION_SEMANTICS |
| 9 | `INPUT_FILES.md` | `InputFile` contract, ownership, mapped and in-memory backends, size limits | INPUT_FILE_ABSTRACTION (contract only), LARGE_FILE_PER_REGION_MAPPING |
| 10 | `S3_STORAGE.md` | `hardwood-s3`: zero-SDK client, SigV4, credentials, `S3Source`, range backing | S3_ZERO_SDK, REMOTE_RANGE_BACKING, one note from S3_OBJECT_STORAGE |
| 11 | `FETCH_PLANNING.md` | Which bytes a read requests: fetch sequence, `CoalescingPolicy`, fetch plans, `SharedRegion`, page masking | REMOTE_READ_PATH (design part), CROSS_COLUMN_COALESCING, SEQUENTIAL_FETCH_PLAN_PAGE_MASKING, facts from COALESCED_OFFSET_INDEX_READS and OFFSET_INDEX_SUPPORT, I/O section of PARSING_PIPELINE_V2 |
| 12 | `FILE_METADATA.md` | Footer read, Thrift parse policy, page-index consistency, per-file metadata cache | THRIFT_METADATA_PARSER_HARDENING, PER_FILE_METADATA (later: PARSED_METADATA_REUSE end state), [facts from deleted sources](#facts-from-deleted-sources) |
| 13 | `EXCEPTION_MODEL.md` | Kept; #1093 sentence rewritten as end state | EXCEPTION_MODEL |
| 14 | `LOGICAL_TYPES.md` | Annotation model, timestamps (incl. FLBA12), Variant, geospatial, read and write | VARIANT_LOGICAL_TYPE, GEOSPATIAL_SUPPORT, FLBA12_TIMESTAMPS, LOCAL_TIMESTAMP_ACCESSOR |
| 15 | `AVRO_BINDING.md` | `hardwood-avro`: type mapping, decode plan, name resolution | AVRO_DECODE_PLAN, AVRO_NAME_RESOLUTION, mapping table from AVRO_GENERICRECORD_SUPPORT (refreshed) |
| 16 | `WRITER.md` | Write model, `OutputFile` publish/discard, components, row-group lifecycle and sizing, memory, footer, threading | WRITER_SUPPORT (design sections), WRITER_ROW_GROUP_SIZING, parts of WRITER_DICTIONARY_SELECTION and WRITER_PRIMITIVE_TYPES |
| 17 | `WRITER_INPUT.md` | Schema builder, `ColumnBatch` contract, shredding, annotation ranges, `RowWriter` | WRITER_NESTED, WRITER_ROW_API, WRITER_ANNOTATION_RANGES, parts of WRITER_LOGICAL_TYPES and WRITER_PRIMITIVE_TYPES |
| 18 | `WRITER_ENCODING.md` | Page/chunk layout, `AUTO` dictionary rule and probes, named policies, codecs, statistics | WRITER_DICTIONARY (layout), WRITER_DICTIONARY_SELECTION, WRITER_DICTIONARY_EARLY_ABANDONMENT (1 para), WRITER_CODECS_AND_ENCODINGS, parts of WRITER_PRIMITIVE_TYPES and WRITER_LOGICAL_TYPES |
| 19 | `WRITER_VALIDATION.md` | Interop gate, reader tiers, coverage assertion | WRITER_INTEROP_GATE, WRITE_COVERAGE_ASSERTION |
| 20 | `DIVE_ARCHITECTURE.md` | Screen stack, state/handler/render split, `ParquetModel` caches, preview window, read-failure guard, test layers | INTERACTIVE_DIVE_TUI, PREVIEW_WINDOW, DIVE_READ_FAILURE_HANDLING |
| 21 | `DIVE_UI_RULES.md` | Normative: visual tiers and `Theme`, navigation rules and `▶`, viewport virtualization | DIVE_THEME, DIVE_NAVIGATION_MODEL (rules only), DIVE_LIST_VIEWPORT_VIRTUALIZATION |
| 22 | `CLI_VALUE_RENDERING.md` | One spelling per value and figure across all commands and dive | SHARED_VALUE_RENDERER, DIVE_SIZE_STATISTICS |
| 23 | `DOCUMENTATION.md` | Diátaxis placement rules, prose check, site stack, two-repo publish, API change reports | DOCS_DIATAXIS_STRUCTURE, DOCS_MIGRATION (topology only), API_CHANGE_REPORT_PUBLISHING |
| 24 | `BUILD_INFRASTRUCTURE.md` | `qa` profile, Error Prone checks, CI dependency closure, manifest metadata | ERROR_PRONE_RULE_ENFORCEMENT, JAR_MANIFEST_BUILD_METADATA |

Root files extended instead of new design docs:

- `TESTING.md` ← INTEGRATION_TESTS, DIFFERENTIAL_TESTING (oracle, `__row__`, comparison layers).
- `PERFORMANCE.md` ← micro-benchmark conventions from NESTED_READ_BENCHMARK and FLAT_WRITE_BENCHMARK.
- `ARCHITECTURE.md` gets a pointer paragraph per area (it has nothing on filtering, the writer, Avro or Variant today).

### Facts from deleted sources

Areas delete their sources outright. These facts from them belong to targets not yet written and have no other copy. The source is readable at `2cba6f90:_designs-legacy/<NAME>.md` (read path area). Each still holds in the code.

| Target | Fact | Source |
|---|---|---|
| `LOGICAL_TYPES.md` | `getDate`, `getUuid`, `getInterval` read nothing from the annotation and check `LogicalAccessorKind.requireDate/requireUuid/requireInterval` first; `getString` is guarded by `requireText` (`TextColumns.holdsText`, the rule the `String` filter literal uses) | NESTED_PRIMITIVE_LEAF_DECODE |
| `LOGICAL_TYPES.md` | Every other annotation-decoding accessor casts the annotation it expects (e.g. `(LogicalType.TimeType) leaf.logicalType()`); another or no annotation fails at that cast, and a physical-type mismatch surfaces as the storage array's `ClassCastException` (#971) | NESTED_PRIMITIVE_LEAF_DECODE |
| `LOGICAL_TYPES.md` | FLOAT against FLOAT16 and the two TIMESTAMP kinds have their own guards: `NestedBatchIndex.requireFloatAccess`, `TimestampAccessorKind.require` | NESTED_PRIMITIVE_LEAF_DECODE |
| `LOGICAL_TYPES.md` | A group element against a leaf accessor is rejected by `PqListImpl.requirePrimitiveElement` / `PqMapImpl.requirePrimitiveValue`; without them the accessor would decode the group's first leaf | NESTED_PRIMITIVE_LEAF_DECODE |
| `LOGICAL_TYPES.md` | Typed whole-column views (`PqList.dates()` and siblings) resolve and guard the annotation once, when the view is built | NESTED_PRIMITIVE_LEAF_DECODE |
| `LOGICAL_TYPES.md` | An unannotated INT96 decodes as an `Instant` by convention; sound because `FileSchema` drops any annotation INT96 cannot carry | NESTED_PRIMITIVE_LEAF_DECODE |
| `LOGICAL_TYPES.md` | ENUM over BYTE_ARRAY decodes exactly like a string | NESTED_PRIMITIVE_LEAF_DECODE |

## Plans (`_plans/`)

| Plan | Source | Tracking |
|---|---|---|
| `WRITER_SUPPORT.md` | Trimmed WRITER_SUPPORT: ordering rules, completed-stage list, open stages 21b, 22–25, 29, 30, 32–35, 37 | #1291 |
| `REMOTE_READ_PATH.md` | Delivery table of REMOTE_READ_PATH | #1262 |
| `READ_PATH_CONVERGENCE.md` | Draft outside this branch; moves to `_plans/` when committed | #1169 |
| `PARSED_METADATA_REUSE.md` | Draft outside this branch; moves to `_plans/` when committed | #837 |
| `DESIGN_DOC_CONSOLIDATION.md` | This file; deleted when the last area lands | — |

## Disposition per doc

M = merge into target, D = delete (plan for finished work, or superseded), K = keep, P = move to `_plans/`, R = move to root file.

| Doc | → | Target | Staleness evidence |
|---|---|---|---|
| ALWAYS_MATCH_STATISTICS | M | 7, 8 | page-level pre-approve called a follow-up; decided against in #1177 |
| API_CHANGE_REPORT_PUBLISHING | M | 23 | current; drop backfill section |
| AVRO_DECODE_PLAN | M | 15 | current |
| AVRO_GENERICRECORD_SUPPORT | M (table only) | 15 | documents `createAvroRowReader`; API is `AvroReaders`; converter now internal |
| AVRO_NAME_RESOLUTION | M | 15 | current |
| BLOOM_FILTER_PUSHDOWN | M | 7 | current |
| BLOOM_FILTER_SUPPORT | M (1 para) | 7 | plan |
| COALESCED_OFFSET_INDEX_READS | D | 11 (1 invariant) | `ChunkRange`, `PageRange`, `PageScanner` gone |
| COLUMN_INDEX_PUSHDOWN | D | 7 (row-range rule) | `PageScanner`, `FileManager`, `SingleFileRowReader` gone |
| COLUMN_ORDER_HANDLING | M | 7 | current |
| COLUMN_READER_ADAPTIVE_BATCH_SIZING | M | 1 | resolution moved to `ParquetFileReader.resolveBatchSize` |
| COLUMN_READER_ARROW_LAYOUT | M | 2 | `Validity` described as `BitSet`; level getters removed |
| COLUMN_READER_BATCH_API | D | — | superseded by the layer model |
| COLUMN_READ_PIPELINE | M | 2 | current |
| COMPAT_FILTER_SUPPORT | M | 6 | converter is now schema-aware |
| CROSS_COLUMN_COALESCING | M | 11 | gate also requires no dictionary handle |
| DICTIONARY_PUSHDOWN | M | 7 | current |
| DICTIONARY_STRING_REUSE | M | 5 | current |
| DIFFERENTIAL_TESTING | R | TESTING.md | says "Proposed"; implemented in #574 |
| DIVE_LIST_VIEWPORT_VIRTUALIZATION | M | 21 | names `RowWindow.bottomPinned`, which does not exist |
| DIVE_NAVIGATION_MODEL | M (rules) | 21 | conformance §1–7 done; heading-stop rule contradicts `Document.java` |
| DIVE_READ_FAILURE_HANDLING | M | 20 | current |
| DIVE_SIZE_STATISTICS | M | 22 | current; drop plan steps |
| DIVE_THEME | M | 21 | "four `Theme` methods", there are five |
| DOCS_DIATAXIS_STRUCTURE | M | 23 | nav copy misses ~15 pages |
| DOCS_MIGRATION | D | 23 (topology) | plan; names a script that no longer exists |
| DRAIN_SIDE_RECORD_FILTERING | M | 8 | `maxRows` risk contradicts `activeMaxRows`; benchmark snapshot |
| EAGER_BATCH_ASSEMBLY_PLAN | D | — | superseded by PARSING_PIPELINE_V2 |
| ERROR_PRONE_RULE_ENFORCEMENT | M | 24 | current |
| EXACT_COLUMN_READER_FILTERING | M | 8 | cites wrong issues (#74/#70, #196) |
| EXCEPTION_MODEL | K | 13 | #1093 sentence stale |
| FILTER_ONLY_COLUMN_SKIP | M | 8 | current |
| FIXED_SIZE_LIST_FASTPATH | M | 4 | current (opt-in); benchmark tables |
| FLAT_WRITE_BENCHMARK | D | PERFORMANCE.md (method) | stage done |
| FLBA12_TIMESTAMPS | M | 14 | current |
| GEOSPATIAL_SUPPORT | M | 14 | wrong enum name; no writer side |
| INLINE_PAGE_STATS_FALLBACK | M (short) | 7 | names removed `canDropLeaf`; "Hardwood does not write Parquet" |
| INPUT_FILE_ABSTRACTION | M (contract) | 9 | migration plan for removed classes |
| INTEGRATION_TESTS | R | TESTING.md | current |
| INTERACTIVE_DIVE_TUI | M | 20 | forward-only cursor replaced by `PreviewWindow`; cursor rules contradict navigation model |
| JAR_MANIFEST_BUILD_METADATA | M | 24 | current |
| LARGE_FILE_PER_REGION_MAPPING | M | 9 | current |
| LAZY_ROW_GROUP_INITIALIZATION | D | 1 (1 invariant) | every named class gone |
| LOCAL_TIMESTAMP_ACCESSOR | M | 14 | current |
| MULTI_FILE_COLUMN_READER | D | — | `MultiFile*` classes gone; API is `openAll` |
| MULTI_FILE_INCREMENTAL_PLANNING | M | 1 | current |
| NESTED_ALL_PRESENT_PAGE_BULK_COPY | M | 4 | claims byte-array leaves covered; gate excludes them |
| NESTED_PRIMITIVE_LEAF_DECODE | M | 5, 14 | current |
| NESTED_READ_BENCHMARK | R | PERFORMANCE.md | motivation belongs on #732/#750 |
| NESTED_REALVIEW_ON_DRAIN | M | 4 | links a deleted doc; methods moved to `ColumnScan` |
| OFFSET_INDEX_SUPPORT | D | 11 (2 facts) | `PageScanner` gone |
| PARSED_METADATA_REUSE | P | — | in flight |
| PARSING_PIPELINE_V2 | M | 1, 3, 11 | `BitSet` nulls; batch clamp [16K, 512K] removed |
| PER_FILE_METADATA | M | 12 | current |
| POOLED_LEVEL_DECODE_SCRATCH | M | 4 | current |
| PREDICATE_LITERALS | M (core) | 6 | current |
| PREDICATE_PUSHDOWN | D | 6, 7 (rules) | `createRowReader(filter)` gone; `ColumnIndex` shape changed |
| PREVIEW_WINDOW | M | 20 | "PR: this branch" |
| READER_CONFIG | M | 1 | current |
| READ_PATH_CONVERGENCE | P | — | in flight |
| RECORD_FILTER_COMPILATION | M (short) | 8 | names removed `RecordFilterEvaluator` |
| REMOTE_RANGE_BACKING | M | 10 | "#75 tracks multi-region" — closed |
| REMOTE_READ_PATH | M + P | 11 + plan | current; delivery table in flight |
| ROW_BASED_SEEK | M | 3 | "multi-file skip out of scope" — now supported |
| ROW_READER_AUGMENTED_PROJECTION | M | 8 | current |
| ROW_SELECTION_SEMANTICS | M (short) | 8 | "In progress"; only #542 open |
| S3_OBJECT_STORAGE | D | 10 (1 note) | describes the AWS SDK client, removed in #144 |
| S3_ZERO_SDK | M (base) | 10 | builder options missing; tail fetch moved to `S3Fetcher` |
| SEQUENTIAL_FETCH_PLAN_PAGE_MASKING | M | 11 | current |
| SHARED_VALUE_RENDERER | M (core) | 22 | current |
| SIMD_VECTOR_API_PLAN | D | 5 (~5 lines) | wrong paths; phases 4–5 never built |
| SIZE_STATISTICS_AND_NAN_COUNTS | M (parse rules) | 7 | "no read path consults them" — two do |
| THRIFT_METADATA_PARSER_HARDENING | M | 12 | current |
| UNANNOTATED_REPEATED_LISTS | M | 4 | current |
| UNIT_STATISTICS_CONVERGENCE | M (spine) | 7 | current |
| UNREADABLE_SORT_ORDER_BOUNDS | M | 7 | current |
| VALIDITY_WORD_BITMAP | M | 2 | current; drop change list |
| VARIANT_LOGICAL_TYPE | M | 14 | wrong package for reassembler; eager copy, not lazy |
| WRITER_ANNOTATION_RANGES | M | 17 | current |
| WRITER_CODECS_AND_ENCODINGS | M | 18 | current |
| WRITER_DICTIONARY | M (layout) | 18 | mid-chunk fallback, `dictionaryPageLimitBytes` gone |
| WRITER_DICTIONARY_EARLY_ABANDONMENT | M (1 para) | 18 | rationale cites removed cap |
| WRITER_DICTIONARY_SELECTION | M | 16, 18 | still describes the removed cap |
| WRITER_DOCS | D | — | plan; done |
| WRITER_INTEROP_GATE | M | 19 | mentions removed PLAIN fallback |
| WRITER_LOGICAL_TYPES | M | 17, 18 | current |
| WRITER_NESTED | M | 17 | INT32-only framing; misses stage-36 batch rule |
| WRITER_PRIMITIVE_TYPES | M | 16, 17, 18 | PLAIN-estimate sizing replaced |
| WRITER_ROW_API | M | 17 | "parquet-java and PyArrow" — PyArrow not in the gate |
| WRITER_ROW_GROUP_SIZING | M | 16 | current |
| WRITER_SUPPORT | M + P | 16 + plan | names `PageBuilder` (never built), `S3OutputFile` (absent) |
| WRITE_COVERAGE_ASSERTION | M | 19 | current |
| WRITE_PATH_BENCHMARK_COVERAGE | D | plan (21b row) | 21a done, 21b never built |

## Decisions

- [x] **Writer follow-up epic.** #9 stays closed as shipped. Epic #1291 "Writer delivery stage 2" tracks the open stages and the open writer issues #1253, #1045, #1147, #957, #791. Also: #989 is closed with 21b unbuilt; #1021 is open although implemented.
- [x] **`LOGICAL_TYPES.md` covers read and write.** Writer-specific legality (physical×logical table, range checks) stays in `WRITER_INPUT.md`, which links to it.
- [x] **One `DIVE_UI_RULES.md`** rather than keeping `DIVE_THEME.md` separate. CLAUDE.md's three dive links become anchors.
- [x] **Benchmark result tables** in design docs are dropped (FIXED_SIZE_LIST_FASTPATH, NESTED_REALVIEW_ON_DRAIN, DRAIN_SIDE_RECORD_FILTERING, RECORD_FILTER_COMPILATION, FLBA12_TIMESTAMPS). Numbers go stale with the code; the method lives in PERFORMANCE.md.

## Code/doc drift found

Code defects and JavaDoc drift found during the survey; fixed independently of the doc work.

- [x] `Theme.error()` reads `$COLORTERM` directly and ignores `hardwood.dive.truecolor`, unlike `accent()`/`selection()` (`cli/.../dive/internal/Theme.java:127`). Screenshot runs render named-ANSI red.
- [x] Navigation rule 1 says headings and blanks are cursor stops; `Document.java` says they are not. Decide which is intended.
- [x] CLAUDE.md names `RowWindow.bottomPinned`; the API is `RowWindow.from(scrollTop, selection, total, viewport)` plus `adjustTop`.
- [x] Stale JavaDoc: `ColumnChunkBuffer` (pages cut "while records arrive"), `ParquetMetadataReader` and `jfr/FileOpenedEvent` (`MultiFileRowReader`, `FileManager`), `DictionaryParser`/`PageDecoder`/`ColumnIndexBuffers` (`PageScanner`), `FileSchema.validateVariantGroup` ("Phase 2").
- [x] `LogicalAccessorKind:91` links COALESCED_OFFSET_INDEX_READS; repointed with the I/O area.
- [x] Unused: `SimdOperations.markNulls`, `unpackBitWidth1`, `unpackBitWidthN` (both implementations, no production caller).

## Links to repoint

Every consolidation PR repoints links to the docs it removes. Current inbound links from outside `_designs/`:

- `CLAUDE.md`: DIVE_THEME, DIVE_NAVIGATION_MODEL, DIVE_LIST_VIEWPORT_VIRTUALIZATION, DOCS_DIATAXIS_STRUCTURE.
- `ROADMAP.md`: 18 links.
- `CONTRIBUTING.md`: INTEGRATION_TESTS.
- Source: `RangeBacking`, `RangeBackedInputFile`, `ParquetModel` (REMOTE_RANGE_BACKING); `Theme`, `ColumnChunkDetailScreen` (DIVE_THEME); `SchemaScreen` (DIVE_LIST_VIEWPORT_VIRTUALIZATION); `LogicalAccessorKind` (EXCEPTION_MODEL); `DifferentialReadTest`, `ReadProjectionTest`, `WriterDictionaryTest`, `DictionaryPushDownBenchmark`; the writer interop and coverage tests in `parquet-testing-runner`; `tools/simple-datagen.py`; `tools/predicate-audit`.
- `.claude/skills/hardwood-review/references/checklist.md` (C1/C2 rules).

## Method

Old docs are the source of intent; the code is the source of fact. Each target doc starts from the contracts and invariants of its source docs, and every claim is checked against the code:

| Outcome | Action |
|---|---|
| Doc and code agree | State it |
| Code differs; a later doc, issue or commit records the change as deliberate | State the current behaviour |
| Code differs; nothing records the change | Divergence: put to the maintainer in the session, with a concrete example, before the PR is opened. A code fix it calls for gets its own issue and PR, merged first; the doc then states the result |

A design doc states what would need a design discussion to change: contracts, invariants, ownership, the reasoning behind them. Tuning constants and class internals stay out; class names appear only as pointers to where something lives. Tests are named at most once per section, as a line listing the test classes that cover it; an invariant no test enforces is marked untested.

## Execution

One area per session, with the `hardwood-design-consolidation` skill (`.claude/skills/hardwood-design-consolidation/`). This checklist is the hand-over point between sessions: an area is ticked in the PR that consolidates it. An area in progress keeps its working notes (divergences, answers, branches) in the local, gitignored `_reviews/1290-<area>.md`. Each PR is opened only once every divergence is resolved, so it carries no open decisions.

- [x] Rules and `_plans/` split: CLAUDE.md, CONTRIBUTING.md, review checklist; WRITER_SUPPORT and REMOTE_READ_PATH moved to `_plans/`
- [x] `_designs-legacy/` for the unconsolidated docs; test attribution once per section
- [x] Filtering (6–8); closes #1110
- [x] CLI and infrastructure (20–24), `TESTING.md`, `PERFORMANCE.md`; repoints CLAUDE.md's dive and Diátaxis rules
- [x] Read path (1–5)
- [x] I/O and metadata (9–13); splits the design part out of `_plans/REMOTE_READ_PATH.md`
- [ ] Writer (16–19); trims `_plans/WRITER_SUPPORT.md` to the open stages of #1291
- [ ] Types (14–15)
- [ ] `ARCHITECTURE.md` pointers; delete `_designs-legacy/`, this plan and the skill
