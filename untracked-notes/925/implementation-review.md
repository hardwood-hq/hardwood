# Implementation review — #925

## Summary
- The two commits add a version-pinned parquet-avro oracle, an internal compatibility conversion entry point with per-conversion name counters, focused schema/materialization tests, an implemented design, and a roadmap entry. Native `AvroReaders` remains on `AvroSchemaConverter.plan(...)`.
- Overall assessment: Not ready. The production seam matches the measured rule in the covered cases, but the oracle does not enforce the plan's lossless source-accounting invariant and the differential tests cover only ordinary nested records. Several explicit Stage 2 preservation and serialization contracts are absent.

## Blockers

No blocker findings.

## Major findings

- [x] **IR-001 — The oracle does not account for every physical source node**
  - Location: `parquet-testing-runner/src/test/java/dev/hardwood/testing/AvroNamedTypeOracleTest.java:149-166`, `AvroNamedTypeOracleTest.renderRoot` at lines 190-193, and `AvroNamedTypeOracleTest.renderType` at lines 206-241
  - Problem: The plan requires a source descriptor tree with direct `Type` references and explicit roles, recursively validated against the complete `MessageType` before conversion. The implementation instead derives sites while walking the source and Avro outputs. For Variant, it checks only that the Avro result has `metadata` and `value` fields and then returns; it never validates the physical source children, their order/types, the `typed_value` subtree, or that suppression occurs only below `typed_value`. For example, removing the named `address` node from the shredded fixture's `typed_value` subtree would leave the rendered golden unchanged and the test green.
  - Why it matters: Global invariants 2 and 3 and Stage 1's completion contract depend on proving that no source named-type occurrence disappears from the golden through an unmeasured transform. The current oracle can report the expected output while failing to establish the Variant suppression behavior that production code copies.
  - Expected correction: Build and recursively validate the planned source descriptors, including ordinary/list/map roles, canonical Variant child mapping, and explicit recursive suppression of only the shredded `typed_value` subtree. Render source sites and raw counter keys from those validated descriptors.
  - Test requirement: Add mutation-sensitive assertions showing that missing, reordered, renamed, or structurally mismatched Variant children and suppressed descendants fail descriptor validation, while the accepted shredded fixture validates every physical node and emits no occurrence for the named suppressed site.

- [x] **IR-002 — Differential parity covers only one ordinary-record schema**
  - Location: `parquet-testing-runner/src/test/java/dev/hardwood/testing/AvroNamedTypeOracleTest.java:80-104`
  - Problem: `hardwoodCompatibilityMatchesReferenceForNestedAndProjectedSchemas()` compares only the repeated-address fixture and a projection that removes `work`. It does not compare Hardwood against parquet-avro for the other supported Stage 1 cases: identical records, LIST elements, MAP values, canonical/shredded Variant, plain/decimal/logical fixed, INT96 fixed mode, qualified roots, or insertion-sensitive ordering. It also omits the planned projected ordinary/list/map pruning differential and key-only MAP completion differential.
  - Why it matters: Stage 2's core completion contract is parity with every supported oracle result. Direct Hardwood-only assertions cannot detect a shared mistaken expectation or a compatibility branch that diverges from parquet-avro outside ordinary records.
  - Expected correction: Construct equivalent `FileSchema` inputs for every supported oracle fixture and compare the normalized named-type sequence and attributes against the genuine converter. Add the projection differential with ordinary/list/map sibling pruning and the separate valid full-value oracle input for Hardwood's key-only MAP projection.
  - Test requirement: Each supported golden section must participate in a differential assertion, including the literal shredded-Variant external collision, fixed sizes/logical annotations, qualified names, insertion renumbering, projected sequence, and retained key-only MAP value occurrence.

- [x] **IR-003 — Projected plan preservation is not verified across the promised traversal shapes**
  - Location: `avro/src/test/java/dev/hardwood/avro/internal/AvroSchemaConverterTest.java:303-333` and `avro/src/test/java/dev/hardwood/avro/AvroRowReaderTest.java:956-1000`
  - Problem: The converter tests cover one projected ordinary-record path and one key-only MAP, but they do not exercise the planned synthetic schema containing a removed earlier occurrence plus retained ordinary, LIST-element, and MAP-value records with pruned siblings. They compare source names rather than exact `SchemaNode` object identity and do not recursively compare native and compatibility plans for field order, nullable unions, logical annotations, fixed sizes, kinds, and contained-plan alignment. The materialization regression uses only the map fixture, so no projected list-record path is covered.
  - Why it matters: Compatibility naming was inserted into the same traversal that builds `AvroPlanNode`. A counter-correct schema can still misalign decode positions or retain the wrong projected subtree. Global invariant 7 and the Stage 2 completion contract explicitly require those non-naming properties to remain identical.
  - Expected correction: Add the planned combined projection fixture and recursively compare native and compatibility plan/schema trees after normalizing only named identifiers. Assert `source()` identity with `isSameAs`, all `Kind` values, field positions, union shape, logical annotations, fixed sizes, list/map alignment, and the core projected leaf set. Add materialization coverage for a projected record reached through a list or map with a sibling pruned.
  - Test requirement: The tests must fail independently for a shifted child position, copied/replaced source node, changed kind or logical annotation, retained unprojected sibling, or omitted named occurrence after projection.

- [x] **IR-004 — Duplicate fixed definitions are absent from the serialization contract test**
  - Location: `avro/src/test/java/dev/hardwood/avro/internal/AvroSchemaConverterTest.java:273-283`
  - Problem: `parquetAvroCompatibilitySerializesDistinctSameNamedDefinitions()` uses `repeatedAddressSchema()`, which supplies different same-named record definitions only. It does not create repeated fixed types with distinguishable widths or logical parameters, despite the acceptance criterion and plan requiring both record and fixed collisions. It also does not assert `Schema.toString()` separately.
  - Why it matters: Avro's named fixed handling is a separate construction path (`AvroSchemaConverter.fixed`) and can fail through size/logical-definition collisions even when duplicate records serialize. The stated acceptance criterion remains unverified.
  - Expected correction: Build one compatibility schema with duplicate source fixed names whose legal definitions differ, alongside the differing record pair, and exercise both `Schema.toString()` and an empty `DataFileWriter.create(...)` header.
  - Test requirement: Assert the fixed occurrences have the literal oracle names/namespaces, retain their distinct sizes/logical parameters, and serialize without Avro treating one definition as a back-reference to the other.

## Minor findings

No minor findings.

## Nits

No nit findings.

## Verified invariants

- The implementation range is `a2c68b58..9cc386cf`, consisting of the two planned `#925` commits in stage order; both commit messages have no AI co-author trailer.
- `AvroSchemaConverter.planForParquetAvroCompatibility(...)` is under `dev.hardwood.avro.internal`, creates fresh resolver state per invocation, and exposes no builder or CLI option.
- The compatibility counter is invocation-local; no static, cached, or `ThreadLocal` counter state exists.
- Named record and fixed construction in `avro/src/main` routes through the selected naming mode. UUID remains an Avro string and does not call the resolver.
- Records resolve before child traversal, Variant resolves once without recursing through physical children, and map conversion retains its value plan for key-only projection.
- `AvroReaders.RowReaderBuilder.build()` still calls only the native two-argument `AvroSchemaConverter.plan(...)`; semantic references show compatibility selection only in tests.
- Native INTERVAL/FLOAT16 names remain canonical while compatibility conversion emits their source names.
- The dependency direction is one-way and test-scoped from `parquet-testing-runner` to `hardwood-avro`; `parquet-java-compat` is unchanged.
- The runner selects parquet-avro `1.17.1` and Avro `1.11.5`, while the production `hardwood-avro` module retains Avro `1.11.4`.
- The design status is `Implemented`, and the roadmap leaves the separate `AvroParquetReader` item unchecked.
- The changed-file boundary matches the two stages' expected scope, and no public user documentation was required for the internal entry point.

## Verification observed

- `mtk mvn -pl avro -am -Dtest=AvroSchemaConverterTest,AvroRowReaderTest -Dsurefire.failIfNoSpecifiedTests=false test`: passed, 58 tests.
- `mtk mvn -pl parquet-testing-runner -am -Dtest=AvroNamedTypeOracleTest -Dsurefire.failIfNoSpecifiedTests=false test`: passed, including 2 oracle tests and the upstream Avro tests selected by the reactor.
- `mtk mvn -pl parquet-testing-runner -am validate`: passed.
- `mtk mvn -pl parquet-testing-runner -am dependency:tree -Dverbose -Dincludes=dev.hardwood:hardwood-avro,org.apache.avro:avro,org.apache.parquet:parquet-avro`: passed and showed the intended test dependency, parquet-avro `1.17.1`, and runner Avro `1.11.5`.
- `mtk mvn process-sources`: passed with imports already sorted in the affected modules.
- `mtk mvn verify`: passed, 3,185 tests run with 66 skipped.
- `git diff --check a2c68b58..9cc386cf`: passed.
- IntelliJ semantic references and text searches accounted for both conversion entry points and every production `Schema.createRecord`/`Schema.createFixed` site. Closed-file diagnostics reported no compiler errors confirmed by Maven; its inaccessible-link JavaDoc diagnostics are inconsistent with the successful project build.

## Resolution

- **IR-001 — Resolved:** `AvroNamedTypeOracleTest` now constructs direct-reference source descriptors, validates every physical child and role before conversion, validates canonical Variant fields and recursively validates the complete `typed_value` subtree, and includes mutation-sensitive missing, reordered, and renamed Variant descriptor assertions. Focused oracle test passes.
- **IR-002 — Resolved:** The runner now differentially compares every supported Stage 1 fixture, projected ordinary/list/map pruning, and the valid full-value oracle against a key-only MAP Hardwood projection. Named-type kind and fixed-size attributes are compared as well as full names. Focused oracle test passes.
- **IR-003 — Resolved:** `AvroSchemaConverterTest` now recursively compares native and compatibility projected plans across retained ordinary, LIST-element, and MAP-value records, including exact source identity, kinds, schema shape, fixed metadata, and child alignment. `AvroRowReaderTest` adds projected LIST-element materialization parity. Focused Avro tests pass.
- **IR-004 — Resolved:** `AvroSchemaConverterTest` now creates duplicate fixed definitions with sizes 4 and 8, asserts their compatibility names and sizes, calls `Schema.toString()`, and creates an empty `DataFileWriter` header. Focused Avro tests pass.
