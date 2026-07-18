<!--

     SPDX-License-Identifier: Apache-2.0

     Copyright The original authors

     Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0

-->
# Micro-benchmarks

JMH benchmarks isolating individual read-path costs (predicate dispatch, page
decode, level handling, SIMD kernels, filtered scans). For polished, publication-grade
end-to-end comparisons against parquet-java, use
[hardwood-benchmarks](https://github.com/hardwood-hq/hardwood-benchmarks) instead.

## Running

```shell
./mvnw -pl core install -DskipTests
./mvnw -pl performance-testing/micro-benchmarks package -Pperformance-test
java -jar performance-testing/micro-benchmarks/target/benchmarks.jar <BenchmarkName> [-p dataDir=<dir>]
```

Every benchmark class carries its exact run recipe (including any extra
parameters) in its JavaDoc header. Standard JMH flags apply: `-prof gc` for
allocation profiling, `-rf json -rff out.json` for machine-readable results,
`-p name=value` to override any `@Param`.

## The benchmarks

| Benchmark | Measures | Fixture |
| --- | --- | --- |
| `RecordFilterMicroBenchmark` | Per-row predicate compile + dispatch cost across predicate shapes, no I/O | none (in-memory) |
| `SimdBenchmark` | Scalar vs. Vector API kernels (null counting, dictionary lookup, …) | none (in-memory) |
| `ValidityIterationBenchmark` | Null-bitmap iteration strategies over a `long[]` column | none (in-memory) |
| `AlwaysMatchReadBenchmark` | Filtered end-to-end reads when statistics prove row groups match in full (#795) | `generate_filter_pushdown_data.py` |
| `DictionaryStringReadBenchmark` | Allocation per full-column scan of a dictionary-encoded string column (run with `-prof gc`) | `generate_dict_string_data.py` |
| `PageScanBenchmark` | Sequential header-based page scan vs. offset-index lookup | `generate_benchmark_data.py` |
| `PageHandlingBenchmark` | Per-page decompression vs. full page decode | taxi data, downloaded by `./mvnw verify -Pperformance-test` |
| `MemoryMapBenchmark` | Raw I/O floor: mmap + copy of a whole file, no decode | taxi data, downloaded by `./mvnw verify -Pperformance-test` |
| `FixedSizeListDecodeBenchmark` | Fixed-size-list fast path vs. general list decode across vector widths | `generate_fixed_size_list_data.py` |
| `FixedSizeListFallbackBenchmark` | Detector cost on almost-fixed-width pages that fall back | `generate_fixed_size_list_data.py` |
| `nested/NestedListReadBenchmark` | `LIST<primitive>` reads across element types and null densities vs. a flat floor | self-generating (`NestedListFileGenerator`) |
| `nested/NestedMultiListReadBenchmark` | Multi-list-column schema effects on the nested read path | self-generating (`NestedListFileGenerator`) |
| `mixed/MixedSchemaReadBenchmark` | Schema-composition effects (scalars next to lists, structs, depth) on the nested path (#732) | self-generating (`MixedSchemaFileGenerator`) |

Python generator scripts live in the parent `performance-testing/` directory and
default their output to `performance-testing/test-data-setup/target/benchmark-data`;
pass that directory as `-p dataDir=...`. Fixture generation is idempotent —
existing files are skipped.

## Correctness gates

The nested and mixed-schema benchmarks have companion gate classes
(`NestedListGate`, `MixedSchemaGate`) that generate their corpus and assert all
read paths produce identical folds. Run the gate before trusting timings from
its benchmark.
