# Build infrastructure

This document covers the quality and packaging infrastructure of Hardwood's Maven build: the `qa` profile and the checks it runs, the project-local Error Prone checks in `error-prone-checks/`, how the CI jobs resolve and cache what they build against, the build metadata stamped into every JAR, and the rule that keeps plugin versions in the parent POM. It does not cover the test suites and how they are split between Surefire and Failsafe ([TESTING.md](../TESTING.md)), the performance tests ([PERFORMANCE.md](../PERFORMANCE.md)), the GraalVM native CLI build ([NATIVE_BUILD.md](../NATIVE_BUILD.md)), or the documentation site, the prose check and the API change reports ([DOCUMENTATION.md](DOCUMENTATION.md)).

## Module layout

| Location | Role |
|---|---|
| `pom.xml` (parent) | `<pluginManagement>` for every plugin, the `qa` and `quick` profiles, the `capture-git-info` execution, the JAR manifest configuration |
| `error-prone-checks/` (`hardwood-error-prone-checks`) | The project-local Error Prone checks; build-only, never deployed |
| `.mvn/jvm.config` | `jdk.compiler` exports and opens that Error Prone needs inside the Maven JVM |
| `.mvn/maven.config` | Reactor parallelism (`-T`) and the number of test JVMs per module (`-DforkCount`) |
| `etc/` | License header templates and the Eclipse formatter configuration |
| `.github/workflows/` | CI; `pr-build.yml` gates pull requests, `main-build.yml` builds `main` and seeds the caches the PR build reads |

`hardwood-error-prone-checks` is the first module of the reactor. It sets `maven.deploy.skip`, because the checks run only during this repository's compilation and no published artifact depends on them at runtime.

### Reactor ordering of the checks

The checks reach `javac` through the compiler plugin's `annotationProcessorPaths` (see [The `qa` profile](#the-qa-profile)), and the Maven reactor does not read a processor path when it orders modules. In a parallel build nothing would then keep a module from compiling while the checks are still being built, and a build against a local repository without the checks artifact would fail to resolve them.

Every module that compiles Java in the default reactor (`core`, `s3`, `aws-auth`, `avro`, `cli`, `test-support`, `parquet-java-compat`, `integration-test`, `parquet-testing-runner`) therefore declares `hardwood-error-prone-checks` as a `test`-scoped dependency. No code imports it. The dependency is an edge the reactor orders on, and test scope keeps the build tool off the main compile classpath and out of the flattened POM. The parent POM cannot carry the dependency, because the checks module inherits from it and would then depend on itself. Modules outside the default reactor (`performance-testing/*`, `tools/predicate-audit`) depend on `hardwood-core` and are ordered after the checks through it.

A new module that compiles Java and does not depend on another Hardwood module must declare the same dependency. Untested: only a cold local repository exposes a missing edge.

## The `qa` profile

The parent POM's `qa` profile carries every check that verifies the build rather than producing it. It activates on `!quick`: a plain `./mvnw verify` runs it, and `-Dquick` turns it off while also activating the `quick` profile, which sets `skipTests`. `-Dquick` changes what is verified, not what is compiled: the JARs are the same with and without it.

| Plugin | Phase | What it does |
|---|---|---|
| `maven-artifact-plugin` `check-buildplan` | `validate` | Fails when a plugin in the build plan is a version known to produce non-reproducible output |
| `maven-enforcer-plugin` `enforce-plugin-versions` | `validate` | Requires the build JDK to be `java.version.build` (25) or later, Maven 3.5.0 or later, and an explicit, non-`LATEST`/`RELEASE`/`SNAPSHOT` version for every plugin |
| `impsort-maven-plugin` `sort` | `process-sources` | Rewrites imports into the groups `java.`, `javax.`, `org.`, `com.`, then others, statics last, and removes unused ones |
| `maven-compiler-plugin` (Error Prone) | `compile`, `test-compile` | Runs the project-local checks (see below) |
| `license-maven-plugin` (`com.mycila`) `check` | `verify` | Fails on a file whose header does not match its template |
| `formatter-maven-plugin` `format` | `none` | Declared with the configuration in `etc/eclipse-formatter-config.xml`; the build does not run it |

`impsort` rewrites files in place; it does not fail the build on unsorted imports. The formatter is not bound to any phase, so no build step formats code.

The license check runs once for the whole tree (`aggregate`) from the root, with two header sets: `etc/license.txt` (Apache-2.0, `SPDX` form) for code, build files, workflows and tools, and `etc/license-cc-by-sa.txt` for `docs/content/`. Its includes are explicit patterns rather than `**`: `*/src/**` covers any new module, and a new top-level directory needs its own include line. Scratch and vendored directories (`.claude/`, `_tmp/`, `.docker-venv/`, `docs/site/` and similar) are excluded in both sets.

### Where `qa` runs

| Build | `qa` active |
|---|---|
| Local `./mvnw verify` | Yes |
| PR and main CI: the step that builds a job's own modules | Yes |
| PR and main CI: the dependency step of each job | No (`-Dquick`) |
| `performance.yml` install step | Yes |
| Release (`release.sh`: `release:prepare` and `release:perform`) | No: `preparationGoals` and `arguments` carry `-Dquick` |
| Native CLI packaging (`cli-early-access.yml`, `release-cli.yml`) | Yes |

Each module is verified under `qa` by exactly one CI job, the one that owns it (see [CI dependency closure](#ci-dependency-closure)). A release builds the tagged commit without `qa`; that commit's checks ran in CI when it reached `main`.

## Error Prone checks

The `qa` profile adds Error Prone to `javac` as a compiler plugin and puts `error_prone_core` and `hardwood-error-prone-checks` on the processor path. It passes `-XepDisableAllChecks` and then enables the project checks one by one at `ERROR`, so Error Prone's built-in bug patterns do not run. A violation is a compile error naming the file, the line, the check and a one-line reason.

| Check | Rule | Rationale |
|---|---|---|
| `NoVar` | No local-variable type inference: a variable declared with `var` (locals, enhanced-`for` variables, try-with-resources resources) is an error | The "Coding" rules in [CLAUDE.md](../CLAUDE.md) require explicit types |
| `NoLegacyJavadoc` | No `/**` in a source file; JavaDoc uses the Markdown `///` form (JEP 467) | The "Documentation" rules in [CLAUDE.md](../CLAUDE.md) require Markdown JavaDoc |

`NoLegacyJavadoc` matches the character sequence `/**` anywhere in the file text, not a parsed comment, and reports the first occurrence per file. A `/**` inside a string literal or a line comment is an error too. Ordinary block comments (`/* */`) are allowed. The remaining Markdown JavaDoc conventions (`[ClassName]` links, fenced code blocks) are not checked by Error Prone; `javadoc:javadoc`, which runs in the `javadoc-and-integration-tests` CI job with doclint set to `all,-missing`, rejects JavaDoc that does not parse.

Both checks apply only to files under a conventional source root, `/src/(main|test)/java<digits>/` (`JavaSourceFiles`). This covers multi-release roots such as `core/src/main/java22/` and skips generated sources under `target/`, which the project does not control. The checks cover main and test sources of every module compiled with `qa` active, except `error-prone-checks` itself.

Adding a check means a new `BugChecker` in `dev.hardwood.build.errorprone`, registered through `@AutoService`, and a `-Xep:<Name>:ERROR` entry in the parent POM's `-Xplugin:ErrorProne` argument. A check that is not named there does not run.

### Wiring

- `error-prone-checks/pom.xml` overrides the inherited `compilerArgs` and `annotationProcessorPaths` (`combine.self="override"`), so the checks module is not compiled with the checks it defines. It clears `maven.compiler.release` and compiles with `source`/`target`, because `javac` rejects `--release` combined with `--add-exports` of system modules.
- Modules that add their own compiler arguments or processors (`core`'s `compile-java22` execution, `performance-testing/micro-benchmarks`) use `combine.children="append"`, so the `qa` arguments are kept.
- Error Prone inspects the `javac` AST through internal `jdk.compiler` packages. The compiler runs inside the Maven JVM, which gets the required `--add-exports`/`--add-opens` from `.mvn/jvm.config`; the checks module's own compilation declares them in its `compilerArgs`, and its tests, which run `javac` in-process through `CompilationTestHelper`, reuse `.mvn/jvm.config` as their Surefire `argLine`.

Tests: `NoVarTest`, `NoLegacyJavadocTest` (error-prone-checks).

## CI dependency closure

`pr-build.yml` and `main-build.yml` share one scheme. No job waits for another or downloads another job's output; each job builds the Hardwood modules it needs itself, so the wall clock of a workflow is its slowest job rather than a chain of jobs.

Each job:

1. Restores `~/.m2/repository` from the Actions cache.
2. Deletes `~/.m2/repository/dev/hardwood`.
3. Builds its dependency closure with `-Dquick install -pl <its modules> -am`. `build-core`, whose modules have no Hardwood dependencies outside its own list, skips this step and names `hardwood-error-prone-checks` in its `-pl` list instead. `javadoc-and-integration-tests` installs every module except `integration-test` (`-pl '!:hardwood-integration-test'`, no `-am`), because `javadoc:javadoc` runs over the whole reactor.
4. Builds and verifies its own modules without `-Dquick`: `clean install -fae`, `clean verify` for the integration tests, and `-Dnative verify -Dgroups=native` in `native-build-check`, which runs only the `native`-tagged ITs against the binary it builds.

The job that owns a module is the one that verifies it under `qa` and runs its tests; every other job only compiles it under `-Dquick`.

| Job | Owns |
|---|---|
| `build-core` | `error-prone-checks`, the BOMs, `test-support`, `core`, `avro`, `parquet-testing-runner` |
| `build-modules` | `s3`, `aws-auth`, `cli`, `parquet-java-compat`; compiles `predicate-audit` and `performance-testing` (see below) |
| `javadoc-and-integration-tests` | `integration-test`; runs `javadoc:javadoc` over the reactor |
| `native-build-check` | The native CLI binary and the `native`-tagged integration tests |
| `predicate-audit` (PR only) | The predicate audit, run only when a change can move its findings |

Where a job uses `-am`, it bounds the dependency build to the job's own closure, so the job does not compile or resolve dependencies of modules it does not use. Because every compiling module has a test-scoped dependency on `hardwood-error-prone-checks` (see [Reactor ordering of the checks](#reactor-ordering-of-the-checks)), `-am` puts the checks module into every closure. An imported BOM is not followed by `-am`, and needs no place in the closure: Maven resolves it from the source tree.

The performance-testing modules and `tools/predicate-audit` are in the reactor only under their profiles (`-Pperformance-test`, `-Ppredicate-audit`). `build-modules` compiles them (`test-compile` and `compile`) on every change, so an API change that breaks them fails the gate; the benchmarks and the audit themselves run elsewhere.

### Why cached Hardwood artifacts are deleted

The cache restores the local repository of an earlier run, including the `dev.hardwood` artifacts it installed. A module that a job's closure fails to build would otherwise resolve to that stale JAR, and the job would compile against an earlier run's code without error. Deleting `dev/hardwood` after the restore turns a gap in the closure into a resolution failure, and leaves the cache serving third-party dependencies only.

### Cache keys

- The key is `maven-<os>-<job>-<hash of all pom.xml>`, with fallbacks to the same job under any POM hash, then to related jobs, then to any Maven cache.
- `main-build.yml` saves its caches (`actions/cache`). `pr-build.yml` only restores them (`actions/cache/restore`): a pull request can restore caches saved on its own ref or on `main`, and a cache saved by a PR would serve only re-runs of that PR while taking space from `main`'s caches and the performance datasets.
- Every PR job therefore has a counterpart in `main-build.yml` under the same job name that resolves at least the same dependencies. The PR-only `predicate-audit` job falls back to the `build-modules` cache, which is why `build-modules` compiles `predicate-audit` on `main` as well. A job added to `pr-build.yml` needs such a counterpart, or every run downloads what the cache lacks.

The closure is not offline: a cache miss or a changed POM resolves third-party artifacts from Maven Central during the run.

### Other workflows

- `performance.yml` installs `hardwood-error-prone-checks`, `core`, `avro`, `s3` and `aws-auth` with `-am` under `qa`, then runs the performance tests with `-amd`. It keeps its own Maven cache.
- `cli-early-access.yml` runs a full `clean install` in one job, uploads `~/.m2/repository/dev/hardwood` as a workflow artifact, and the per-OS native packaging jobs download it. This is the one workflow whose jobs pass Hardwood artifacts between each other.
- `release-cli.yml` builds each native package from the release tag with `-pl :hardwood-error-prone-checks,:hardwood-cli -am`.
- `release.yml` runs `release.sh` (see [RELEASING.md](../RELEASING.md)).

All actions are referenced by commit SHA.

Untested: no check enforces that a PR job has a `main` counterpart or that a closure is complete; the `dev/hardwood` deletion makes an incomplete closure fail the job.

## Manifest build metadata

Every JAR the build produces carries in `META-INF/MANIFEST.MF`:

| Attribute | Value | Source |
|---|---|---|
| `Implementation-Title` | `${project.name}` | Maven Archiver `addDefaultImplementationEntries` |
| `Implementation-Version` | `${project.version}` | Maven Archiver `addDefaultImplementationEntries` |
| `Implementation-Build` | Short (7-character) commit SHA of `HEAD`, or `unknown` | `revision` property, an explicit `manifestEntries` entry |

The configuration sits in the parent's `<pluginManagement>` entry for `maven-jar-plugin`, so it applies to every JAR module without per-module setup. Modules add their own entries (`Automatic-Module-Name`, `Multi-Release`) next to it; Maven's default merge of `manifestEntries` keeps the inherited `Implementation-Build`. Maven Archiver also writes `Created-By`, `Build-Jdk-Spec` (the build JDK's specification version) and `Java-Version`.

A JAR found on a classpath, in a bug report or in a dependency tree can be traced to the exact commit it was built from.

### Capturing the revision

The `capture-git-info` execution of `maven-antrun-plugin` is bound to `initialize` in the parent's `<build><plugins>`, so every module runs it, under `-Dquick` too. It runs `git rev-parse --short=7 HEAD` and `git status --porcelain` in the repository root and exports two properties:

| Property | Value |
|---|---|
| `revision` | The short SHA; `unknown` when `git` is missing, fails, or prints nothing (a build outside a checkout, such as an unpacked source archive) |
| `revisionDirty` | `true` when `git status --porcelain` lists any tracked or untracked change, otherwise `false` (also `false` when `git` fails) |

Both invocations are best effort: a missing `git` never fails the build.

### Consumers

The library does not read the manifest. `hardwood-core` gets the same values through resource filtering: `src/main/resources-filtered/dev/hardwood/build-info.properties` carries `project.version`, `project.revision` and `project.revision.dirty`, and is the only filtered resource root, which keeps token substitution away from the native-image configuration under `src/main/resources`. `dev.hardwood.internal.BuildInfo` reads it and maps a missing, blank or unsubstituted (`${...}`) value to `unknown` rather than failing; the identifier is descriptive, so an unidentifiable build shows as `unknown` instead of being mistaken for a known one.

| Consumer | Form |
|---|---|
| `ParquetFileWriter` default `created_by` | `hardwood version <version> (build <sha>[-dirty])` |
| `hardwood --version` (`cli` `Version`) | `<version> (<sha>[-dirty])` |
| JAR manifest `Implementation-Build` | `<sha>`, without the dirty mark |

Tests: `BuildInfoTest`, `WriterFooterMetadataTest`, `HardwoodCommandTest` (cli). The manifest attributes themselves are untested.

### Reproducibility

`project.build.outputTimestamp` in the parent POM fixes the timestamps of JAR entries; the release plugin updates it on each release. `check-buildplan` (in `qa`) rejects plugin versions known to break reproducible output. The manifest metadata is compatible with a byte-identical rebuild:

- `Implementation-Title` and `Implementation-Version` derive from the POM.
- The commit SHA is a function of the commit being built. No build timestamp or host name is added; `Build-Jdk-Spec` records only the JDK's major version, so a rebuild must use the same major JDK.
- `revisionDirty` is `false` for a clean checkout of a release tag, and the manifest does not carry it.

A rebuild from a source archive without `.git` resolves `revision` to `unknown` and does not reproduce a released JAR; reproduction is from a checkout of the release tag. No CI job rebuilds and compares artifacts: reproducibility rests on the fixed timestamp, `check-buildplan` and the deterministic manifest. Untested.

## Plugin version management

Every plugin version is declared once, in the parent POM's `<pluginManagement>` (kept in alphabetical order). Module POMs and profiles reference plugins by `groupId` and `artifactId` only. One place per version means an upgrade touches one line and no two modules build with different versions of the same plugin. `requirePluginVersions` in the `qa` enforcer execution guarantees that every plugin has an explicit, non-snapshot version, but not where it is declared; the placement rule is not checked by the build.

Build extensions cannot be managed through `<pluginManagement>`. Their versions come from a parent POM property instead (`os-maven-plugin.version`, used by `cli`'s `os-maven-plugin` extension).

## Boundaries

- The Maven JARs are reproducible; the native CLI distribution archives are not (#320).
- Only `var` and legacy JavaDoc are checked mechanically. Other rules in [CLAUDE.md](../CLAUDE.md) that a check could enforce (no fully qualified class names in code, GitHub Actions referenced by SHA) rely on review.
