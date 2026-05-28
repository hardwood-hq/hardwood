# Error Prone Rule Enforcement

**Status: Implemented**

## Overview

A subset of the coding rules in [CLAUDE.md](../CLAUDE.md) are enforced mechanically
at compile time using [Error Prone](https://errorprone.info/) with project-local
checks. The checks live in a dedicated `hardwood-error-prone-checks` Maven module and
are wired into every other module's `javac` invocation as an annotation processor.

Two rules are enforced today:

- `NoVar` — flags any use of Java `var` (local variable type inference).
- `NoLegacyJavadoc` — flags any `/** */` JavaDoc block; only `///` Markdown JavaDoc
  (JEP 467) is allowed.

Both produce a compile error pointing at the offending file and line; the rule name
and a one-line reason appear in the diagnostic.

## Module layout

```
error-prone-checks/
├── pom.xml
└── src/
    ├── main/java/dev/hardwood/build/errorprone/
    │   ├── JavaSourceFiles.java       # shared helper: is this file under src/(main|test)/java*/?
    │   ├── NoVar.java
    │   └── NoLegacyJavadoc.java
    └── test/java/dev/hardwood/build/errorprone/
        ├── NoVarTest.java
        └── NoLegacyJavadocTest.java
```

`JavaSourceFiles.isConventionalJavaSource(...)` accepts any source root matching
`/src/(main|test)/java\d*/`, so multi-release source roots like `core/src/main/java22/`
are checked alongside the standard `src/main/java/` root. Sources outside any
conventional Java root (e.g. `target/generated-sources/...`) are skipped — generated
code and third-party stubs must not fail the build.

## Build wiring

The Error Prone plugin and the `hardwood-error-prone-checks` annotation-processor
path live in the parent pom's `qa` profile (alongside the license check, formatter,
import sort, and plugin-version enforcer). The `qa` profile activates by default via
`!quick`, so:

- `./mvnw verify` — runs Error Prone (matches CI behaviour).
- `./mvnw -Dquick verify` — skips Error Prone for fast inner-loop builds.

The `error-prone-checks` module itself opts out of inheriting Error Prone (via
`combine.self="override"` on `compilerArgs` and `annotationProcessorPaths`) to avoid
a circular dependency. The `performance-testing/micro-benchmarks` module uses
`combine.children="append"` so its own compiler args (JMH annotation processor,
`--add-modules jdk.incubator.vector`) compose with the qa profile's Error Prone
args. The `core` module's `compile-java22` execution uses `combine.children="append"`
on its compiler args for the same reason.

## CI propagation

GitHub Actions workflows that run `./mvnw … -pl <module-list>` need
`hardwood-error-prone-checks` to be either built in the same reactor or already
present in the local Maven repository. Two strategies are used:

- **Multi-job workflows** (`main-build.yml`, `pr-build.yml`): the first install
  job (`build-core`) includes `:hardwood-error-prone-checks` in its `-pl` list and
  uploads the resulting JAR as part of the `maven-repo-core` artifact. Downstream
  jobs download the artifact into `~/.m2/repository/dev/hardwood/` and can omit the
  module from their own `-pl` lists.
- **Single-job workflows** (`performance.yml`, `cli-early-access.yml`,
  `release-cli.yml`): the install step lists `:hardwood-error-prone-checks`
  explicitly, since there is no upstream job to seed the local Maven repository.

## JDK module exports

Error Prone needs access to internal `jdk.compiler` packages to inspect the javac
AST. The required `--add-exports` / `--add-opens` flags are declared in:

- `.mvn/jvm.config` — applies to the Maven invocation itself (the compiler plugin
  runs in-process with Maven).
- `error-prone-checks/pom.xml` `<compilerArgs>` — applies when compiling the
  checks against the Error Prone API.
- `error-prone-checks/pom.xml` `<surefire><argLine>` — applies when running the
  check unit tests, which use `CompilationTestHelper` to invoke javac in-process.

## Rules not yet enforced

CLAUDE.md lists several other mechanically-checkable rules (no fully-qualified class
names, GitHub Actions referenced by SHA, etc.). Those are out of scope for the
initial enforcement module; the framework is set up so additional checks can be
added incrementally as new files under
`error-prone-checks/src/main/java/dev/hardwood/build/errorprone/`.
