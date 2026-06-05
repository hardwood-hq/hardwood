# JAR manifest build metadata

Status: Completed

Every published JAR carries its project version and the short Git commit SHA it
was built from, in the JAR manifest (`META-INF/MANIFEST.MF`). This lets a JAR
recovered from a classpath, a bug report, or a dependency tree be traced back to
an exact source revision.

## Manifest entries

For each module that produces a JAR, the manifest includes:

- `Implementation-Title` — the module's `${project.name}`.
- `Implementation-Version` — the module's `${project.version}`.
- `Implementation-Build` — the short (7-character) Git commit SHA of `HEAD`.

`Implementation-Title` and `Implementation-Version` are produced by Maven
Archiver's `addDefaultImplementationEntries`. `Implementation-Build` is an
explicit manifest entry resolved from the `${revision}` property.

## Capturing the revision

A single `maven-antrun-plugin` execution (`capture-git-info`), bound to the
`initialize` phase in the parent POM, runs `git rev-parse --short=7 HEAD` and
`git status --porcelain` against the repository root and exports two Ant
properties:

- `revision` — the short SHA, or `unknown` when Git is unavailable or the build
  runs outside a checkout (e.g. an unpacked source tarball).
- `revisionDirty` — `true` when the working tree has uncommitted changes,
  otherwise `false`.

Because the execution lives in the parent POM's `<build><plugins>`, every
module inherits it; the JAR-manifest configuration lives in the parent's
`<pluginManagement>` for `maven-jar-plugin` and therefore applies to every JAR
module (`core`, `avro`, `aws-auth`, `s3`, `cli`, …) without per-module setup.

The `hardwood-cli` module additionally consumes `revision` and `revisionDirty`
through resource filtering in `application.properties`, surfacing them in
`hardwood --version`.

## Reproducibility

The project's reproducibility guarantee is enforced by
`project.build.outputTimestamp` together with
`maven-artifact-plugin:check-buildplan`. The manifest metadata is compatible
with it:

- `Implementation-*` version and vendor entries derive from the POM and are
  fully deterministic.
- The commit SHA is a deterministic function of the commit being built, so a
  rebuild from the same revision yields byte-identical output. No build
  timestamp, host name, or JDK identifier is added.
- The working-tree dirty flag is `false` for any clean checkout of a released
  tag, so released artifacts are unaffected by it.

One inherent limitation: a byte-for-byte rebuild from a `.git`-less source
archive resolves `revision` to `unknown`, which would not match a JAR released
from a real checkout. This project verifies reproducibility from a Git checkout
of the tag (where `git rev-parse` succeeds), not from a detached source
tarball, so this does not affect the guarantee as exercised.
