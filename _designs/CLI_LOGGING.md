# CLI logging

Describes where the `hardwood` CLI and the `dive` TUI send what they print: a command's result, the CLI's own messages, and the log records of Hardwood and the libraries the CLI runs; the format and threshold of those records; and how the same configuration reaches the JVM and the native binary.

Related documents:

- [DIVE_ARCHITECTURE.md](DIVE_ARCHITECTURE.md): the dive screen model and its terminal session
- [CLI_VALUE_RENDERING.md](CLI_VALUE_RENDERING.md): how values and figures are spelled in a command's result
- [EXCEPTION_MODEL.md](EXCEPTION_MODEL.md): the exceptions a failed read raises, which a command reports
- [NATIVE_BUILD.md](../NATIVE_BUILD.md): building the native binary
- [docs/content/reference/cli.md](../docs/content/reference/cli.md): the commands and their options

## Streams

stdout carries a command's result and nothing else, so it can be piped into another tool or a file: the rows of `print` and `convert`, a schema, the tables of `info` and `inspect`. Everything else goes to stderr.

A failed command prints its own message to stderr and returns a non-zero exit code (`Main.run` maps the result). These messages are part of the command's answer and are printed directly; they are not log records and no threshold applies to them.

## One hub: `java.util.logging`

Every log record reaches `java.util.logging`, configured once by `CliLogging`:

| Source | API | Route |
|---|---|---|
| Hardwood | `System.Logger` | the JDK's default `LoggerFinder`, which logs to `java.util.logging` |
| AWS SDK (`aws-auth`, S3 credentials) | SLF4J 1.7 | `slf4j-jdk14` |
| Aesh, JLine | `java.util.logging` | direct |

The native image is built with `-H:-UseServiceLoaderFeature`, so a provider found through `ServiceLoader` is not registered for it automatically. The SLF4J route is a 1.7 binding, a static class. Hardwood's route is the JDK's own `System.LoggerFinder` provider in `java.logging`, which the JDK looks up through `ServiceLoader`; `NativeBinarySmokeIT` checks that a Hardwood warning reaches the binary's stderr in the format below.

## Format and threshold

`Main.run` calls `CliLogging.configure()` after it has set stdout and stderr to UTF-8. The root logger gets one handler, which prints each record as one line, `LEVEL: message`, with an attached exception appended after a colon, and the root level is `WARNING`. The level names are `java.util.logging`'s, so a `System.Logger` `ERROR` prints as `SEVERE`. The CLI's own warnings use the same `WARNING: ` prefix.

The handler writes to the `System.err` current when a record is published, where `ConsoleHandler` keeps the one current when it was created, so it follows the UTF-8 stream `Main` installs and the stream each command test captures.

The configuration is code rather than a `logging.properties` resource, so it needs no resource or reflection registration in the native image. An explicit `java.util.logging.config.file` or `java.util.logging.config.class` system property, given to the JVM or the native binary, takes precedence and `configure` leaves the configuration untouched.

## dive

The TUI owns the terminal, so no record reaches stderr while it runs. `CliLogging.forTui` removes the root logger's handlers for the session and restores them when `dive` exits. With `--log-file`, one file handler on the root logger receives the session's records in a timestamped one-line format: Hardwood's from `FINE`, which includes every S3 fetch, and every other source's from `WARNING`. Without it, the session's records are discarded.

## Levels in Hardwood

Hardwood logs at `WARNING` what a reader of the file should know: statistics discarded for pruning, annotations dropped, an unrecognised reader option. What describes the environment rather than the file, such as whether SIMD decoding is available or which GZIP decompressor is in use, and the internals of a read are logged at `DEBUG` or below, so neither the CLI nor an application embedding the library shows them by default.

Tests: `dev.hardwood.cli.internal.CliLoggingTest`, `dev.hardwood.cli.command.CliLoggingTest`, `DiveCommandTest`, `NativeBinarySmokeIT`.
