# InputFile abstraction for file access

## Context

The entire read pipeline is typed to `MappedByteBuffer`, coupling it to memory-mapped local files. This blocks object store support (#31) and in-memory reading. The goals are to:

1. Introduce a thin `InputFile` interface at the file-access boundary
2. Widen all internal types from `MappedByteBuffer` to `ByteBuffer`
3. Keep the existing mmap backend as default, with no performance regression
4. Enable future backends (S3, in-memory) without further internal changes
5. Establish an explicit open/close lifecycle on `InputFile`
6. Remove all `Path`-based entry points from the public API

## InputFile interface

New file: `core/src/main/java/dev/hardwood/InputFile.java`

```java
public interface InputFile extends Closeable {
    void open() throws IOException;
    ByteBuffer readRange(long offset, int length) throws IOException;
    long length() throws IOException;
    String name();

    static InputFile of(ByteBuffer buffer) { ... }
    static InputFile of(Path path) { ... }
    static List<InputFile> ofPaths(List<Path> paths) { ... }
    static List<InputFile> ofBuffers(List<ByteBuffer> buffers) { ... }
}
```

- `open()` performs expensive resource acquisition (e.g. memory-mapping, network connect). Must be called before `readRange`/`length`. Idempotent.
- `readRange` returns a `ByteBuffer` (may be a zero-copy slice of a mapping or a heap buffer)
- `length` returns file size
- `name` provides an identifier for logging/JFR events
- `Closeable` for resource cleanup (e.g. close connections for network backends)
- Static factories for common backends: `of(Path)` (memory-mapped), `of(ByteBuffer)` (in-memory)
- List factories: `ofPaths(List<Path>)`, `ofBuffers(List<ByteBuffer>)`

Package: `dev.hardwood` — it's a user-facing type (users can implement it for custom backends).

## MappedInputFile implementation

New file: `core/src/main/java/dev/hardwood/internal/reader/MappedInputFile.java`

```java
public class MappedInputFile implements InputFile {
    private final Path path;
    private final String name;
    private MappedByteBuffer mapping;

    public MappedInputFile(Path path) { ... }

    @Override public void open() throws IOException {
        if (mapping != null) return; // idempotent
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            // 2 GB check, JFR event, channel.map()
            mapping = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);
        }
        // channel closed eagerly — MappedByteBuffer remains valid, released by GC
    }

    @Override public ByteBuffer readRange(long offset, int length) {
        return mapping.slice((int) offset, length); // zero-copy
    }
    @Override public long length() { return mapping.capacity(); }
    @Override public String name() { return name; }
    @Override public void close() { /* no-op: channel closed eagerly, MappedByteBuffer released by GC */ }
}
```

This consolidates the mmap + channel logic currently duplicated in `ParquetFileReader.open()` and `FileManager.mapAndReadMetadata()`. The 2 GB file size check and `FileMappingEvent` JFR event move here.

## ByteBufferInputFile implementation

New file: `core/src/main/java/dev/hardwood/internal/reader/ByteBufferInputFile.java`

```java
public class ByteBufferInputFile implements InputFile {
    private final ByteBuffer buffer;

    public ByteBufferInputFile(ByteBuffer buffer) { ... }

    @Override public void open() { /* no-op */ }
    @Override public ByteBuffer readRange(long offset, int length) {
        return buffer.slice((int) offset, length);
    }
    @Override public long length() { return buffer.capacity(); }
    @Override public String name() { return "<memory>"; }
    @Override public void close() { /* no-op */ }
}
```

## Internal type widening (MappedByteBuffer → ByteBuffer)

Mechanical type changes — no behavioral changes. All operations used (`slice`, `get`, `remaining`, `position`, `duplicate`) exist on `ByteBuffer`. All underlying libraries (ZSTD, Snappy, LZ4, Inflater, FFM) accept `ByteBuffer`.

### Files to change

| File | Change |
|------|--------|
| `Decompressor.java` + 8 implementations | `decompress(MappedByteBuffer, int)` → `decompress(ByteBuffer, int)` |
| `PageInfo.java` | `MappedByteBuffer pageData` → `ByteBuffer pageData` |
| `PageReader.java` | `decodePage(MappedByteBuffer, ...)` → `decodePage(ByteBuffer, ...)`, internal `slice()` calls |
| `FileState.java` | Remove `Path` + `MappedByteBuffer` fields, replace with `InputFile inputFile` |

## Refactor PageScanner to use InputFile

```java
// Before
public PageScanner(ColumnSchema, ColumnChunk, HardwoodContextImpl,
    MappedByteBuffer fileMapping, long fileMappingBaseOffset, String filePath, int rowGroupIndex)

// After
public PageScanner(ColumnSchema, ColumnChunk, HardwoodContextImpl,
    InputFile inputFile, int rowGroupIndex)
```

- Replace `fileMapping.slice(offset - fileMappingBaseOffset, length)` with `inputFile.readRange(offset, length)`
- Remove `fileMappingBaseOffset` — `InputFile` covers the whole file, offsets are absolute
- Remove `filePath` parameter — use `inputFile.name()` for JFR events

## Update callers

### ParquetFileReader
- Store `InputFile` instead of `FileChannel` + `MappedByteBuffer`
- `open(InputFile)` — call `open()`, own both context and file
- `open(InputFile, HardwoodContext)` — call `open()`, own file, don't own context
- `close()` always closes the `InputFile`; only closes context if it owns it
- Remove all `Path` overloads

### Hardwood
- `open(InputFile)` — delegate to `ParquetFileReader.open(inputFile, context)`. The returned reader owns the file.
- `openAll(List<InputFile>)` — delegate to `MultiFileParquetReader`. Files are opened/closed by `FileManager`.
- Remove all `Path` overloads

### MultiFileParquetReader
- Constructor takes `List<InputFile>` instead of `List<Path>`
- `FileManager` handles open/close lifecycle for all files

### FileManager
- `openFirst()` — call `inputFile.open()` on first file, read metadata
- `loadFile()` — call `inputFile.open()` on subsequent files (async prefetch path)
- `close()` — join in-flight prefetch futures, then close all `InputFile` instances
- Use `inputFile.name()` for error messages in schema validation

### ParquetMetadataReader
- `readMetadata(MappedByteBuffer, Path)` → `readMetadata(InputFile)`
- Use `inputFile.readRange()` for magic validation and footer reading
- Use `inputFile.name()` for error messages

### ColumnReader, SingleFileRowReader
- Store `InputFile` instead of `MappedByteBuffer`
- Pass `InputFile` to `PageScanner`

## Public API changes

### New type: `dev.hardwood.InputFile`

The `InputFile` interface becomes the single way to specify file sources. Users create instances via static factories and pass them to the reader API:

```java
// Single file
try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path))) { ... }

// Multiple files
try (Hardwood hardwood = Hardwood.create();
     MultiFileParquetReader parquet = hardwood.openAll(InputFile.ofPaths(paths))) { ... }

// In-memory
try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(byteBuffer))) { ... }
```

### Removed methods

| Class | Removed method | Replacement |
|-------|---------------|-------------|
| `ParquetFileReader` | `open(Path)` | `open(InputFile.of(path))` |
| `ParquetFileReader` | `open(Path, HardwoodContext)` | `open(InputFile.of(path), context)` |
| `Hardwood` | `open(Path)` | `open(InputFile.of(path))` |
| `Hardwood` | `openAll(List<Path>)` | `openAll(InputFile.ofPaths(paths))` |
| `Hardwood` | `openAll(Path, Path...)` | `openAll(InputFile.ofPaths(List.of(...)))` |
| `Hardwood` | `create(int)` | _(removed, no replacement)_ |

### Added methods

| Class | Method | Description |
|-------|--------|-------------|
| `InputFile` | `open()` | Perform resource acquisition (called by the framework) |
| `InputFile` | `readRange(long, int)` | Read a byte range from the file |
| `InputFile` | `length()` | Return file size in bytes |
| `InputFile` | `name()` | Return identifier for logging/JFR |
| `InputFile` | `of(Path)` | Create memory-mapped input file |
| `InputFile` | `of(ByteBuffer)` | Create in-memory input file |
| `InputFile` | `ofPaths(List<Path>)` | Create memory-mapped input files |
| `InputFile` | `ofBuffers(List<ByteBuffer>)` | Create in-memory input files |
| `ParquetFileReader` | `open(InputFile)` | Open with dedicated context (owns file) |
| `ParquetFileReader` | `open(InputFile, HardwoodContext)` | Open with shared context (caller owns file) |
| `Hardwood` | `open(InputFile)` | Open single file with shared context |
| `Hardwood` | `openAll(List<InputFile>)` | Open multiple files with prefetching |

### Migration

All call sites change mechanically:

```java
// Before                                    // After
ParquetFileReader.open(path)                 ParquetFileReader.open(InputFile.of(path))
hardwood.open(path)                          hardwood.open(InputFile.of(path))
hardwood.openAll(paths)                      hardwood.openAll(InputFile.ofPaths(paths))
hardwood.openAll(first, second)              hardwood.openAll(InputFile.ofPaths(List.of(first, second)))
```

## Ownership summary

| Entry point | Who calls `open()`? | Who calls `close()`? |
|-------------|---------------------|----------------------|
| `ParquetFileReader.open(InputFile)` | Reader | Reader |
| `ParquetFileReader.open(InputFile, HardwoodContext)` | Reader | Reader |
| `Hardwood.open(InputFile)` | Reader (via above) | Reader |
| `Hardwood.openAll(List<InputFile>)` | FileManager | FileManager (via `MultiFileParquetReader.close()`) |

In all cases, passing an `InputFile` to a reader transfers ownership. The reader opens and closes the file.

## File change summary

### New files (3)
- `core/src/main/java/dev/hardwood/InputFile.java`
- `core/src/main/java/dev/hardwood/internal/reader/MappedInputFile.java`
- `core/src/main/java/dev/hardwood/internal/reader/ByteBufferInputFile.java`

### Modified files (~20)
- `Decompressor.java` + 8 implementations (9 files)
- `PageInfo.java`, `PageReader.java`, `PageScanner.java`
- `ParquetMetadataReader.java`, `FileState.java`
- `ParquetFileReader.java`, `ColumnReader.java`, `SingleFileRowReader.java`
- `FileManager.java`, `MultiFileParquetReader.java`, `MultiFileRowReader.java`, `Hardwood.java`
- ~25 test/bench files (replace `open(path)` → `open(InputFile.of(path))`, etc.)

## Verification

1. `./mvnw verify -pl core` — all core tests pass (mmap path unchanged behaviorally)
2. `./mvnw verify -pl integration-test` — integration tests pass
3. `./mvnw verify` — full build including parquet-java-compat and parquet-testing-runner
4. `ByteBufferInputFileTest` — verifies the abstraction works for non-mmap sources
