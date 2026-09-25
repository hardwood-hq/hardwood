# Plan: Bloom Filter Read Path (#669)

**Status: Implemented**

## Context

A Parquet column chunk may carry a [split-block bloom filter](https://github.com/apache/parquet-format/blob/master/BloomFilter.md) for probabilistic membership testing. Unlike min/max statistics, which only bound a range, a bloom filter answers equality membership ("is value X present?") even when the value range is wide.

The filter is stored in the file body at `bloom_filter_offset`, separate from the footer. The footer fields `bloom_filter_offset` / `bloom_filter_length` (`ColumnMetaData` Thrift fields 14/15) are already surfaced on `ColumnMetaData`; they are pointers to the filter body.

This feature reads and decodes the filter those pointers reference and exposes a membership-check primitive. Consulting the filter during query evaluation to prune column chunks (predicate push-down) is built on top of this and is deferred to #105.

All types are internal (`dev.hardwood.internal.*`); no public API or CLI surface is added.

## Layout

The bloom-filter domain lives in one package; the Thrift readers live with the other Thrift readers.

- `internal/bloomfilter/` — `BloomFilterHeader`, `BloomFilter`, `SplitBlockBloomFilter`, `XxHash64`
- `internal/thrift/` — `BloomFilterHeaderReader`, `BloomFilterReader`

Dependency direction: `thrift → bloomfilter` (the readers produce the model types); `bloomfilter` depends only on the JDK.

## Step 1: Header model and reader

The filter body begins with a Thrift `BloomFilterHeader` struct: `numBytes` plus three single-variant unions (`algorithm`, `hash`, `compression`).

### `internal/bloomfilter/BloomFilterHeader.java`
```java
public record BloomFilterHeader(
        int numBytes,
        Algorithm algorithm,
        Hash hash,
        Compression compression) {
    public enum Algorithm { BLOCK; /* fromVariant(short) */ }
    public enum Hash { XXHASH; /* fromVariant(short) */ }
    public enum Compression { UNCOMPRESSED; /* fromVariant(short) */ }
}
```
Each enum exposes `fromVariant(short)` mapping the union's Thrift field id to its sole constant, throwing `IllegalArgumentException` on an unknown variant.

### `internal/thrift/BloomFilterHeaderReader.java`
Parses the struct: `numBytes` (field 1) via `readNonNegativeI32`; the three unions (fields 2–4) by reading the single set variant — its field id, its empty inner struct, and the union's STOP — and mapping via the enums' `fromVariant`. Wire types are validated per field, and all four fields are checked present after the struct, throwing `IllegalStateException` on a missing or malformed field, consistent with the other struct-content readers (`BoundingBoxReader`, `LogicalTypeReader`). `BloomFilterReader` raises the same type when the decoded `numBytes` is not a positive multiple of 32 or the bitset is truncated, so both halves of the decode report malformed input uniformly.

## Step 2: Filter model and reader

### `internal/bloomfilter/BloomFilter.java`
```java
public record BloomFilter(BloomFilterHeader header, ByteBuffer bitset) {
    public boolean mightContain(long hash) {
        return SplitBlockBloomFilter.mightContain(bitset, hash);
    }
}
```
`bitset` is a read-only, little-endian `ByteBuffer` view that shares storage with the bytes fetched from the file — for a memory-mapped input it is not copied. It is probed through absolute reads only; its position is neither used nor modified.

### `internal/thrift/BloomFilterReader.java`
Reads the header, then takes the following `numBytes` as the bitset:
```java
BloomFilterHeader header = BloomFilterHeaderReader.read(reader);
int numBytes = header.numBytes();
if (numBytes > reader.remaining()) { throw new IllegalStateException(/* truncated */); }
ByteBuffer bitset = reader.readSlice(numBytes);
return new BloomFilter(header, bitset);
```
The caller positions `reader` over the bytes fetched from `bloom_filter_offset`; this reader performs no file I/O.

### `internal/thrift/ThriftCompactReader.java`
Two supporting methods:
- `int remaining()` — bytes still available.
- `ByteBuffer readSlice(int length)` — a zero-copy, read-only, little-endian view of the next `length` bytes, advancing past them. The slice shares storage with the reader's buffer, so a memory-mapped input stays backed by the mapped file.

## Step 3: Split-block membership check

### `internal/bloomfilter/SplitBlockBloomFilter.java`
```java
public static boolean mightContain(ByteBuffer bitset, long hash)
```
The bitset is an array of 32-byte blocks, each holding eight little-endian 32-bit words. The high 32 bits of the hash select one block (`((hash >>> 32) * numBlocks) >>> 32`); the low 32 bits derive one bit per word (`1 << ((key * SALT[i]) >>> 27)`) using the eight `SALT` constants fixed by the specification. The value is present only if all eight bits are set; any clear bit proves it absent (no false negatives). Reads are absolute (`numBlocks` from `capacity()`), so the shared buffer's position is irrelevant.

## Step 4: Value hashing

### `internal/bloomfilter/XxHash64.java`
The XXH64 hash the bloom filter spec mandates (seed 0). Entry points:
```java
public static long hash(byte[] data)                       // binary values
public static long hash(byte[] data, int offset, int length)
public static long hash(long value)                        // INT64 — 8 LE bytes, no allocation
public static long hash(int value)                         // INT32 — 4 LE bytes, no allocation
```
The `long` / `int` overloads compute the hash of the value's little-endian plain encoding directly, without building a byte array.

## Hashing boundary

`XxHash64` exposes only hashing primitives over `long`, `int`, and `byte[]`. It does not know about Parquet physical types. Mapping a column's physical type to the correct primitive (`INT64` → `hash(long)`, `INT32` → `hash(int)`, `BYTE_ARRAY` → `hash(byte[])`, …), and converting non-binary values (e.g. strings) to their plain-encoded bytes, is the caller's responsibility and belongs to the push-down integration (#105).

## Testing

Verified end-to-end against `core/src/test/resources/bloom_filter_test.parquet`, which carries bloom filters on an `INT64`, an `INT32`, and a variable-length `STRING` column (string lengths 1–64 exercise every XXH64 code path: the 1-byte tail, the 4-byte tail, and the ≥32-byte accumulator-lane loop). Tests assert that every stored value reports present (a bloom filter has no false negatives) and that absent values are discriminated. The header/bitset reader is additionally checked for an exact split between the header bytes consumed and the declared `numBytes`.
