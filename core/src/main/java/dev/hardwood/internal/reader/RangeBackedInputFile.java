/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import dev.hardwood.InputFile;

/// An [InputFile] decorator that caches fetched byte ranges in a
/// sparse temp file mmapped into the process. See #373 and
/// `_designs-legacy/REMOTE_RANGE_BACKING.md`.
///
/// The first call to [#readRange] for a given range delegates to the
/// wrapped file, writes the bytes into the mapping at their absolute
/// offset, and marks the range populated. Subsequent reads of any
/// range fully covered by populated bytes return zero-copy slices of
/// the mapping — no further calls to the wrapped file.
///
/// **Footprint.** The mapping reserves `length()` bytes of virtual
/// address space at [#open]. The temp file is sparse-truncated to the
/// same size; only touched pages occupy real memory or disk on
/// filesystems that support sparse files (ext4, xfs, apfs, ntfs).
/// Real footprint scales with bytes fetched, not file size.
///
/// **Limit.** [MappedByteBuffer#slice(int, int)] is `int`-typed, so
/// the mapping cannot exceed [Integer#MAX_VALUE] bytes. Files larger
/// than 2 GB cannot be range-backed; [#open] throws on those.
///
/// **Lifecycle.** [#close] drops the mapping reference and deletes the
/// temp file. The unmap itself is GC-driven, so the address space and
/// the file's blocks are reclaimed on the next collection rather than
/// at `close()`. Where the platform refuses to unlink a file that still
/// carries a mapping — Windows does — the delete is deferred to JVM
/// exit rather than failing the close. The wrapped file is closed via
/// the standard delegation.
public final class RangeBackedInputFile implements InputFile {

    private static final System.Logger LOG = System.getLogger(RangeBackedInputFile.class.getName());

    private final InputFile delegate;
    private final Path tempDir;

    private long fileLength = -1;
    private Path tempFile;
    private FileChannel channel;
    private MappedByteBuffer mapping;
    private RangeSet populated;

    /// Creates a decorator that caches ranges fetched from `delegate`
    /// into a sparse temp file under `tempDir`.
    ///
    /// @param delegate the wrapped [InputFile] (must not be opened yet)
    /// @param tempDir the directory in which the backing temp file is
    ///        created; must exist and be writeable
    public RangeBackedInputFile(InputFile delegate, Path tempDir) {
        this.delegate = delegate;
        this.tempDir = tempDir;
    }

    @Override
    public synchronized void open() throws IOException {
        if (mapping != null) {
            return;
        }
        delegate.open();
        long length = delegate.length();
        if (length > Integer.MAX_VALUE) {
            // The file is correct and readable; this cache is what cannot address it,
            // and the message names the option that reads it anyway.
            throw new UnsupportedOperationException("File too large for range backing: "
                    + delegate.name() + " is " + length
                    + " bytes; the mmap-backed cache supports up to "
                    + Integer.MAX_VALUE + " bytes. Open with "
                    + "RangeBacking.NONE for files > 2 GB.");
        }
        this.fileLength = length;
        this.tempFile = Files.createTempFile(tempDir, "hardwood-range-", ".cache");
        try {
            this.channel = FileChannel.open(tempFile,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
            channel.truncate(length);
            this.mapping = channel.map(FileChannel.MapMode.READ_WRITE, 0, length);
            this.populated = new RangeSet();
        }
        catch (IOException | RuntimeException e) {
            // Allocation failed mid-setup; clean up the temp file we
            // already created so it doesn't leak.
            cleanupTempFile();
            throw e;
        }
    }

    @Override
    public synchronized ByteBuffer readRange(long offset, int length) throws IOException {
        if (mapping == null) {
            throw new IllegalStateException("File not opened: " + name());
        }
        if (offset < 0 || length < 0 || offset + length > fileLength) {
            throw new IllegalArgumentException(
                    "Range [" + offset + ", " + (offset + length)
                    + ") falls outside file [0, " + fileLength + ") (" + name() + ")");
        }
        long end = offset + length;
        if (!populated.contains(offset, end)) {
            // Fetch every gap in `[offset, end)` from the delegate and
            // write it into the mapping. We hold this object's monitor
            // throughout so concurrent readers either see the fully
            // populated range or wait — no torn reads from a partial
            // refill.
            for (long[] gap : populated.missing(offset, end)) {
                long gapStart = gap[0];
                long gapEnd = gap[1];
                int gapLen = Math.toIntExact(gapEnd - gapStart);
                ByteBuffer fetched = delegate.readRange(gapStart, gapLen);
                MappedByteBuffer slot = mapping;
                slot.position(Math.toIntExact(gapStart));
                slot.put(fetched);
                populated.add(gapStart, gapEnd);
            }
        }
        return mapping.slice(Math.toIntExact(offset), length);
    }

    @Override
    public long length() {
        if (fileLength < 0) {
            throw new IllegalStateException("File not opened: " + name());
        }
        return fileLength;
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public synchronized void close() throws IOException {
        IOException firstFailure = null;
        // Drop the mapping reference; the actual unmap happens when the
        // GC runs the cleaner. We don't have a portable way to force it.
        mapping = null;
        if (channel != null) {
            try {
                channel.close();
            }
            catch (IOException e) {
                firstFailure = e;
            }
            channel = null;
        }
        cleanupTempFile();
        try {
            delegate.close();
        }
        catch (IOException e) {
            if (firstFailure == null) {
                firstFailure = e;
            }
            else {
                firstFailure.addSuppressed(e);
            }
        }
        if (firstFailure != null) {
            throw firstFailure;
        }
    }

    /// Deletes the backing file, or defers the delete to JVM exit if the
    /// platform refuses it. Windows will not unlink a file that still
    /// carries a mapping, and the unmap here is GC-driven, so a failure
    /// is expected there rather than exceptional — failing [#close] over
    /// it would break every range-backed read on that platform. The
    /// deferral is logged so a leaked temp file is never silent.
    private void cleanupTempFile() {
        if (tempFile == null) {
            return;
        }
        try {
            Files.deleteIfExists(tempFile);
        }
        catch (IOException e) {
            tempFile.toFile().deleteOnExit();
            LOG.log(System.Logger.Level.WARNING,
                    "Could not delete range-cache file {0} ({1}); deferred to JVM exit",
                    tempFile, e);
        }
        tempFile = null;
    }
}
