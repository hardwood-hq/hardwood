/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.arrow;

import java.io.IOException;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.types.pojo.Schema;

import dev.hardwood.Experimental;
import dev.hardwood.arrow.internal.ArrowPlan;
import dev.hardwood.arrow.internal.BatchLoader;
import dev.hardwood.reader.BinaryDictionary;
import dev.hardwood.reader.ColumnReaders;

/// Reads Parquet columns as Arrow [org.apache.arrow.vector.VectorSchemaRoot] batches.
///
/// Wraps a Hardwood [ColumnReaders] and serves each of its batches through Arrow Java's
/// [ArrowReader] contract: [#loadNextBatch()] advances the columns, loads any new or changed
/// dictionaries into the reader's [org.apache.arrow.vector.dictionary.DictionaryProvider], and
/// loads the batch into the root [#getVectorSchemaRoot()] returns, replacing the previous batch.
/// Anything built on `ArrowReader`, such as `ArrowStreamWriter`, `ArrowFileWriter` and the C Data
/// Interface's `Data.exportArrayStream`, consumes this reader unchanged.
///
/// ```java
/// try (BufferAllocator allocator = new RootAllocator();
///      ParquetFileReader fileReader = ParquetFileReader.open(inputFile);
///      ArrowBatchReader reader = ArrowReaders.batchReader(fileReader, allocator)) {
///     VectorSchemaRoot root = reader.getVectorSchemaRoot();
///     while (reader.loadNextBatch()) {
///         BigIntVector ids = (BigIntVector) root.getVector("id");
///         // ...
///     }
/// }
/// ```
///
/// **Dictionaries.** When dictionary output is configured on [ArrowReaders.BatchReaderBuilder],
/// the reader exposes each dictionary-encoded column as a signed 32-bit index vector in the root
/// paired with its value vector in [#getDictionaryVectors()] / [#lookup(long)]. A dictionary is
/// loaded once per row group and reused across every batch drawn from that row group; a new row
/// group or a plain-encoded fallback batch replaces the dictionary transparently.
///
/// **One root, reused.** Every batch is loaded into the same root, and loading a batch
/// releases the buffers of the previous one. A consumer that keeps a batch beyond the next
/// [#loadNextBatch()] transfers it out first, with a `TransferPair` or a `VectorUnloader`,
/// and releases what it transferred itself.
///
/// **Memory.** Every buffer comes from the [BufferAllocator] the reader was built with. The
/// reader creates no allocator of its own and does not close the one it is given; after
/// [#close()] it holds no memory from it.
///
/// **Lifecycle.** The reader owns the [ColumnReaders] it wraps and closes it, but not the
/// [dev.hardwood.reader.ParquetFileReader] the columns came from. A failure while loading a
/// batch releases what the batch had allocated, stops the underlying read, and leaves the
/// reader unusable: a later [#loadNextBatch()] throws [IllegalStateException].
///
/// **This API is [Experimental]:** the columns the reader serves and the Arrow types it
/// produces may change in future releases without prior deprecation, as may the
/// [dev.hardwood.reader.ColumnReader] it is built on.
@Experimental
public final class ArrowBatchReader extends ArrowReader {

    private final ColumnReaders columns;
    private final ArrowPlan plan;
    private final BinaryDictionary[] loadedDictionaries;
    private long bytesLoaded;
    private boolean failed;
    private boolean closed;

    ArrowBatchReader(BufferAllocator allocator, ColumnReaders columns, ArrowPlan plan) throws IOException {
        super(allocator);
        this.columns = columns;
        this.plan = plan;
        this.loadedDictionaries = new BinaryDictionary[plan.dictionaryNodes().size()];
        // Initialize the root and empty dictionary map upfront (zero allocator bytes) so
        // ArrowReader.getDictionaryIds() and lookup(long) are usable before getVectorSchemaRoot().
        try {
            ensureInitialized();
        }
        catch (IOException | RuntimeException e) {
            try {
                columns.close();
            }
            catch (IOException | RuntimeException closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
    }

    /// Loads the next batch of columns (and any new or replacement dictionaries) into the root.
    ///
    /// @return `true` when a batch was loaded, `false` at the end of the read, and on every
    ///         call after it
    /// @throws IOException if the bytes could not be read
    /// @throws dev.hardwood.reader.ParquetReadException if the file's bytes are not what a
    ///         Parquet file can say, or a value contradicts the range its annotation declares
    /// @throws UnsupportedOperationException if a value has no Arrow representation, such as
    ///         an `INT96` timestamp outside the range of a 64-bit nanosecond count
    /// @throws IllegalStateException if the reader is closed, if an earlier load failed, or if
    ///         a batch's binary values exceed what 32-bit offsets address
    @Override
    public boolean loadNextBatch() throws IOException {
        if (closed) {
            throw new IllegalStateException("ArrowBatchReader is closed");
        }
        if (failed) {
            throw new IllegalStateException("ArrowBatchReader failed while loading a batch and cannot continue");
        }
        try {
            // Preparing the root can allocate (an offset sentinel per variable-width vector), so
            // it fails the reader the way a failed load does.
            prepareLoadNextBatch();
            if (!columns.nextBatch()) {
                return false;
            }
            BatchLoader.LoadedBatch loaded = BatchLoader.load(plan, columns, allocator, loadedDictionaries);
            // loadDictionary and loadRecordBatch close each message, so compute body length first.
            long bodyLength = loaded.computeBodyLength();
            int nextDict = 0;
            boolean recordConsumed = false;
            try {
                List<ArrowDictionaryBatch> dicts = loaded.dictionaries();
                while (nextDict < dicts.size()) {
                    ArrowDictionaryBatch dictBatch = dicts.get(nextDict);
                    nextDict++;
                    loadDictionary(dictBatch);
                }
                recordConsumed = true;
                loadRecordBatch(loaded.recordBatch());
            }
            catch (RuntimeException e) {
                loaded.closeFrom(nextDict, recordConsumed);
                throw e;
            }
            loaded.commitDictionaries(loadedDictionaries);
            bytesLoaded += bodyLength;
            return true;
        }
        catch (IOException | RuntimeException e) {
            failed = true;
            try {
                columns.close();
            }
            catch (IOException | RuntimeException closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
    }

    /// The bytes of Arrow buffers loaded so far: the sum of every loaded dictionary and record
    /// batch's body. There is no byte channel beneath this reader, so this is what the method's
    /// contract has to count.
    @Override
    public long bytesRead() {
        return bytesLoaded;
    }

    @Override
    protected void closeReadSource() throws IOException {
        closed = true;
        columns.close();
    }

    @Override
    protected Schema readSchema() {
        return plan.schema();
    }
}
