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
/// Wraps a Hardwood [ColumnReaders] behind Arrow Java's [ArrowReader] contract, so
/// `ArrowStreamWriter`, `ArrowFileWriter`, and the C Data Interface (`Data.exportArrayStream`)
/// consume this reader unchanged. When dictionary output is configured on
/// [ArrowReaders.BatchReaderBuilder], dictionary-encoded columns appear as signed 32-bit index
/// vectors paired with their value vectors in [#getDictionaryVectors()] / [#lookup(long)], reused
/// within each row group and replaced across row groups or on plain-page fallback.
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
/// Every batch is loaded into the single root returned by [#getVectorSchemaRoot()], releasing the
/// previous batch's buffers; transfer vectors out (`TransferPair` / `VectorUnloader`) to keep them
/// across batches. All buffers come from the caller's [BufferAllocator], which the reader does not
/// close. Closing the reader closes the wrapped [ColumnReaders] but not the
/// [dev.hardwood.reader.ParquetFileReader]. A load failure releases what the batch allocated,
/// closes the underlying columns, and leaves the reader unusable.
///
/// **This API is [Experimental]:** it may change in future releases without prior deprecation.
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
        // Initialize the root and empty dictionary map upfront (zero bytes) so getDictionaryIds()
        // and lookup(long) are usable before getVectorSchemaRoot().
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

    /// Loads the next batch (and any new or replacement dictionaries) into the root.
    ///
    /// @return `true` when a batch was loaded, `false` at end of stream
    /// @throws IOException if the underlying bytes could not be read
    /// @throws dev.hardwood.reader.ParquetReadException if a value violates its Parquet annotation
    /// @throws UnsupportedOperationException if a value has no Arrow representation (e.g. an
    ///         out-of-range `INT96` timestamp)
    /// @throws IllegalStateException if closed, after an earlier failure, or if a batch's binary
    ///         values exceed 32-bit Arrow offsets
    @Override
    public boolean loadNextBatch() throws IOException {
        if (closed) {
            throw new IllegalStateException("ArrowBatchReader is closed");
        }
        if (failed) {
            throw new IllegalStateException("ArrowBatchReader failed while loading a batch and cannot continue");
        }
        try {
            prepareLoadNextBatch();
            if (!columns.nextBatch()) {
                return false;
            }
            BatchLoader.LoadedBatch loaded = BatchLoader.load(plan, columns, allocator, loadedDictionaries);
            long bodyLength = loaded.computeBodyLength();
            int nextDict = 0;
            boolean recordConsumed = false;
            try {
                List<ArrowDictionaryBatch> dicts = loaded.dictionaries();
                while (nextDict < dicts.size()) {
                    ArrowDictionaryBatch dictBatch = dicts.get(nextDict++);
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

    /// Total bytes of Arrow dictionary and record-batch buffers loaded so far.
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
