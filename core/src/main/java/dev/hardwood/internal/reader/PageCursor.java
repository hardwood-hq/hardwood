/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import dev.hardwood.HardwoodContext;

/**
 * Cursor over a column's pages with async prefetching.
 * Pages are decoded in parallel using the provided executor.
 * <p>
 * The prefetch depth automatically adapts based on whether pages are ready when needed:
 * <ul>
 *   <li>If nextPage() finds the prefetched page ready (hit), depth may decrease</li>
 *   <li>If nextPage() has to wait for a page (miss), depth increases</li>
 * </ul>
 * This ensures slow-to-decode columns automatically get more prefetch parallelism.
 * <p>
 * For multi-file reading, the cursor automatically fetches pages from the next file
 * when the remaining pages in the current file fall below the prefetch depth.
 * <p>
 * <b>Eager Assembly Mode:</b> When a {@link ColumnAssemblyBuffer} is provided, a virtual
 * thread is started to continuously consume decoded pages and assemble them into batches.
 * This pipelines assembly with decoding, so batches are ready when the consumer needs them.
 */
public class PageCursor {

    private static final System.Logger LOG = System.getLogger(PageCursor.class.getName());

    private static final int INITIAL_PREFETCH_DEPTH = 4;
    private static final int MAX_PREFETCH_DEPTH = 8;

    private final ArrayList<PageInfo> pageInfos;
    private final HardwoodContext context;
    private final Executor executor;
    private final String columnName;
    private final int projectedColumnIndex;
    private final String initialFileName;
    private PageReader pageReader;
    private int nextPageIndex = 0;

    // Multi-file support
    private final FileManager fileManager;
    private int currentFileIndex = 0;
    private int currentFileEndIndex; // Exclusive: pages [0, currentFileEndIndex) belong to current file
    private PageReader nextFileReader; // Reader for prefetched next file (null if not yet fetched)

    // Adaptive prefetch queue
    private final Deque<CompletableFuture<Page>> prefetchQueue = new ArrayDeque<>();
    private int targetPrefetchDepth = INITIAL_PREFETCH_DEPTH;

    // Eager assembly support (optional)
    private final ColumnAssemblyBuffer assemblyBuffer;

    /**
     * Creates a PageCursor for single-file reading without eager assembly.
     */
    public PageCursor(List<PageInfo> pageInfos, HardwoodContext context) {
        this(pageInfos, context, null, -1, null, null);
    }

    /**
     * Creates a PageCursor for single-file reading with optional eager assembly.
     *
     * @param pageInfos pages from the file
     * @param context hardwood context with executor
     * @param assemblyBuffer optional buffer for eager batch assembly (may be null for nested schemas)
     */
    public PageCursor(List<PageInfo> pageInfos, HardwoodContext context, ColumnAssemblyBuffer assemblyBuffer) {
        this(pageInfos, context, null, -1, null, assemblyBuffer);
    }

    /**
     * Creates a PageCursor with optional multi-file support.
     *
     * @param pageInfos initial pages from the first file
     * @param context hardwood context with executor
     * @param fileManager file manager for fetching pages from subsequent files (may be null)
     * @param projectedColumnIndex the projected column index for multi-file page requests
     * @param initialFileName the initial file name for logging (may be null)
     */
    public PageCursor(List<PageInfo> pageInfos, HardwoodContext context,
                      FileManager fileManager, int projectedColumnIndex, String initialFileName) {
        this(pageInfos, context, fileManager, projectedColumnIndex, initialFileName, null);
    }

    /**
     * Creates a PageCursor with optional multi-file support and eager assembly.
     *
     * @param pageInfos initial pages from the first file
     * @param context hardwood context with executor
     * @param fileManager file manager for fetching pages from subsequent files (may be null)
     * @param projectedColumnIndex the projected column index for multi-file page requests
     * @param initialFileName the initial file name for logging (may be null)
     * @param assemblyBuffer optional buffer for eager batch assembly (may be null)
     */
    public PageCursor(List<PageInfo> pageInfos, HardwoodContext context,
                      FileManager fileManager, int projectedColumnIndex, String initialFileName,
                      ColumnAssemblyBuffer assemblyBuffer) {
        this.pageInfos = new ArrayList<>(pageInfos);
        this.context = context;
        this.executor = context.executor();
        this.fileManager = fileManager;
        this.projectedColumnIndex = projectedColumnIndex;
        this.initialFileName = initialFileName;
        this.currentFileEndIndex = pageInfos.size();
        this.assemblyBuffer = assemblyBuffer;

        if (pageInfos.isEmpty()) {
            this.columnName = "unknown";
            this.pageReader = null;
        }
        else {
            PageInfo first = pageInfos.get(0);
            this.columnName = first.columnSchema().name();
            this.pageReader = new PageReader(
                    first.columnMetaData(),
                    first.columnSchema(),
                    context.decompressorFactory());
        }

        // Start prefetching immediately
        fillPrefetchQueue();

        // For eager assembly, start a virtual thread to continuously consume pages
        // and assemble them into batches. Virtual threads are ideal here because:
        // 1. The thread blocks waiting for decoded pages (I/O-like wait)
        // 2. Virtual threads are lightweight and managed by the JVM
        // 3. No need for daemon thread management
        if (assemblyBuffer != null) {
            Thread.startVirtualThread(this::runAssemblyThread);
        }
    }

    /**
     * Assembly thread that continuously consumes decoded pages and assembles them into batches.
     * <p>
     * This is the single producer for the assembly buffer's ring buffer. Decoded pages
     * come from parallel decoder threads via nextPage(), but batch assembly happens
     * here sequentially to maintain the single-producer guarantee.
     * <p>
     * When exhausted, signals the assembly buffer to finish. If an error occurs,
     * it is reported to the assembly buffer so the consumer can receive it.
     */
    private void runAssemblyThread() {
        try {
            while (hasNext()) {
                Page page = nextPage();
                if (page != null) {
                    assemblyBuffer.appendPage(page);
                }
            }
            assemblyBuffer.finish();
        }
        catch (Throwable t) {
            assemblyBuffer.signalError(t);
        }
    }

    /**
     * Check if there are more pages available.
     */
    public boolean hasNext() {
        if (!prefetchQueue.isEmpty() || nextPageIndex < pageInfos.size()) {
            return true;
        }
        // Queue empty and current pages exhausted - check if there are more files
        return fileManager != null && fileManager.hasFile(currentFileIndex + 1);
    }

    /**
     * Get the next decoded page. Blocks if the page is still being decoded.
     *
     * @return the next page, or null if exhausted
     */
    public Page nextPage() {
        if (prefetchQueue.isEmpty()) {
            if (nextPageIndex >= pageInfos.size()) {
                // Current file exhausted - try to load next file (blocking if needed)
                if (!tryLoadNextFileBlocking()) {
                    signalExhausted();
                    return null; // Truly exhausted
                }
                // Pages were added from next file, try to fill prefetch queue
                fillPrefetchQueue();
                if (prefetchQueue.isEmpty()) {
                    // Still empty - decode synchronously
                    if (nextPageIndex >= pageInfos.size()) {
                        return null;
                    }
                    return decodePageAndRelease(nextPageIndex++);
                }
                // Fall through to get from prefetch queue
            }
            else {
                // Queue empty but pages remain - decode synchronously and increase depth
                LOG.log(System.Logger.Level.DEBUG, "[{0}] Prefetch queue empty for column ''{1}''",
                        getCurrentFileName(), columnName);
                targetPrefetchDepth = Math.min(targetPrefetchDepth + 1, MAX_PREFETCH_DEPTH);
                return decodePageAndRelease(nextPageIndex++);
            }
        }

        CompletableFuture<Page> future = prefetchQueue.pollFirst();

        // Miss: had to wait - increase prefetch depth
        if (!future.isDone() && targetPrefetchDepth < MAX_PREFETCH_DEPTH) {
            targetPrefetchDepth++;
            LOG.log(System.Logger.Level.DEBUG, "[{0}] Prefetch miss for column ''{1}'', increasing depth to {2}",
                    getCurrentFileName(), columnName, targetPrefetchDepth);
        }

        // Refill queue after consuming
        fillPrefetchQueue();

        return future.join();
    }

    /**
     * Fill the prefetch queue up to targetPrefetchDepth.
     * <p>
     * Proactively fetches pages from the next file when remaining pages in the current
     * file fall below the prefetch depth. File boundaries are tracked to ensure each
     * page is decoded with the correct PageReader.
     * <p>
     * The next file fetch is non-blocking: pages are only added if the file is already
     * loaded. This prevents one column from blocking while waiting for file loading,
     * which would drain its prefetch queue and cause misses.
     */
    private void fillPrefetchQueue() {
        // Proactively fetch next file if current file is running low
        // Only fetch if we haven't already fetched it (nextFileReader == null)
        int pagesRemainingInCurrentFile = currentFileEndIndex - nextPageIndex;
        if (fileManager != null
                && nextFileReader == null
                && pagesRemainingInCurrentFile < targetPrefetchDepth
                && fileManager.hasFile(currentFileIndex + 1)) {

            // Ensure the next file is being loaded (non-blocking)
            fileManager.ensureFileLoading(currentFileIndex + 1);

            // Only get pages if file is already ready (non-blocking check)
            // This prevents blocking which would drain the prefetch queue and cause misses
            if (fileManager.isFileReady(currentFileIndex + 1)) {
                List<PageInfo> nextFilePages = fileManager.getPages(currentFileIndex + 1, projectedColumnIndex);
                if (nextFilePages != null && !nextFilePages.isEmpty()) {
                    LOG.log(System.Logger.Level.DEBUG,
                            "[{0}] Fetching pages for column ''{1}'', adding {2} pages",
                            fileManager.getFileName(currentFileIndex + 1), columnName, nextFilePages.size());

                    // Create PageReader for the new file (don't update this.pageReader yet)
                    PageInfo firstNewPage = nextFilePages.get(0);
                    if (pageReader == null || !pageReader.isCompatibleWith(firstNewPage.columnMetaData())) {
                        nextFileReader = new PageReader(
                                firstNewPage.columnMetaData(),
                                firstNewPage.columnSchema(),
                                context.decompressorFactory());
                    }
                    else {
                        // Same metadata, can reuse current reader
                        nextFileReader = pageReader;
                    }

                    // Add pages from next file (currentFileEndIndex stays the same - it marks the boundary)
                    pageInfos.addAll(nextFilePages);
                }
            }
            // If file not ready, we'll try again next time fillPrefetchQueue is called
        }

        // Fill prefetch queue from available pages
        while (prefetchQueue.size() < targetPrefetchDepth && nextPageIndex < pageInfos.size()) {
            // Check if we're crossing into the next file
            if (nextPageIndex >= currentFileEndIndex && nextFileReader != null) {
                // Switch to next file's reader
                pageReader = nextFileReader;
                nextFileReader = null;
                currentFileIndex++;
                currentFileEndIndex = pageInfos.size(); // Update boundary for potential next file
                LOG.log(System.Logger.Level.DEBUG,
                        "[{0}] Switched to new file reader for column ''{1}''",
                        fileManager.getFileName(currentFileIndex), columnName);
            }

            int pageIndex = nextPageIndex++;
            PageReader reader = this.pageReader;
            prefetchQueue.addLast(CompletableFuture.supplyAsync(
                    () -> decodePageAndRelease(pageIndex, reader), executor));
        }
    }

    /**
     * Signals that this cursor is exhausted and no more pages will be produced.
     * If eager assembly is enabled, this signals the assembly buffer to finish.
     */
    private void signalExhausted() {
        if (assemblyBuffer != null) {
            assemblyBuffer.finish();
        }
    }

    /**
     * Returns the assembly buffer if eager assembly is enabled.
     */
    public ColumnAssemblyBuffer getAssemblyBuffer() {
        return assemblyBuffer;
    }

    /**
     * Tries to load pages from the next file, blocking if necessary.
     * Called when we've exhausted the current file's pages and need more.
     *
     * @return true if pages were added from the next file, false if no more files
     */
    private boolean tryLoadNextFileBlocking() {
        if (fileManager == null || !fileManager.hasFile(currentFileIndex + 1)) {
            return false;
        }

        // Already have pages from next file?
        if (nextFileReader != null) {
            return true; // Pages already added to pageInfos
        }

        LOG.log(System.Logger.Level.DEBUG,
                "[{0}] Blocking on file load for column ''{1}''",
                fileManager.getFileName(currentFileIndex + 1), columnName);

        // Block on getting pages from next file
        List<PageInfo> nextFilePages = fileManager.getPages(currentFileIndex + 1, projectedColumnIndex);
        if (nextFilePages == null || nextFilePages.isEmpty()) {
            return false;
        }

        // Create PageReader for the new file
        PageInfo firstNewPage = nextFilePages.get(0);
        if (pageReader == null || !pageReader.isCompatibleWith(firstNewPage.columnMetaData())) {
            nextFileReader = new PageReader(
                    firstNewPage.columnMetaData(),
                    firstNewPage.columnSchema(),
                    context.decompressorFactory());
        }
        else {
            nextFileReader = pageReader;
        }

        // Add pages from next file
        pageInfos.addAll(nextFilePages);

        LOG.log(System.Logger.Level.DEBUG,
                "[{0}] Loaded {1} pages for column ''{2}''",
                fileManager.getFileName(currentFileIndex + 1), nextFilePages.size(), columnName);

        return true;
    }

    /**
     * Gets the current file name for logging purposes.
     */
    private String getCurrentFileName() {
        if (fileManager != null) {
            String fileName = fileManager.getFileName(currentFileIndex);
            if (fileName != null) {
                return fileName;
            }
        }
        return initialFileName != null ? initialFileName : "unknown";
    }

    /**
     * Decode the page at the given index, release its PageInfo reference, and return the decoded page.
     * Uses the current pageReader for decoding.
     */
    private Page decodePageAndRelease(int pageIndex) {
        return decodePageAndRelease(pageIndex, pageReader);
    }

    /**
     * Decode the page at the given index, release its PageInfo reference, and return the decoded page.
     * The reader is passed explicitly to ensure pages are decoded with the correct
     * reader even when async tasks execute after the pageReader has been updated
     * for a subsequent file.
     */
    private Page decodePageAndRelease(int pageIndex, PageReader reader) {
        Page page = decodePage(pageInfos.get(pageIndex), reader);
        pageInfos.set(pageIndex, null);
        return page;
    }

    private Page decodePage(PageInfo pageInfo, PageReader reader) {
        try {
            return reader.decodePage(pageInfo.pageData(), pageInfo.dictionary());
        }
        catch (Exception e) {
            throw new RuntimeException("Error decoding page for column " + pageInfo.columnSchema().name(), e);
        }
    }
}
