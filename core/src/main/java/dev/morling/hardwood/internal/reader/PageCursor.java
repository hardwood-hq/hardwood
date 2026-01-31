/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.reader;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import dev.morling.hardwood.reader.HardwoodContext;

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
 */
public class PageCursor {

    private static final System.Logger LOG = System.getLogger(PageCursor.class.getName());

    private static final int INITIAL_PREFETCH_DEPTH = 2;
    private static final int MAX_PREFETCH_DEPTH = 8;
    private static final int HITS_TO_DECREASE = 20;
    private static final int MISSES_TO_INCREASE = 2;

    private final ArrayList<PageInfo> pageInfos;
    private final HardwoodContext context;
    private final Executor executor;
    private final String columnName;
    private final int projectedColumnIndex;
    private PageReader pageReader;
    private int nextPageIndex = 0;

    // Cross-file prefetching support
    private final CrossFilePrefetchCoordinator crossFileCoordinator;

    // Adaptive prefetch queue
    private final Deque<CompletableFuture<Page>> prefetchQueue = new ArrayDeque<>();
    private int targetPrefetchDepth = INITIAL_PREFETCH_DEPTH;
    private int hitCount = 0;
    private int missCount = 0;

    // Flag indicating this cursor needs pages from next file (set during iteration, acted on at batch boundary)
    private boolean pendingNextFilePages = false;

    /**
     * Creates a PageCursor for single-file reading.
     */
    public PageCursor(List<PageInfo> pageInfos, HardwoodContext context) {
        this(pageInfos, context, null, -1);
    }

    /**
     * Creates a PageCursor with optional cross-file prefetching support.
     *
     * @param pageInfos initial pages from the first file
     * @param context hardwood context with executor
     * @param crossFileCoordinator coordinator for getting pages from next files (may be null)
     * @param projectedColumnIndex the projected column index for cross-file page requests
     */
    public PageCursor(List<PageInfo> pageInfos, HardwoodContext context,
                      CrossFilePrefetchCoordinator crossFileCoordinator, int projectedColumnIndex) {
        this.pageInfos = new ArrayList<>(pageInfos);
        this.context = context;
        this.executor = context.executor();
        this.crossFileCoordinator = crossFileCoordinator;
        this.projectedColumnIndex = projectedColumnIndex;
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
    }

    /**
     * Check if there are more pages available.
     */
    public boolean hasNext() {
        return nextPageIndex < pageInfos.size() || !prefetchQueue.isEmpty();
    }

    /**
     * Get the next decoded page. Blocks if the page is still being decoded.
     *
     * @return the next page, or null if exhausted
     */
    public Page nextPage() {
        if (prefetchQueue.isEmpty()) {
            if (nextPageIndex >= pageInfos.size()) {
                return null;
            }
            // Queue empty but pages remain - decode synchronously and increase depth
            LOG.log(System.Logger.Level.DEBUG, "Prefetch queue empty for column ''{0}''", columnName);
            targetPrefetchDepth = Math.min(targetPrefetchDepth + 1, MAX_PREFETCH_DEPTH);
            return decodePage(pageInfos.get(nextPageIndex++));
        }

        CompletableFuture<Page> future = prefetchQueue.pollFirst();

        if (future.isDone()) {
            // Hit: prefetch was ready
            hitCount++;
            missCount = 0;
            if (hitCount >= HITS_TO_DECREASE && targetPrefetchDepth > INITIAL_PREFETCH_DEPTH) {
                targetPrefetchDepth--;
                hitCount = 0;
            }
        }
        else {
            // Miss: had to wait
            LOG.log(System.Logger.Level.DEBUG, "Prefetch miss for column ''{0}'', depth={1}",
                    columnName, targetPrefetchDepth);
            missCount++;
            hitCount = 0;
            if (missCount >= MISSES_TO_INCREASE) {
                targetPrefetchDepth = Math.min(targetPrefetchDepth + 1, MAX_PREFETCH_DEPTH);
                LOG.log(System.Logger.Level.DEBUG, "Increasing prefetch depth for column ''{0}'' to {1}",
                        columnName, targetPrefetchDepth);
                missCount = 0;
            }
        }

        // Refill queue after consuming
        fillPrefetchQueue();

        return future.join();
    }

    /**
     * Fill the prefetch queue up to targetPrefetchDepth.
     * If cross-file coordinator is available and current file pages are exhausted,
     * marks this cursor as pending next file pages (to be added at batch boundary).
     */
    private void fillPrefetchQueue() {
        // Fill from current file's pages
        while (prefetchQueue.size() < targetPrefetchDepth && nextPageIndex < pageInfos.size()) {
            int pageIndex = nextPageIndex++;
            prefetchQueue.addLast(CompletableFuture.supplyAsync(
                    () -> decodePage(pageInfos.get(pageIndex)), executor));
        }

        // If queue not full and current file exhausted, mark as needing next file pages
        // Don't add pages here - that happens at batch boundary to keep all columns in sync
        if (crossFileCoordinator != null && prefetchQueue.size() < targetPrefetchDepth
                && nextPageIndex >= pageInfos.size() && !pendingNextFilePages) {
            pendingNextFilePages = true;
            LOG.log(System.Logger.Level.DEBUG,
                    "Column ''{0}'' marked as pending next file pages", columnName);
        }
    }

    /**
     * Check if this cursor needs pages from the next file.
     *
     * @return true if this cursor has exhausted current file pages and needs more
     */
    public boolean needsNextFilePages() {
        return pendingNextFilePages;
    }

    /**
     * Check if the prefetch queue is running low and we're about to need next file pages.
     * Used by ColumnValueIterator to stop reading before triggering file transition.
     *
     * @return true if the queue is low and current file pages are nearly exhausted
     */
    public boolean isNearFileTransition() {
        // We're near a file transition if:
        // 1. We have a cross-file coordinator with more files available
        // 2. All current file pages have been queued (nextPageIndex >= pageInfos.size())
        // 3. The prefetch queue is running low (less than half full)
        return crossFileCoordinator != null
                && crossFileCoordinator.hasMoreFiles()
                && nextPageIndex >= pageInfos.size()
                && prefetchQueue.size() < targetPrefetchDepth / 2;
    }

    /**
     * Add pages from the next file to this cursor.
     * Called from MultiFileRowReader at batch boundaries to ensure all columns transition together.
     */
    public void addNextFilePages() {
        if (!pendingNextFilePages || crossFileCoordinator == null) {
            return;
        }

        List<PageInfo> nextFilePages = crossFileCoordinator.getNextFilePages(projectedColumnIndex);
        if (nextFilePages != null && !nextFilePages.isEmpty()) {
            LOG.log(System.Logger.Level.DEBUG,
                    "Cross-file prefetch for column ''{0}'', adding {1} pages from next file",
                    columnName, nextFilePages.size());

            // Update PageReader for the new file's column metadata if different
            PageInfo firstNewPage = nextFilePages.get(0);
            if (pageReader == null || !pageReader.isCompatibleWith(firstNewPage.columnMetaData())) {
                pageReader = new PageReader(
                        firstNewPage.columnMetaData(),
                        firstNewPage.columnSchema(),
                        context.decompressorFactory());
            }

            // Extend pageInfos with next file's pages
            pageInfos.addAll(nextFilePages);

            // Fill the prefetch queue from the new pages
            while (prefetchQueue.size() < targetPrefetchDepth && nextPageIndex < pageInfos.size()) {
                int pageIndex = nextPageIndex++;
                prefetchQueue.addLast(CompletableFuture.supplyAsync(
                        () -> decodePage(pageInfos.get(pageIndex)), executor));
            }
        }

        pendingNextFilePages = false;
    }

    /**
     * Decode a page from its PageInfo.
     */
    private Page decodePage(PageInfo pageInfo) {
        try {
            return pageReader.decodePage(pageInfo.pageData(), pageInfo.dictionary());
        }
        catch (Exception e) {
            throw new RuntimeException("Error decoding page for column " + pageInfo.columnSchema().name(), e);
        }
    }
}
