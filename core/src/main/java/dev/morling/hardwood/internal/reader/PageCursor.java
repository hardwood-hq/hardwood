/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.reader;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

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

    private final List<PageInfo> pageInfos;
    private final Executor executor;
    private final String columnName;
    private final PageReader pageReader;
    private int nextPageIndex = 0;

    // Adaptive prefetch queue
    private final Deque<CompletableFuture<Page>> prefetchQueue = new ArrayDeque<>();
    private int targetPrefetchDepth = INITIAL_PREFETCH_DEPTH;
    private int hitCount = 0;
    private int missCount = 0;

    public PageCursor(List<PageInfo> pageInfos, Executor executor) {
        this.pageInfos = pageInfos;
        this.executor = executor;
        if (pageInfos.isEmpty()) {
            this.columnName = "unknown";
            this.pageReader = null;
        }
        else {
            PageInfo first = pageInfos.get(0);
            this.columnName = first.columnSchema().name();
            this.pageReader = new PageReader(
                    first.columnMetaData(),
                    first.columnSchema());
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
     */
    private void fillPrefetchQueue() {
        while (prefetchQueue.size() < targetPrefetchDepth && nextPageIndex < pageInfos.size()) {
            int pageIndex = nextPageIndex++;
            prefetchQueue.addLast(CompletableFuture.supplyAsync(
                    () -> decodePage(pageInfos.get(pageIndex)), executor));
        }
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
