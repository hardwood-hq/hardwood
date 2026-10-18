/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

/// The pipeline that decodes one column of a read into the [BatchExchange] its reader polls:
/// a [ScheduledColumn].
public interface ColumnPipeline extends AutoCloseable {

    /// Stops the pipeline and waits until it is quiescent, so that the caller can release what the
    /// read's `InputFile`s own. The first close of a read's pipelines stops all of them, so a
    /// reader closing them one after another waits for them all at once.
    @Override
    void close();
}
