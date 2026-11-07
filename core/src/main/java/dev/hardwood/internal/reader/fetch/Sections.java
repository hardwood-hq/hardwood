/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.fetch;

import java.util.List;

import dev.hardwood.InputFile;

/// Where a read's fetch plans find their bytes: the sections of each planned work item.
public interface Sections {

    /// The sections inserted for work item `workItemIndex`, `null` when it has none.
    List<? extends Section> sections(int workItemIndex);

    /// The sections of work item `workItemIndex` of file `fileIndex`, one per span in position
    /// order: those inserted before, if the work item has any, or new ones.
    List<? extends Section> insert(int fileIndex, int workItemIndex, InputFile inputFile, List<Span> spans);

    /// A section of `length` bytes at `start` that a walk has reached past the sections its plan
    /// holds, needed at once.
    Section insertDemanded(int workItemIndex, InputFile inputFile, long start, int length, String reason);

    /// A section planning reads: `length` bytes at `start`, held by `holders` work items, started
    /// when demanded ([Section#demand]).
    Section planned(InputFile inputFile, long start, int length, String reason, int holders);

    /// The section of `span` sliced from bytes the read holds without a request (a file in
    /// memory, or the file's tail), `null` when it holds none of them.
    Section heldInPlace(InputFile inputFile, Span span);
}
