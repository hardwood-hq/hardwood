/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.fetch;

import java.io.IOException;

import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.reader.batch.Dictionary;
import dev.hardwood.internal.reader.decode.DataPage;
import dev.hardwood.internal.reader.decode.DictionaryParser;
import dev.hardwood.internal.reader.memory.WorkingSet;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.PageType;
import dev.hardwood.schema.ColumnSchema;

/// The dictionary step of both walks: a dictionary page whose bytes the walk has read, parsed as a
/// task the read admits ([WorkingSet.Column#parseDictionary]), or a dictionary pruning parsed,
/// charged to the column as it takes it.
final class DictionaryStep {

    private DictionaryStep() {
    }

    /// The dictionary of the chunk whose first page is `page`, or what its parse waits for; a page
    /// that is no dictionary page has none, and takes no task.
    static WorkingSet.DictionaryParse parse(DataPage page, ColumnSchema column, ColumnMetaData metaData,
                                            HardwoodContextImpl context, WorkingSet.Column workingSet)
            throws IOException {
        PageHeader header = page.header();
        if (header.type() != PageType.DICTIONARY_PAGE) {
            return new WorkingSet.DictionaryParse(null, null);
        }
        // Not retitled on the way out. The parser says what is wrong with the dictionary (a
        // checksum that disagrees, a header that will not parse) and naming the step that noticed
        // would replace that with less.
        return workingSet.parseDictionary(header.uncompressedPageSize(),
                () -> DictionaryParser.parsePage(header, page.body(), column, metaData, context, workingSet));
    }

    /// `preloaded`, the dictionary pruning parsed for the chunk, charged to the column as it takes
    /// it; `null` where pruning parsed none.
    static Dictionary preloaded(Dictionary preloaded, WorkingSet.Column workingSet) {
        if (preloaded != null) {
            workingSet.dictionary(preloaded);
        }
        return preloaded;
    }
}
