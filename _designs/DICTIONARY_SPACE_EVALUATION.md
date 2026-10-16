# Dictionary-space predicate evaluation

Describes how a drain-side binary matcher decides a dictionary-encoded batch once per referenced dictionary entry rather than once per row: the per-value matcher contract, the per-entry outcomes, when they are decided and how long they live, and how rows without an entry id are decided.

Related documents:

- [RECORD_FILTERING.md](RECORD_FILTERING.md): the drain-side matchers this wraps, their eligibility and the merge of their masks
- [VALUE_DECODE.md](VALUE_DECODE.md): the dictionary page and the per-value entry indices a batch records
- [COLUMN_READER.md](COLUMN_READER.md): the batch layout, including the deferred byte views of dictionary values
- [READ_PIPELINE.md](READ_PIPELINE.md): why a batch of a binary column holds values of one column chunk
- [FETCH_PLANNING.md](FETCH_PLANNING.md): the fetch plans that read a column chunk's pages, and the row ranges and masks that narrow them

## Scope

Every binary predicate that `BatchFilterCompiler` compiles to a batch matcher is evaluated in dictionary space: equality, inequality, the four orderings and membership. Negated membership resolves to a conjunction of inequality leaves and is evaluated in dictionary space through them. The batch-filter eligibility rules in [RECORD_FILTERING.md](RECORD_FILTERING.md) stay authoritative, and dictionary encoding adds no rule of its own: a leaf on the batch path is evaluated in dictionary space whatever its `Comparison`, and a leaf off it (a nested leaf, a predicate on `NestedRowReader`) is evaluated per row.

Fixed-width physical types (`INT32`, `INT64`, `FLOAT`, `DOUBLE`) are evaluated per row even when dictionary encoded.

## Matcher contract

`BinaryBatchMatcher` has two operations with the same value semantics:

- `test` writes the result bitmap for a whole batch; and
- `testValue` decides one non-null value held in `bytes[from, to)`.

Each concrete binary matcher keeps its own whole-batch loop. `testValue` is the semantic primitive dictionary evaluation decides an entry with, so equality, ordering, decimal comparison and negation have one definition each: an entry of a variable-width decimal column compares by value, and a padded spelling of the literal matches as it does per row.

Short-value equality (`EQ` and `NOT_EQ` against a literal of at most eight bytes) has a matcher of its own, `BinaryShortEqBatchMatcher`, rather than being the one-member case of short-value `IN`. C2 compiles `ShortValueEquality`'s loop over the `IN` members from the member counts it has profiled, so equality sharing that loop would decide how it is compiled for every later `IN`. For the same reason `ShortValueEquality` keeps its per-row decision inline in the whole-batch loop and gives `testValue` its own copy: a decision shared with dictionary evaluation's per-entry calls would make the `IN` loop's compiled code depend on which columns were read before.

## Per-entry outcomes

`BatchFilterCompiler` wraps every compiled binary leaf in a `DictionaryBinaryBatchMatcher`. A batch without a dictionary goes to the delegate's `test`. For a batch with one, the wrapper keeps one byte of state per entry of the batch's dictionary:

- `UNKNOWN`: the entry has not been decided;
- `MATCH`: the entry satisfies the delegate; or
- `NO_MATCH`: it does not.

An entry is decided through the delegate's `testValue` over the entry's bytes in the dictionary, at most once per column chunk; every later row and batch holding the entry reads the stored outcome. A null row is skipped before either lookup and has an unset bit.

The state belongs to one dictionary object, which is one column chunk's dictionary, and is keyed by its identity. A batch with a different dictionary resets the state to `UNKNOWN` over that dictionary's size, reusing the array.

### When entries are decided

A column chunk's dictionary holds only values that occur in the chunk, so a read that decodes every row of the chunk references every entry. For such a read the wrapper decides every entry when the dictionary arrives, in dictionary order. The rows reference entries in arbitrary order, and a large dictionary does not fit in cache, so deciding entries front to back costs less than deciding them as rows first reach them, for the same set of comparisons.

Any other read decides an entry when a row first references it. Page pruning, row masks and row limits then leave the entries no surviving row references undecided, so comparison work is bounded by the distinct entries the read touches rather than by the dictionary's size.

`RowGroupIterator` decides per column chunk whether the read decodes every row, and the fetch plan records it on the chunk's dictionary (`Dictionary.ByteArrayDictionary.markReadInFull()`) before handing out the first page:

- an indexed read (`IndexedFetchPlan`) decodes every row when the read has no row limit and needs every page without a row mask;
- a sequential read (`SequentialFetchPlan`) decodes every row when the read has no row limit, no row ranges, and no predicate leaf on the column that could drop a page by its inline statistics. The plan reads the dictionary page before any data page header, so it cannot learn in time whether such a leaf drops a page.

Leaves on other columns do not narrow the rows a column's matcher sees: a sequential plan replaces a page it drops by a page of nulls in that column only, and `AndBatchMatcher` evaluates both sides over the whole batch. A consumer that stops reading early without a row limit leaves the rest of the chunk unread after every entry was decided; the cost is bounded by one pass over the dictionary.

## Rows without an entry id

A batch records an entry id per value ([VALUE_DECODE.md](VALUE_DECODE.md)) and draws on at most one dictionary, since a read with a binary column ends its batches at row-group boundaries ([READ_PIPELINE.md](READ_PIPELINE.md)). An id of `-1` therefore marks a null or a value written `PLAIN` after the chunk's dictionary filled up. The wrapper decides such a non-null row through the delegate's `testValue` over the row's byte view.

The wrapper reads no other view. `ColumnBatchMatcher.readsEveryValueView()` returns `false` for it, and for a same-column `AND` or `OR` whose sides both do, so the column worker leaves the batch's dictionary values with their views deferred ([COLUMN_READER.md](COLUMN_READER.md)). A filtered read that consumes the column through its dictionary ids or its strings then never builds the views. The first `-1` row of a batch with deferred views builds them, as any reader's first view access does.

Tests: `DictionaryBinaryBatchMatcherTest`, `DictionaryReadInFullTest`, `DictionarySpaceEvaluationTest`, `ShortValueEqualityTest`, `ColumnWorkerTest`.
