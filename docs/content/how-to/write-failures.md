<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Handle Write Failures

A `ParquetFileWriter` publishes its file on `close()`. When writing fails part way, the file should not be published.

## A Write Throws

No extra code is needed. When `ColumnWriter.writeBatch` or `RowWriter.writeRow` throws, the writer rejects further writes, and `close()` discards the output. This holds for every exception a write call throws, including one thrown by the filler.

```java
try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
    ColumnWriter columns = writer.columnWriter();
    for (int[] chunk : chunks) {
        columns.writeBatch(batch -> batch.ints("id", chunk));   // throws: nothing is published
    }
}
```

A writer whose write call has thrown cannot be written to again, so skipping a rejected record and carrying on with the next is not possible: the file has to be written again from the start.

## The Data Source Throws

The writer does not see an exception raised by the code producing the data outside a filler, so `close()` would publish the rows written so far. Call `abort()` before the exception leaves the block:

```java
try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
    try {
        ColumnWriter columns = writer.columnWriter();
        while (source.hasNext()) {
            int[] chunk = source.next();                          // may throw
            columns.writeBatch(batch -> batch.ints("id", chunk));
        }
    }
    catch (Exception e) {
        try {
            writer.abort();
        }
        catch (IOException discardFailure) {
            e.addSuppressed(discardFailure);
        }
        throw e;
    }
}
```

`abort()` discards the output and closes the writer, so the `close()` at the end of the block does nothing. Call `abort()` the same way to abandon a file for any other reason.

## The Output Cannot Be Discarded

When the output cannot be released, `close()` or `abort()` throws the `IOException`. Adding it to the original exception with `addSuppressed`, as above, keeps both. On a local file, the temporary sibling of the target path (`<name>.hardwood-tmp`) may then be left behind.

The complete rules are in the [Writer Reference](../reference/writer.md#finishing-and-abandoning-a-file).
