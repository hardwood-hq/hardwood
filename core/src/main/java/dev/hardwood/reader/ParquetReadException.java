/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import dev.hardwood.internal.ReadScope;

/// Thrown when a file's own bytes are wrong: the read succeeded and what it
/// returned does not say something a Parquet file can say.
///
/// A bad magic number, a footer that will not parse, a dictionary page the
/// metadata places outside its column chunk, a page whose checksum fails, values
/// that do not decode under the encoding the file declares for them.
///
/// **Trying again will not help.** That is what separates this from
/// [java.io.IOException], which the reader raises when reading the file failed —
/// a disk error, an S3 failure after its own retries have run out — and where a
/// second attempt may well succeed. A caller deciding whether to retry can decide
/// on the type alone.
///
/// Distinct again from [UnsupportedOperationException], which the reader raises
/// for a file that is entirely correct and that this library cannot read: Parquet
/// Modular Encryption, an encoding not implemented, a compression codec whose
/// library is absent. Retrying will not help there either, but the remedy is a
/// dependency or a different tool rather than a different file.
///
/// Unchecked, because there is nothing for a caller to do at the point of failure.
public class ParquetReadException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /// Where the read was when this was raised, or `null` if it was raised
    /// outside a read.
    ///
    /// Taken at construction, which is the only moment the answer is known: a
    /// frame that catches this is by definition no longer in the place the
    /// failure happened. Not serialised — the message states the same facts,
    /// and an exception that has been through serialisation is being read
    /// rather than acted on.
    ///
    /// That the file being wrong is the type that carries one is the whole
    /// rule. An [UnsupportedOperationException] says the file is correct and
    /// this library will not read it, so there is nothing at any byte for a
    /// caller to go and look at and it is given no place to name — not by a
    /// check at some raise site, but because it is not this type.
    private final transient ReadScope.Place place;

    public ParquetReadException(String message) {
        super(message);
        this.place = ReadScope.current();
    }

    public ParquetReadException(String message, Throwable cause) {
        super(message, cause);
        this.place = ReadScope.current();
    }

    /// The message, behind where the read was.
    ///
    /// Composed here rather than at construction so that nothing has to rebuild
    /// the exception — and with it its type, which callers catch on — in order
    /// to say where it was.
    @Override
    public String getMessage() {
        String message = super.getMessage();
        return place == null ? message : place.describe() + message;
    }
}
