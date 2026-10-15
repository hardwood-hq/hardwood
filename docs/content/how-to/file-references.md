<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# File References

A group annotated with the `FILE` logical type holds a reference to a range of bytes: bytes stored in the value itself, or a URI naming an object in external storage, optionally with an offset and a size. `getFile` reads such a group as a `PqFile`. The children a `FILE` group may define, and the rules a valid value follows, are listed under [FILE groups](../reference/accessors.md#file-groups).

## Read a reference

```java
try (RowReader rows = fileReader.rowReader()) {
    while (rows.hasNext()) {
        rows.next();
        PqFile photo = rows.getFile("photo");
        if (photo == null) {
            continue;   // null group
        }
        String uri = photo.uri();                 // null for a value stored inline
        String mediaType = photo.contentType();   // null when the file records none
    }
}
```

`PqStruct.getFile` reads a `FILE` field of a nested struct, `PqList.files()` the elements of a list of `FILE` groups, and `PqMap.Entry.getFileValue()` a map value. `getValue` returns a `PqFile` as well.

A value that is not a valid reference, such as one setting `offset` without `uri`, fails with an error on the call that reads it. Other rows read normally, and `getRawValue` returns the group's children as a `PqStruct` whatever they hold.

## Get the referenced bytes

Hardwood returns the stored fields and does not fetch the object a URI names. Read the bytes with your own storage client:

```java
byte[] bytes;
if (photo.inline() != null) {
    bytes = photo.inline();
}
else if (photo.hasSize()) {
    bytes = storage.readRange(photo.uri(), photo.offset(), photo.size());
}
else {
    bytes = storage.readAll(photo.uri());
}
```

`storage` stands for your client. A relative URI is resolved against a base of your choosing. `checksum()` returns the stored token, such as `SHA-256:…` or `ETAG:…`, for you to verify.

## Write a reference

Declare the group with `file`, and set a value with `setFile`:

```java
FileSchema schema = FileSchema.builder("schema")
        .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED)
        .file("photo", RepetitionType.OPTIONAL)
        .list("attachments", RepetitionType.OPTIONAL, element -> element.file(RepetitionType.OPTIONAL))
        .build();

try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(path), schema)) {
    writer.rowWriter().writeRow(row -> row
            .setLong("id", 1)
            .setFile("photo", PqFile.external("s3://bucket/bundle.tar", 4096, 51200)
                    .withContentType("image/png"))
            .setList("attachments", list -> list
                    .addFile(PqFile.inline(thumbnailBytes).withContentType("image/jpeg"))
                    .addFile(PqFile.external("s3://bucket/report.pdf"))));
}
```

`PqFile.inline` and the `PqFile.external` factories build each valid form, listed under [FILE groups](../reference/accessors.md#file-groups). `withContentType`, `withChecksum` and `withInline` add a field. The columnar writer takes the group's children as ordinary columns, `photo.uri`, `photo.offset` and so on, and a batch holding an invalid value fails with an error; see [Logical Types and Row Setters](../reference/writer.md#logical-types-and-row-setters).
