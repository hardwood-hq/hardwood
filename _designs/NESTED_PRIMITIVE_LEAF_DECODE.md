# Design: leaf decode on the read path

**Status: Implemented.** Tracking issue: #1164.

How a row-reader accessor turns a stored leaf value into the Java value it
returns. The flat and nested paths do this the same way.

## Two routes

A **typed** accessor names the type it returns, so it reads the stored
primitive out of the column array and calls the `LogicalTypeConverter` entry
point for that type. Nothing is boxed on the way: `getDate` reaches
`intToDate(int)` from an `int[]`, never through an `Integer`.

A **generic** accessor — `getValue`, `PqList.values()`, `PqMap.Entry.getValue()`
— returns whatever the column holds, so it starts from an `Object` and
dispatches. Those go through `NestedLeafDecoder.decode`, which adds the
`SchemaNode` unwrap and returns a group node untouched, since struct, list and
map values are built by the flyweights and carry no leaf decode.

The box on the generic route is the return type, not an artifact, and stays.

## Leaf classification

Two rules decide how a leaf decodes: whether it is a string, and whether it is
an unannotated `INT96`, which the format leaves unannotated and readers
conventionally treat as a timestamp. The `INT96` rule keys on the annotation's
absence — one that survives into the schema is one no physical type is imposed
on, `FileSchema` having dropped any that `INT96` cannot carry.

`LeafKind` is the single statement of both. `FlatRowReader` classifies once per
column at construction, `NestedLeafDecoder` per leaf, and the string-interning
gate on the recording side (`BatchExchange`) asks it the same question the
consumer side does, so the side that records dictionary indices and the side
that reads them back cannot disagree.

## Byte-array payloads

`DECIMAL`, `UUID` and `INTERVAL` over a byte-array column never carried a box —
the stored value is already a `byte[]`. They carried a copy:
`BinaryBatchValues.byteArrayAt` materialises an array that the decode reads its
9, 16 or 12 bytes out of and drops.

`BinaryBatchValues.decimalAt`, `uuidAt` and `intervalAt` read the payload where
it sits, each backed by an offset-taking overload on `LogicalTypeConverter`, in
the shape `float16At` already had. Whether the discarded copy survives is
otherwise left to escape analysis, and that decision is not reliable: on a list
of `FIXED_LEN_BYTE_ARRAY` decimals it was eliminated before this change and not
after, costing more than reading the primitive directly had saved. Reading in
place does not depend on the decision going the right way.

## Type mismatches

Asking an accessor for a type the column does not hold surfaces as whatever the
storage array's cast raises, which is what
[EXCEPTION_MODEL.md](EXCEPTION_MODEL.md) states for this case; #971 covers
giving that failure a message that names the column, across both paths at once.

An explicit guard runs only where the cast cannot detect the mismatch: `FLOAT`
against `FLOAT16` and the two `TIMESTAMP` kinds, which share their array type
(`NestedBatchIndex.requireFloatAccess`, `TimestampAccessorKind`); and a group
element against a leaf, where `PqListImpl.requirePrimitiveElement` and
`PqMapImpl.requirePrimitiveValue` stop an accessor reading the group's first
leaf column and decoding whatever it holds.

## Scope

`NestedBatchIndex.valueArrays` stays package-visible and is read directly by the
flyweights. Giving it typed accessors so the field can go private is a separate
encapsulation: it is read in roughly 45 places across eight classes.
