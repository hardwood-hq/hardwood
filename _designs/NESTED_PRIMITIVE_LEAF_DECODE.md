# Design: primitive leaf decode on the nested read path

**Status: Planned.** Tracking issue: #1164.

## Goal

The flat and nested read paths store leaf values in the same primitive
arrays. Both decode them the same way: read the primitive out of the
array and hand it to a `LogicalTypeConverter` entry point that takes
that primitive. Neither path creates an intermediate box on an accessor
whose result is already a reference type, and the two rules that
classify a leaf are stated once for both.

## Storage

`NestedBatchIndex` holds one array per projected column, in the same
representations `FlatRowReader` uses: `int[]`, `long[]`, `float[]`,
`double[]`, `boolean[]`, and `BinaryBatchValues` for the two byte-array
physical types.

A flyweight reads a stored value by indexing that array through the
column's representation, which is what the primitive accessors do
today: `PqStructImpl.readInt` is
`((int[]) batch.valueArrays[projCol])[idx]`. The logical-type accessors
read the same way.

`NestedBatchIndex.getValue` boxes, and remains for the paths whose
result is an `Object` regardless: `PqList.values()`, which returns
`List<Object>`, and `NestedBatchIndex.decodeLeaf`, which serves the
generic accessors. A box on those paths is the return type, not an
artifact.

## Typed accessors

A typed accessor names the type it returns, so it reads the primitive
and calls the decode for that type directly:

```java
private LocalDate readDate(FieldDesc.Primitive child) {
    int projCol = child.projectedCol();
    int idx = resolveValueIndex(projCol);
    if (batch.isElementNull(projCol, idx)) {
        return null;
    }
    return LogicalTypeConverter.intToDate(
            ((int[]) batch.valueArrays[projCol])[idx]);
}
```

`PqStructImpl`, `NestedBatchDataView` and `PqMapImpl` each carry one
such helper per logical type rather than a single `Class`-keyed helper,
so the logical accessors sit alongside `readInt`, `readLong` and
`readFloat` in the same shape.

`PqListImpl.LeafList<T>` takes an `IntFunction<T>` over the element
position, so each accessor supplies a lambda that reads and decodes in
one step:

```java
public List<LocalDate> dates() {
    int projCol = listDesc.firstLeafProjCol();
    return new LeafList<>(pos -> LogicalTypeConverter.intToDate(
            ((int[]) batch.valueArrays[projCol])[pos]));
}
```

The lambda resolves the array per element rather than capturing it, so
the read is as lazy as it is today and a view does not pin the batch it
was created from.

The interned-`String` case is one such lambda, so `LeafList` carries no
flag selecting between a raw read and an interned one.

## Leaf classification

Two rules decide how a leaf decodes, and both paths ask the same
question: whether the leaf is a string, and whether it is an
unannotated `INT96`, which the format leaves unannotated and readers
conventionally treat as a timestamp.

`LeafKind` states both. It is a package-level enum in
`dev.hardwood.internal.reader` with a factory over the pair that decides
them:

```java
enum LeafKind {
    STRING, INT96_TIMESTAMP, RAW, CONVERT;

    static LeafKind of(PhysicalType type, LogicalType logicalType) { ... }
}
```

`FlatRowReader` classifies each column once at construction and switches
on the result in `getValue`. `NestedLeafDecoder` switches on it per
leaf. `BatchExchange`'s string-interning gate asks
`LeafKind.of(...) == STRING`, which is the same question the consumer
side asks, so the side that records dictionary indices and the side that
reads them back cannot disagree.

## NestedLeafDecoder

What the nested flyweights need beyond the decode table is the
`SchemaNode` unwrap: a group node is returned untouched, because struct,
list and map values are built by the flyweights and never carry a leaf
decode. `NestedLeafDecoder` holds that, over `LeafKind` and
`LogicalTypeConverter`. It serves the generic accessors; the typed ones
read primitives directly and do not go through it.

## Scope

`NestedBatchIndex.valueArrays` stays package-visible. Giving it typed
accessors so the field can go private is a worthwhile encapsulation and
is not part of this design: the field is read directly in roughly 45
places across eight classes, and the read helpers here use the idiom
those places already use.

## Type mismatches

A caller asking an accessor for a type the column does not hold surfaces
as whatever the storage array's cast raises, which is what
[EXCEPTION_MODEL.md](EXCEPTION_MODEL.md) states for this case and what
the flat path already does. #971 covers giving that failure a message
that names the column, across both paths at once.

An explicit guard runs only where the cast cannot detect the mismatch:
`FLOAT` against `FLOAT16`, which share no array distinction, and the two
`TIMESTAMP` kinds, which are both `long[]`. Those keep the guards they
have — `NestedBatchIndex.requireFloatAccess` and
`TimestampAccessorKind.require`.

## Testing

`ValueConverterTest` follows the class to `NestedLeafDecoderTest`.
`LeafKind.of` is covered directly over the physical/logical pairs that
select each kind, including an `INT96` leaf with and without a
surviving annotation.

Each logical-type accessor on `PqStruct`, `PqList` and `PqMap` is
covered for the type it returns, over both a nullable and an all-present
column, and for the `FLOAT16` and `TIMESTAMP`-kind guards. The existing
nested accessor suites carry this.
