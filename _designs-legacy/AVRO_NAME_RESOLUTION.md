# Avro Name Resolution

**Status: Implemented.**

Avro named types emitted from a Parquet schema use grammar-legal, deterministic names.

## Sanitization

A name is legal when it matches `[A-Za-z_][A-Za-z0-9_]*`. Hardwood preserves legal names. For other names, each character outside `[A-Za-z0-9_]` becomes `_`; an initial digit receives a leading `_`; an empty name becomes `_`.

## Scope-local names

Named fields are resolved within each sibling scope. Sanitization collisions use the legal raw member as the unsuffixed name when one exists. Otherwise, the member with the smallest natural `String` order receives the unsuffixed name. Other colliding members receive `_2`, `_3`, and later suffixes, skipping names already reserved in the scope.

Two siblings with the same raw name are rejected. Resolution uses schema-node identity and the complete unprojected schema tree.

The suffix a colliding member receives is a function of every name in its scope, because the scope reserves each unsuffixed candidate before any suffix is handed out. Siblings `a-b` and `a.b` resolve to `a_b` and `a_b_2`; the same two beside a third sibling literally named `a_b_2` resolve to `a_b` and `a_b_3`. A sibling whose own name occupies the suffix space therefore renames the members that draw from it. No suffix scheme avoids this: `a_b_2` is a legal raw name, and a legal raw name is never rewritten.

## Root names

The root name is split at the last dot. The final segment is the local name. Earlier segments form the namespace. Each segment is sanitized independently. A root stores its original Parquet name in the `hardwood.parquetName` schema property when its emitted local name differs from that original name.

## Canonical fixed types

The `interval` and `float16` fixed types are emitted with those exact names and no
namespace. A root named `interval` or `float16` therefore has the same full name as the
canonical type it would sit beside. Conversion rejects that schema, naming the root and the
canonical type.

The rejection is decided from the complete unprojected schema, so a file either converts or
does not, whatever projection is applied. A projection that excludes the conflicting column
is rejected too.

## Descendant namespaces

A descendant named type is placed in the namespace formed by the root full name followed by the resolved local names of its preceding value-path segments. LIST and MAP value positions contribute their container names. Synthetic `list` and `key_value` encoding wrappers do not contribute names.

The root is not appended to its own namespace. Therefore descendants of `schema` include `schema.home.address`, and a child named `root` below root `root` becomes `root.root`.

## Recovering Parquet names

When a record, fixed type, or field is rewritten, Hardwood attaches the exact raw Parquet segment under `hardwood.parquetName`. For a rewritten root, the property contains the original full Parquet root name. The property is omitted when the emitted local name equals the raw name. The property survives Avro schema serialization and parser round-trips.

## Invariants

- Every emitted record and fixed full name is legal and unique.
- A legal Parquet name is never rewritten.
- Descendant names are stable under projection, and under sibling changes outside the suffix space a sanitization collision draws from.
- A descendant cannot collide with the root or the canonical `interval` and `float16` fixed types.
- Repeated `interval` and `float16` logical types retain their canonical names and sizes.
