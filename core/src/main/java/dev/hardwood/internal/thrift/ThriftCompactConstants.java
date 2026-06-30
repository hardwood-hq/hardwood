/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

/// Thrift Compact Protocol type codes, defined once and shared by the reader and
/// writer so the wire constants are not duplicated.
/// Reference: https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md
final class ThriftCompactConstants {

    /// Type codes for struct field headers.
    public enum FieldType {

        BOOLEAN_TRUE(Codes.BOOLEAN_TRUE),
        BOOLEAN_FALSE(Codes.BOOLEAN_FALSE),
        BYTE(Codes.BYTE),
        I16(Codes.I16),
        I32(Codes.I32),
        I64(Codes.I64),
        DOUBLE(Codes.DOUBLE),
        BINARY(Codes.BINARY),
        LIST(Codes.LIST),
        SET(Codes.SET),
        MAP(Codes.MAP),
        STRUCT(Codes.STRUCT),
        UUID(Codes.UUID);

        private final byte code;

        FieldType(byte code) {
            this.code = code;
        }

        /// The Thrift Compact Protocol wire code for this type.
        byte code() {
            return code;
        }

        /// Wire type codes as compile-time constants. Callers that dispatch in a
        /// `switch` on the raw wire byte — notably [ThriftCompactReader] — reference
        /// these so the codes are defined once, without re-declaring the literals.
        /// The [FieldType] and [ElementType] enums are built from them too.
        static final class Codes {

            static final byte BOOLEAN_TRUE = 0x01;
            static final byte BOOLEAN_FALSE = 0x02;
            static final byte BYTE = 0x03;
            static final byte I16 = 0x04;
            static final byte I32 = 0x05;
            static final byte I64 = 0x06;
            static final byte DOUBLE = 0x07;
            static final byte BINARY = 0x08;
            static final byte LIST = 0x09;
            static final byte SET = 0x0A;
            static final byte MAP = 0x0B;
            static final byte STRUCT = 0x0C;
            static final byte UUID = 0x0D;

            private Codes() {
            }
        }
    }

    /// Type codes for list/set/map elements. These mirror [FieldType] except boolean:
    /// a collection has a single [#BOOL] element type, whereas a field packs
    /// true/false into the type. `BOOL` is written as `1` — the de-facto standard;
    /// the spec's original code was `2` and readers should accept either.
    public enum ElementType {

        BOOL(FieldType.Codes.BOOLEAN_TRUE),
        BYTE(FieldType.Codes.BYTE),
        I16(FieldType.Codes.I16),
        I32(FieldType.Codes.I32),
        I64(FieldType.Codes.I64),
        DOUBLE(FieldType.Codes.DOUBLE),
        BINARY(FieldType.Codes.BINARY),
        LIST(FieldType.Codes.LIST),
        SET(FieldType.Codes.SET),
        MAP(FieldType.Codes.MAP),
        STRUCT(FieldType.Codes.STRUCT),
        UUID(FieldType.Codes.UUID);

        private final byte code;

        ElementType(byte code) {
            this.code = code;
        }

        /// The Thrift Compact Protocol wire code for this element type.
        byte code() {
            return code;
        }
    }

    private ThriftCompactConstants() {
    }
}
