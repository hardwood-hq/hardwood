/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.parquet.schema;

import java.util.List;

/**
 * Root message type for Parquet schema.
 * <p>
 * The MessageType is the root of a Parquet schema. It is always REQUIRED and
 * contains the top-level fields of the schema.
 * </p>
 */
public class MessageType extends GroupType {

    /**
     * Create a message type with a list of fields.
     *
     * @param name the message name
     * @param fields the top-level fields
     */
    public MessageType(String name, List<Type> fields) {
        super(Repetition.REQUIRED, name, null, fields);
    }

    /**
     * Create a message type with varargs fields.
     *
     * @param name the message name
     * @param fields the top-level fields
     */
    public MessageType(String name, Type... fields) {
        super(Repetition.REQUIRED, name, null, List.of(fields));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("message ").append(getName()).append(" {\n");
        for (Type field : getFields()) {
            sb.append("  ").append(field).append("\n");
        }
        sb.append("}");
        return sb.toString();
    }
}
