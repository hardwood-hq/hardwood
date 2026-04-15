/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

/// AesGcmV1 encryption algorithm
///
/// @param aadPrefix AAD prefix, or 'null' if absent
/// @param aadFileUnique Unique file identifier part of AAD suffix, or 'null' if absent
/// @param supplyAadPrefix In files encrypted with AAD prefix without storing it,readers must supply the prefix, or 'null' if absent
public record AesGcmCtrV1(
        byte[] aadPrefix,
        byte[] aadFileUnique,
        boolean supplyAadPrefix) {
}
