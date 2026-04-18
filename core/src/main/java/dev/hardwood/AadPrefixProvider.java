/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

/// AAD prefix provider if AAD prefix is not stored in file and supply_aad_prefix is true
/// Caller needs to provide its implementation to provide AAD prefix.
@FunctionalInterface
public interface AadPrefixProvider {

    /// Provides the AAD prefix if not stored in file
    byte[] provideAadPrefix();
}
