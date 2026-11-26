/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

/// The bytes a [RowPlan]'s variable-width stages hold, counted by the stages as they stage and
/// drop values, so that checking the batch against its payload limit after each record reads
/// one field.
final class StagedPayload {

    long bytes;
}
