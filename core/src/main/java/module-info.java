/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
module dev.morling.hardwood {
    requires static snappy.java;
    requires static com.github.luben.zstd_jni;
    requires static org.lz4.java;
    requires static com.aayushatharva.brotli4j;
    exports dev.morling.hardwood.row;
    exports dev.morling.hardwood.metadata;
    exports dev.morling.hardwood.reader;
    exports dev.morling.hardwood.schema;
}
