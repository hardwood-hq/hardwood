/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.hadoop.fs;

import java.net.URI;

/**
 * Minimal Hadoop Path shim that wraps java.nio.file.Path.
 * <p>
 * This class provides API compatibility with Hadoop's Path class without
 * requiring the Hadoop dependency. It wraps a standard Java NIO Path.
 * </p>
 */
public class Path {

    private final java.nio.file.Path nioPath;

    /**
     * Create a Path from a string path.
     *
     * @param pathString the path string
     */
    public Path(String pathString) {
        this.nioPath = java.nio.file.Path.of(pathString);
    }

    /**
     * Create a Path from a Java NIO Path.
     *
     * @param nioPath the NIO path
     */
    public Path(java.nio.file.Path nioPath) {
        this.nioPath = nioPath;
    }

    /**
     * Create a Path from a URI.
     *
     * @param uri the URI
     */
    public Path(URI uri) {
        this.nioPath = java.nio.file.Path.of(uri);
    }

    /**
     * Create a Path by resolving a child path against a parent.
     *
     * @param parent the parent path
     * @param child the child path string
     */
    public Path(Path parent, String child) {
        this.nioPath = parent.nioPath.resolve(child);
    }

    /**
     * Create a Path from parent string and child string.
     *
     * @param parent the parent path string
     * @param child the child path string
     */
    public Path(String parent, String child) {
        this.nioPath = java.nio.file.Path.of(parent).resolve(child);
    }

    /**
     * Get the underlying Java NIO Path.
     *
     * @return the NIO path
     */
    public java.nio.file.Path toNioPath() {
        return nioPath;
    }

    /**
     * Convert to a URI.
     *
     * @return the URI representation
     */
    public URI toUri() {
        return nioPath.toUri();
    }

    /**
     * Get the file name (last component of the path).
     *
     * @return the file name
     */
    public String getName() {
        java.nio.file.Path fileName = nioPath.getFileName();
        return fileName != null ? fileName.toString() : "";
    }

    /**
     * Get the parent path.
     *
     * @return the parent path, or null if no parent
     */
    public Path getParent() {
        java.nio.file.Path parent = nioPath.getParent();
        return parent != null ? new Path(parent) : null;
    }

    /**
     * Check if this is an absolute path.
     *
     * @return true if absolute
     */
    public boolean isAbsolute() {
        return nioPath.isAbsolute();
    }

    @Override
    public String toString() {
        return nioPath.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof Path))
            return false;
        return nioPath.equals(((Path) o).nioPath);
    }

    @Override
    public int hashCode() {
        return nioPath.hashCode();
    }
}
