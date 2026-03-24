/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.hadoop.fs;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Set;

/// Minimal Hadoop Path shim that wraps a URI.
///
/// This class provides API compatibility with Hadoop's Path class without
/// requiring the Hadoop dependency. Local paths are backed by a `file://`
/// URI; remote paths (e.g. `s3a://`) store the URI as-is.
public class Path {

    private static final Set<String> LOCAL_SCHEMES = Set.of("file", "");

    private final URI uri;

    /// Create a Path from a string path.
    ///
    /// @param pathString the path string
    public Path(String pathString) {
        this.uri = parseUri(pathString);
    }

    /// Create a Path from a URI.
    ///
    /// @param uri the URI
    public Path(URI uri) {
        this.uri = uri;
    }

    /// Create a Path by resolving a child path against a parent.
    ///
    /// @param parent the parent path
    /// @param child the child path string
    public Path(Path parent, String child) {
        if (isLocal(parent.uri)) {
            java.nio.file.Path resolved = java.nio.file.Path.of(parent.uri).resolve(child);
            this.uri = resolved.toUri();
        }
        else {
            String parentPath = parent.uri.getPath();
            if (!parentPath.endsWith("/")) {
                parentPath = parentPath + "/";
            }
            this.uri = parent.uri.resolve(parentPath + child);
        }
    }

    /// Create a Path from parent string and child string.
    ///
    /// @param parent the parent path string
    /// @param child the child path string
    public Path(String parent, String child) {
        this(new Path(parent), child);
    }

    /// Convert to a URI.
    ///
    /// @return the URI representation
    public URI toUri() {
        return uri;
    }

    /// Get the file name (last component of the path).
    ///
    /// @return the file name
    public String getName() {
        String path = uri.getPath();
        if (path == null || path.isEmpty()) {
            return "";
        }
        int lastSlash = path.lastIndexOf('/');
        return lastSlash >= 0 ? path.substring(lastSlash + 1) : path;
    }

    /// Get the parent path.
    ///
    /// @return the parent path, or null if no parent
    public Path getParent() {
        if (isLocal(uri)) {
            java.nio.file.Path parent = java.nio.file.Path.of(uri).getParent();
            return parent != null ? new Path(parent.toUri()) : null;
        }
        String path = uri.getPath();
        if (path == null || path.equals("/") || path.isEmpty()) {
            return null;
        }
        int lastSlash = path.lastIndexOf('/');
        if (lastSlash <= 0) {
            return null;
        }
        try {
            return new Path(new URI(uri.getScheme(), uri.getAuthority(),
                    path.substring(0, lastSlash), null, null));
        }
        catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid parent path for: " + uri, e);
        }
    }

    /// Check if this is an absolute path.
    ///
    /// @return true if absolute
    public boolean isAbsolute() {
        if (isLocal(uri)) {
            return java.nio.file.Path.of(uri).isAbsolute();
        }
        return uri.getScheme() != null;
    }

    @Override
    public String toString() {
        if (isLocal(uri)) {
            return java.nio.file.Path.of(uri).toString();
        }
        return uri.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof Path other))
            return false;
        return uri.equals(other.uri);
    }

    @Override
    public int hashCode() {
        return uri.hashCode();
    }

    private static URI parseUri(String pathString) {
        // Try parsing as a URI first (handles s3a://, file://, etc.)
        try {
            URI parsed = new URI(pathString);
            if (parsed.getScheme() != null) {
                return parsed;
            }
        }
        catch (URISyntaxException e) {
            // Fall through to local path handling
        }
        // Treat as local path
        return java.nio.file.Path.of(pathString).toUri();
    }

    private static boolean isLocal(URI uri) {
        String scheme = uri.getScheme();
        return scheme == null || LOCAL_SCHEMES.contains(scheme);
    }
}
