/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.hadoop.conf;

import java.util.HashMap;
import java.util.Map;

/**
 * Minimal Hadoop Configuration shim.
 * <p>
 * Provides get/set for properties to maintain API compatibility with Hadoop's
 * Configuration class. Note that configuration values do not affect Hardwood's
 * behavior - this is purely for API compatibility.
 * </p>
 */
public class Configuration {

    private final Map<String, String> properties = new HashMap<>();

    /**
     * Create an empty configuration.
     */
    public Configuration() {
    }

    /**
     * Create a configuration.
     *
     * @param loadDefaults ignored (no Hadoop defaults to load)
     */
    public Configuration(boolean loadDefaults) {
        // No-op: we don't load Hadoop defaults
    }

    /**
     * Copy constructor.
     *
     * @param other the configuration to copy
     */
    public Configuration(Configuration other) {
        this.properties.putAll(other.properties);
    }

    /**
     * Set a property value.
     *
     * @param name the property name
     * @param value the property value
     */
    public void set(String name, String value) {
        properties.put(name, value);
    }

    /**
     * Get a property value.
     *
     * @param name the property name
     * @return the value, or null if not set
     */
    public String get(String name) {
        return properties.get(name);
    }

    /**
     * Get a property value with a default.
     *
     * @param name the property name
     * @param defaultValue the default value
     * @return the value, or the default if not set
     */
    public String get(String name, String defaultValue) {
        return properties.getOrDefault(name, defaultValue);
    }

    /**
     * Get an integer property value.
     *
     * @param name the property name
     * @param defaultValue the default value
     * @return the value, or the default if not set
     */
    public int getInt(String name, int defaultValue) {
        String value = properties.get(name);
        return value != null ? Integer.parseInt(value) : defaultValue;
    }

    /**
     * Get a long property value.
     *
     * @param name the property name
     * @param defaultValue the default value
     * @return the value, or the default if not set
     */
    public long getLong(String name, long defaultValue) {
        String value = properties.get(name);
        return value != null ? Long.parseLong(value) : defaultValue;
    }

    /**
     * Get a boolean property value.
     *
     * @param name the property name
     * @param defaultValue the default value
     * @return the value, or the default if not set
     */
    public boolean getBoolean(String name, boolean defaultValue) {
        String value = properties.get(name);
        return value != null ? Boolean.parseBoolean(value) : defaultValue;
    }

    /**
     * Set an integer property value.
     *
     * @param name the property name
     * @param value the property value
     */
    public void setInt(String name, int value) {
        properties.put(name, Integer.toString(value));
    }

    /**
     * Set a long property value.
     *
     * @param name the property name
     * @param value the property value
     */
    public void setLong(String name, long value) {
        properties.put(name, Long.toString(value));
    }

    /**
     * Set a boolean property value.
     *
     * @param name the property name
     * @param value the property value
     */
    public void setBoolean(String name, boolean value) {
        properties.put(name, Boolean.toString(value));
    }

    /**
     * Clear all properties.
     */
    public void clear() {
        properties.clear();
    }

    /**
     * Get the number of properties.
     *
     * @return the property count
     */
    public int size() {
        return properties.size();
    }
}
