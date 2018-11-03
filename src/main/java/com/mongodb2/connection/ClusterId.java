/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb2.connection;

import org.bson2.types.ObjectId;

import static com.mongodb2.assertions.Assertions.notNull;

/**
 * A client-generated identifier that uniquely identifies a connection to a MongoDB cluster, which could be sharded, replica set,
 * or standalone.
 *
 * @since 3.0
 */
public final class ClusterId {
    private final String value;
    private final String description;

    /**
     * Construct an instance.
     *
     */
    public ClusterId() {
        this(null);
    }

    /**
     * Construct an instance.
     *
     * @param description the user defined description of the MongoClient
     */
    public ClusterId(final String description) {
        this.value = new ObjectId().toHexString();
        this.description = description;
    }

    // for testing only, as cluster identifiers should really be unique
    ClusterId(final String value, final String description) {
        this.value = notNull("value", value);
        this.description = description;
    }

    /**
     * Gets the value of the identifier.
     *
     * @return the value
     */
    public String getValue() {
        return value;
    }

    /**
     * Gets the user defined description of the MongoClient.
     *
     * @return the user defined description of the MongoClient
     */
    public String getDescription() {
        return description;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ClusterId clusterId = (ClusterId) o;

        if (!value.equals(clusterId.value)) {
            return false;
        }
        if (description != null ? !description.equals(clusterId.description) : clusterId.description != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = value.hashCode();
        result = 31 * result + (description != null ? description.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ClusterId{"
               + "value='" + value + '\''
               + ", description='" + description + '\''
               + '}';
    }
}
