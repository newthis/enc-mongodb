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

import com.mongodb2.annotations.Immutable;

import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb2.assertions.Assertions.isTrue;
import static com.mongodb2.assertions.Assertions.notNull;
import static java.lang.String.format;

/**
 * An immutable connection identifier of a connection to a MongoDB server.
 *
 * <p>Contains a locally created id and if available the MongoDB server created connection id</p>
 *
 * @since 3.0
 */
@Immutable
public final class ConnectionId {
    private static final AtomicInteger INCREMENTING_ID = new AtomicInteger();

    private final ServerId serverId;
    private final int localValue;
    private final Integer serverValue;
    private final String stringValue;

    /**
     * Construct an instance with the given server id.
     *
     * @param serverId the server id
     */
    ConnectionId(final ServerId serverId) {
        this(serverId, INCREMENTING_ID.incrementAndGet(), null);
    }

    private ConnectionId(final ServerId serverId, final int localValue, final Integer serverValue) {
        this.serverId = notNull("serverId", serverId);
        this.localValue = localValue;
        this.serverValue = serverValue;
        if (serverValue == null) {
            stringValue = format("connectionId{localValue:%s}", localValue);
        } else {
            stringValue = format("connectionId{localValue:%s, serverValue:%s}", localValue, serverValue);
        }
    }

    ConnectionId withServerValue(final int serverValue) {
        isTrue("server value is null", this.serverValue == null);
        return new ConnectionId(serverId, localValue, serverValue);
    }

    /**
     * Gets the server id.
     *
     * @return the server id
     */
    public ServerId getServerId() {
        return serverId;
    }

    /**
     * Gets the locally created id value for the connection
     *
     * @return the locally created id value for the connection
     */
    public int getLocalValue() {
        return localValue;
    }

    /**
     * Gets the server generated id value for the connection or null if not set.
     *
     * @return the server generated id value for the connection or null if not set.
     */
    public Integer getServerValue() {
        return serverValue;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ConnectionId that = (ConnectionId) o;

        if (localValue != that.localValue) {
            return false;
        }
        if (!serverId.equals(that.serverId)) {
            return false;
        }
        if (serverValue != null ? !serverValue.equals(that.serverValue) : that.serverValue != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = serverId.hashCode();
        result = 31 * result + localValue;
        result = 31 * result + (serverValue != null ? serverValue.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return stringValue;
    }
}
