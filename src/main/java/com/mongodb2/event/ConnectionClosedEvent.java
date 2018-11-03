/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.mongodb2.event;

import com.mongodb2.annotations.Beta;
import com.mongodb2.connection.ConnectionId;

import static org.bson2.assertions.Assertions.notNull;

/**
 * A connection closed event.
 */
@Beta
public final class ConnectionClosedEvent {

    private final ConnectionId connectionId;

    /**
     * Constructs a new instance of the event.
     *
     * @param connectionId the connection id
     */
    public ConnectionClosedEvent(final ConnectionId connectionId) {
        this.connectionId = notNull("connectionId", connectionId);
    }

    /**
     * Gets the identifier for this connection.
     *
     * @return the connection id
     */
    public ConnectionId getConnectionId() {
        return connectionId;
    }

    @Override
    public String toString() {
        return "ConnectionClosedEvent{"
                       + "connectionId=" + connectionId
                       + '}';
    }
}
