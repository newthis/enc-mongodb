/*
 * Copyright 2016 MongoDB, Inc.
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
 *
 */

package com.mongodb2.event;

import com.mongodb2.connection.ServerId;

import static com.mongodb2.assertions.Assertions.notNull;

/**
 * A server opening event.
 *
 * @since 3.3
 */
public final class ServerOpeningEvent {
    private final ServerId serverId;

    /**
     * Construct an instance.
     *
     * @param serverId the non-null serverId
     */
    public ServerOpeningEvent(final ServerId serverId) {
        this.serverId = notNull("serverId", serverId);
    }

    /**
     * Gets the serverId.
     *
     * @return the serverId
     */
    public ServerId getServerId() {
        return serverId;
    }

    @Override
    public String toString() {
        return "ServerOpeningEvent{"
                       + "serverId=" + serverId
                       + '}';
    }
}
