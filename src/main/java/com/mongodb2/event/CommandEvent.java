/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb2.event;

import com.mongodb2.connection.ConnectionDescription;

/**
 * An event representing a MongoDB database command.
 *
 * @since 3.1
 */
public abstract class CommandEvent {
    private final int requestId;
    private final ConnectionDescription connectionDescription;
    private final String commandName;

    /**
     * Construct an instance.
     * @param requestId the request id
     * @param connectionDescription the connection description
     * @param commandName the command name
     */
    public CommandEvent(final int requestId, final ConnectionDescription connectionDescription,
                        final String commandName) {
        this.requestId = requestId;
        this.connectionDescription = connectionDescription;
        this.commandName = commandName;
    }

    /**
     * Gets the request identifier
     *
     * @return the request identifier
     */
    public int getRequestId() {
        return requestId;
    }

    /**
     * Gets the description of the connection to which the operation will be sent.
     *
     * @return the connection description
     */
    public ConnectionDescription getConnectionDescription() {
        return connectionDescription;
    }

    /**
     * Gets the name of the command.
     *
     * @return the command name
     */
    public String getCommandName() {
        return commandName;
    }
}


