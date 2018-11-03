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
 */

package com.mongodb2.event;

import com.mongodb2.connection.ConnectionId;
import org.bson2.BsonDocument;

import java.util.concurrent.TimeUnit;

import static com.mongodb2.assertions.Assertions.isTrueArgument;
import static com.mongodb2.assertions.Assertions.notNull;

/**
 * An event for successful completion of a server heartbeat.
 *
 * @since 3.3
 */
public final class ServerHeartbeatSucceededEvent {
    private final ConnectionId connectionId;
    private final BsonDocument reply;
    private final long elapsedTimeNanos;

    /**
     * Construct an instance.
     *
     * @param connectionId the non-null connectionId
     * @param reply the non-null reply to an isMaster command
     * @param elapsedTimeNanos the non-negative elapsed time in nanoseconds
     */
    public ServerHeartbeatSucceededEvent(final ConnectionId connectionId, final BsonDocument reply, final long elapsedTimeNanos) {
        this.connectionId = notNull("connectionId", connectionId);
        this.reply = notNull("reply", reply);
        isTrueArgument("elapsed time is not negative", elapsedTimeNanos >= 0);
        this.elapsedTimeNanos = elapsedTimeNanos;
    }

    /**
     * Gets the connectionId.
     *
     * @return the connectionId
     */
    public ConnectionId getConnectionId() {
        return connectionId;
    }

    /**
     * Gets the reply to the isMaster command executed for this heartbeat.
     *
     * @return the reply
     */
    public BsonDocument getReply() {
        return reply;
    }

    /**
     * Gets the elapsed time in the given time unit.
     *
     * @param timeUnit the non-null timeUnit
     *
     * @return the elapsed time in the given time unit
     */
    public long getElapsedTime(final TimeUnit timeUnit) {
        return timeUnit.convert(elapsedTimeNanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public String toString() {
        return "ServerHeartbeatSucceededEvent{"
                + "connectionId=" + connectionId
                + ", reply=" + reply
                + ", elapsedTimeNanos=" + elapsedTimeNanos
                + "} ";
    }
}
