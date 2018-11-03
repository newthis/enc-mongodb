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

import com.mongodb2.annotations.ThreadSafe;
import com.mongodb2.async.SingleResultCallback;
import org.bson2.ByteBuf;

import java.util.List;

@ThreadSafe
interface InternalConnection extends BufferProvider {

    /**
     * Gets the description of this connection.
     *
     * @return the connection description
     */
    ConnectionDescription getDescription();

    /**
     * Opens the connection so its ready for use
     */
    void open();

    /**
     * Opens the connection so its ready for use
     *
     * @param callback the callback to be called once the connection has been opened
     */
    void openAsync(SingleResultCallback<Void> callback);

    /**
     * Closes the connection.
     */
    void close();

    /**
     * Returns if the connection has been opened
     *
     * @return true if connection has been opened
     */
    boolean opened();

    /**
     * Returns the closed state of the connection
     *
     * @return true if connection is closed
     */
    boolean isClosed();

    /**
     * Send a message to the server. The connection may not make any attempt to validate the integrity of the message.
     *
     * @param byteBuffers   the list of byte buffers to send.
     * @param lastRequestId the request id of the last message in byteBuffers
     */
    void sendMessage(List<ByteBuf> byteBuffers, int lastRequestId);

    /**
     * Receive a response to a sent message from the server.
     *
     * @param responseTo the request id that this message is a response to
     * @return the response
     */
    ResponseBuffers receiveMessage(int responseTo);

    /**
     * Asynchronously send a message to the server. The connection may not make any attempt to validate the integrity of the message.
     *
     * @param byteBuffers   the list of byte buffers to send
     * @param lastRequestId the request id of the last message in byteBuffers
     * @param callback      the callback to invoke on completion
     */
    void sendMessageAsync(List<ByteBuf> byteBuffers, int lastRequestId, SingleResultCallback<Void> callback);

    /**
     * Asynchronously receive a response to a sent message from the server.
     *
     * @param responseTo the request id that this message is a response to
     * @param callback the callback to invoke on completion
     */
    void receiveMessageAsync(int responseTo, SingleResultCallback<ResponseBuffers> callback);

}
