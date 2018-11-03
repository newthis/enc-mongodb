/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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

import com.mongodb2.async.SingleResultCallback;
import com.mongodb2.event.CommandListener;

/**
 * An interface for the execution of a MongoDB wire protocol conversation
 *
 * @param <T> the return value of the Protocol response message
 */
interface Protocol<T> {
    /**
     * Execute the protocol.
     *
     * @param connection the connection to execute the protocol on
     * @return the response from execution of the protocol
     */
    T execute(final InternalConnection connection);

    /**
     * Execute the protocol asynchronously.
     *
     * @param connection the connection to execute the protocol on
     * @param callback   the callback that is passed the result of the execution
     */
    void executeAsync(final InternalConnection connection, SingleResultCallback<T> callback);

    void setCommandListener(CommandListener commandListener);
}
