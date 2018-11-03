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

import com.mongodb2.ServerAddress;
import com.mongodb2.internal.connection.PowerOfTwoBufferPool;

import static com.mongodb2.assertions.Assertions.notNull;

/**
 * Factory to create a Stream that's an AsynchronousSocketChannelStream. Throws an exception if SSL is enabled.
 *
 * @since 3.0
 */
public class AsynchronousSocketChannelStreamFactory implements StreamFactory {
    private final SocketSettings settings;
    private final BufferProvider bufferProvider = new PowerOfTwoBufferPool();

    /**
     * Create a new factory.
     *
     * @param settings    the settings for the connection to a MongoDB server
     * @param sslSettings the settings for connecting via SSL
     */
    public AsynchronousSocketChannelStreamFactory(final SocketSettings settings, final SslSettings sslSettings) {
        if (sslSettings.isEnabled()) {
            throw new UnsupportedOperationException("No SSL support in java.nio.channels.AsynchronousSocketChannel. For SSL support use "
                                                    + "com.mongodb.connection.netty.NettyStreamFactoryFactory");
        }

        this.settings = notNull("settings", settings);
    }

    @Override
    public Stream create(final ServerAddress serverAddress) {
        return new AsynchronousSocketChannelStream(serverAddress, settings, bufferProvider);
    }

}
