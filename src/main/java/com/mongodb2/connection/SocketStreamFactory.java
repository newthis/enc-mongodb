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

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;

import static com.mongodb2.assertions.Assertions.notNull;

/**
 * Factory for creating instances of {@code SocketStream}.
 *
 * @since 3.0
 */
public class SocketStreamFactory implements StreamFactory {
    private final SocketSettings settings;
    private final SslSettings sslSettings;
    private final SocketFactory socketFactory;
    private final BufferProvider bufferProvider = new PowerOfTwoBufferPool();

    /**
     * Creates a new factory with the given settings for connecting to servers and the given SSL settings
     *
     * @param settings    the SocketSettings for connecting to a MongoDB server
     * @param sslSettings whether SSL is enabled.
     */
    public SocketStreamFactory(final SocketSettings settings, final SslSettings sslSettings) {
        this(settings, sslSettings, null);
    }

    /**
     * Creates a new factory with the given settings for connecting to servers and a factory for creating connections.
     *
     * @param settings      the SocketSettings for connecting to a MongoDB server
     * @param sslSettings   the SSL for connecting to a MongoDB server
     * @param socketFactory a SocketFactory for creating connections to servers.
     */
    public SocketStreamFactory(final SocketSettings settings, final SslSettings sslSettings, final SocketFactory socketFactory) {
        this.settings = notNull("settings", settings);
        this.sslSettings = notNull("sslSettings", sslSettings);
        this.socketFactory = socketFactory;
    }

    @Override
    public Stream create(final ServerAddress serverAddress) {
        Stream stream;
        if (socketFactory != null) {
            stream = new SocketStream(serverAddress, settings, sslSettings, socketFactory, bufferProvider);
        } else if (sslSettings.isEnabled()) {
            stream = new SocketStream(serverAddress, settings, sslSettings, SSLSocketFactory.getDefault(), bufferProvider);
        } else if (System.getProperty("org.mongodb.useSocket", "false").equals("true")) {
            stream = new SocketStream(serverAddress, settings, sslSettings, SocketFactory.getDefault(), bufferProvider);
        } else {
            stream = new SocketChannelStream(serverAddress, settings, sslSettings, bufferProvider);
        }

        return stream;
    }

}
