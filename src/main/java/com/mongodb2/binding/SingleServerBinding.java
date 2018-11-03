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

package com.mongodb2.binding;

import com.mongodb2.ReadPreference;
import com.mongodb2.ServerAddress;
import com.mongodb2.connection.Cluster;
import com.mongodb2.connection.Connection;
import com.mongodb2.connection.Server;
import com.mongodb2.connection.ServerDescription;
import com.mongodb2.selector.ServerAddressSelector;

import static com.mongodb2.assertions.Assertions.notNull;

/**
 * A simple binding where all connection sources are bound to the server specified in the constructor.
 *
 * @since 3.0
 */
public class SingleServerBinding extends AbstractReferenceCounted implements ReadWriteBinding {
    private final Cluster cluster;
    private final ServerAddress serverAddress;
    private final ReadPreference readPreference;

    /**
     * Creates an instance, defaulting to {@link com.mongodb2.ReadPreference#primary()} for reads.
     * @param cluster       a non-null  Cluster which will be used to select a server to bind to
     * @param serverAddress a non-null  address of the server to bind to
     */
    public SingleServerBinding(final Cluster cluster, final ServerAddress serverAddress) {
        this(cluster, serverAddress, ReadPreference.primary());
    }

    /**
     * Creates an instance.
     * @param cluster        a non-null  Cluster which will be used to select a server to bind to
     * @param serverAddress  a non-null  address of the server to bind to
     * @param readPreference a non-null  ReadPreference for read operations
     */
    public SingleServerBinding(final Cluster cluster, final ServerAddress serverAddress, final ReadPreference readPreference) {
        this.cluster = notNull("cluster", cluster);
        this.serverAddress = notNull("serverAddress", serverAddress);
        this.readPreference = notNull("readPreference", readPreference);
    }

    @Override
    public ConnectionSource getWriteConnectionSource() {
        return new SingleServerBindingConnectionSource();
    }

    @Override
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    @Override
    public ConnectionSource getReadConnectionSource() {
        return new SingleServerBindingConnectionSource();
    }

    @Override
    public SingleServerBinding retain() {
        super.retain();
        return this;
    }

    private final class SingleServerBindingConnectionSource extends AbstractReferenceCounted implements ConnectionSource {
        private final Server server;

        private SingleServerBindingConnectionSource() {
            SingleServerBinding.this.retain();
            server = cluster.selectServer(new ServerAddressSelector(serverAddress));
        }

        @Override
        public ServerDescription getServerDescription() {
            return server.getDescription();
        }

        @Override
        public Connection getConnection() {
            return cluster.selectServer(new ServerAddressSelector(serverAddress)).getConnection();
        }

        @Override
        public ConnectionSource retain() {
            super.retain();
            SingleServerBinding.this.retain();
            return this;
        }

        @Override
        public void release() {
            SingleServerBinding.this.release();
        }
    }
}

