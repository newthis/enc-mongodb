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

import com.mongodb2.MongoCredential;
import com.mongodb2.event.ClusterListener;
import com.mongodb2.event.ConnectionListener;
import com.mongodb2.event.ConnectionPoolListener;

import java.util.List;

/**
 * Factory for {@code Cluster} implementations.
 *
 * @since 3.0
 */
public interface ClusterFactory {

    // CHECKSTYLE:OFF

    /**
     * Creates a cluster with the given settings.  The cluster mode will be based on the mode from the settings.
     *
     *
     * @param settings                 the cluster settings
     * @param serverSettings           the server settings
     * @param connectionPoolSettings   the connection pool settings
     * @param streamFactory            the stream factory
     * @param heartbeatStreamFactory   the heartbeat stream factory
     * @param credentialList           the credential list
     * @param clusterListener          an optional listener for cluster-related events
     * @param connectionPoolListener   an optional listener for connection pool-related events
     * @param connectionListener       an optional listener for connection-related events
     * @return the cluster
     */
    Cluster create(ClusterSettings settings,
                   ServerSettings serverSettings,
                   ConnectionPoolSettings connectionPoolSettings,
                   StreamFactory streamFactory,
                   StreamFactory heartbeatStreamFactory,
                   List<MongoCredential> credentialList,
                   ClusterListener clusterListener,
                   ConnectionPoolListener connectionPoolListener,
                   ConnectionListener connectionListener);

    // CHECKSTYLE:ON
}
