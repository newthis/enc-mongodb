/*
 * Copyright 2008-2016 MongoDB, Inc.
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

/**
 * An adapter for cluster listener implementations, for clients that want to listen for a subset of cluster events.  Extend this class to
 * listen for cluster events and override the methods of interest.
 *
 * @since 3.3
 */
public abstract class ClusterListenerAdapter implements ClusterListener {
    @Override
    public void clusterOpening(final ClusterOpeningEvent event) {
    }

    @Override
    public void clusterClosed(final ClusterClosedEvent event) {
    }

    @Override
    public void clusterDescriptionChanged(final ClusterDescriptionChangedEvent event) {
    }
}
