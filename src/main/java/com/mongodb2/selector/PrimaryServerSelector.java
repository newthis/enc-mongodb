/*
 * Copyright 2008-2015 MongoDB, Inc.
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

package com.mongodb2.selector;

import com.mongodb2.connection.ClusterDescription;
import com.mongodb2.connection.ServerDescription;

import java.util.List;

/**
 * A server selector that chooses servers that are primaries.
 *
 * @since 3.0
 * @deprecated Use either {@link ReadPreferenceServerSelector} or {@link WritableServerSelector}, depending on your requirements
 */
@Deprecated
public final class PrimaryServerSelector implements ServerSelector {

    @Override
    @SuppressWarnings("deprecation")
    public List<ServerDescription> select(final ClusterDescription clusterDescription) {
        return clusterDescription.getPrimaries();
    }

    @Override
    public String toString() {
        return "PrimaryServerSelector";
    }
}
