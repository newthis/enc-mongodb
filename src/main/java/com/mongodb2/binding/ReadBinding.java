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

/**
 * A factory of connection sources to servers that can be read from and that satisfy the specified read preference.
 *
 * @since 3.0
 */
public interface ReadBinding extends ReferenceCounted {
    /**
     * The read preference that all connection sources returned by this instance will satisfy.
     * @return the non-null read preference
     */
    ReadPreference getReadPreference();

    /**
     * Returns a connection source to a server that satisfies the specified read preference.
     * @return the connection source
     */
    ConnectionSource getReadConnectionSource();

    @Override
    ReadBinding retain();
}
