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

package com.mongodb2.util;

/**
 * Interface describing methods for serializing an object to a string.
 */
public interface ObjectSerializer {
    /**
     * Serializes {@code obj} into {@code buf}.
     *
     * @param obj object to serialize
     * @param buf buffer to serialize into
     */
    void serialize(Object obj, StringBuilder buf);

    /**
     * Serializes {@code obj}.
     *
     * @param obj object to serialize
     * @return the serialized object
     */
    String serialize(Object obj);
}
