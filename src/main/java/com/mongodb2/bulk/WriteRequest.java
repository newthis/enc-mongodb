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

package com.mongodb2.bulk;

/**
 * An abstract base class for a write request.
 *
 * @since 3.0
 */
public abstract class WriteRequest {

    /**
     * The type of write.
     */
    public enum Type {
        /**
         * An insert.
         */
        INSERT,

        /**
         * An update that uses update operators.
         */
        UPDATE,

        /**
         * An update that replaces the existing document.
         */
        REPLACE,

        /**
         * A delete.
         */
        DELETE
    }

    WriteRequest() {
    }

    /**
     * Gets the type of the write.
     *
     * @return the type
     */
    public abstract Type getType();
}
