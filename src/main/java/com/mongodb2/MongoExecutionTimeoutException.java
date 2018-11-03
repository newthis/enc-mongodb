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

package com.mongodb2;

/**
 * Exception indicating that the execution of the current operation timed out as a result of the maximum operation time being exceeded.
 *
 * @since 2.12
 */
public class MongoExecutionTimeoutException extends MongoException {
    private static final long serialVersionUID = 5955669123800274594L;

    /**
     * Construct a new instance.
     *
     * @param code the error code
     * @param message the error message
     */
    public MongoExecutionTimeoutException(final int code, final String message) {
        super(code, message);

    }
}
