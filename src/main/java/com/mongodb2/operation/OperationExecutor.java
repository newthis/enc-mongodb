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

package com.mongodb2.operation;

import com.mongodb2.ReadPreference;

/**
 * An interface describing the execution of a read or a write operation.
 *
 * @since 3.0
 */
public interface OperationExecutor {
    /**
     * Execute the read operation with the given read preference.
     *
     * @param operation the read operation.
     * @param readPreference the read preference.
     * @param <T> the operations result type.
     * @return the result of executing the operation.
     */
    <T> T execute(ReadOperation<T> operation, ReadPreference readPreference);

    /**
     * Execute the write operation.
     *
     * @param operation the write operation.
     * @param <T> the operations result type.
     * @return the result of executing the operation.
     */
    <T> T execute(WriteOperation<T> operation);
}
