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

/**
 * Represents the results of a map-reduce operation as a cursor.  Users can iterate over the results and additionally get relevant
 * statistics about the operation.
 *
 * @param <T> the operations result type.
 * @since 3.0
 */
public interface MapReduceBatchCursor<T> extends BatchCursor<T> {
    /**
     * Get the statistics for this map-reduce operation
     *
     * @return the statistics
     */
    MapReduceStatistics getStatistics();
}
