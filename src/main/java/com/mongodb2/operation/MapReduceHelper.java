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

import org.bson2.BsonDocument;

final class MapReduceHelper {

    static MapReduceStatistics createStatistics(final BsonDocument result) {
        return new MapReduceStatistics(getInputCount(result), getOutputCount(result), getEmitCount(result),
                                       getDuration(result));
    }

    private static int getInputCount(final BsonDocument result) {
        return result.getDocument("counts").getNumber("input").intValue();
    }

    private static int getOutputCount(final BsonDocument result) {
        return result.getDocument("counts").getNumber("output").intValue();
    }

    private static int getEmitCount(final BsonDocument result) {
        return result.getDocument("counts").getNumber("emit").intValue();
    }

    private static int getDuration(final BsonDocument result) {
        return result.getNumber("timeMillis").intValue();
    }

    private MapReduceHelper() {
    }
}
