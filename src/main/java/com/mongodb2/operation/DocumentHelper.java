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

import org.bson2.BsonBoolean;
import org.bson2.BsonDocument;
import org.bson2.BsonInt32;
import org.bson2.BsonInt64;
import org.bson2.BsonValue;

final class DocumentHelper {

    private DocumentHelper() {
    }

    static void putIfTrue(final BsonDocument command, final String key, final boolean condition) {
        if (condition) {
            command.put(key, BsonBoolean.TRUE);
        }
    }

    static void putIfNotNull(final BsonDocument command, final String key, final BsonValue value) {
        if (value != null) {
            command.put(key, value);
        }
    }

    static void putIfNotZero(final BsonDocument command, final String key, final int value) {
        if (value != 0) {
            command.put(key, new BsonInt32(value));
        }
    }

    static void putIfNotZero(final BsonDocument command, final String key, final long value) {
        if (value != 0) {
            command.put(key, new BsonInt64(value));
        }
    }
}
