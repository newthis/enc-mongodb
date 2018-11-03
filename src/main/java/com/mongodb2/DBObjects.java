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

import org.bson2.BsonDocument;
import org.bson2.BsonDocumentReader;
import org.bson2.codecs.DecoderContext;

final class DBObjects {
    public static DBObject toDBObject(final BsonDocument document) {
        return MongoClient.getDefaultCodecRegistry().get(DBObject.class).decode(new BsonDocumentReader(document),
                                                                                DecoderContext.builder().build());
    }

    private DBObjects() {
    }
}
