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

import org.bson2.BsonDocumentWrapper;
import org.bson2.codecs.Encoder;

class InsertRequest extends WriteRequest {
    private final DBObject document;
    private final Encoder<DBObject> codec;

    public InsertRequest(final DBObject document, final Encoder<DBObject> codec) {
        this.document = document;
        this.codec = codec;
    }

    public DBObject getDocument() {
        return document;
    }

    @Override
    com.mongodb2.bulk.WriteRequest toNew() {
        return new com.mongodb2.bulk.InsertRequest(new BsonDocumentWrapper<DBObject>(document, codec));
    }
}
