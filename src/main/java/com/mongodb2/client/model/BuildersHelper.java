/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb2.client.model;

import org.bson2.BsonDocument;
import org.bson2.BsonDocumentWriter;
import org.bson2.codecs.Encoder;
import org.bson2.codecs.EncoderContext;
import org.bson2.codecs.configuration.CodecRegistry;
import org.bson2.conversions.Bson;

final class BuildersHelper {

    @SuppressWarnings("unchecked")
    static <TItem> void encodeValue(final BsonDocumentWriter writer, final TItem value, final CodecRegistry codecRegistry) {
        if (value == null) {
            writer.writeNull();
        } else if (value instanceof Bson) {
            ((Encoder) codecRegistry.get(BsonDocument.class)).encode(writer,
                                                                     ((Bson) value).toBsonDocument(BsonDocument.class, codecRegistry),
                                                                     EncoderContext.builder().build());
        } else {
            ((Encoder) codecRegistry.get(value.getClass())).encode(writer, value, EncoderContext.builder().build());
        }
    }

    private BuildersHelper() {
    }
}
