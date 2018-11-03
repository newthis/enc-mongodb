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

import org.bson2.BsonArray;
import org.bson2.BsonDocument;
import org.bson2.BsonValue;
import org.bson2.codecs.BsonArrayCodec;
import org.bson2.codecs.BsonBinaryCodec;
import org.bson2.codecs.BsonBooleanCodec;
import org.bson2.codecs.BsonDBPointerCodec;
import org.bson2.codecs.BsonDateTimeCodec;
import org.bson2.codecs.BsonDecimal128Codec;
import org.bson2.codecs.BsonDocumentCodec;
import org.bson2.codecs.BsonDoubleCodec;
import org.bson2.codecs.BsonInt32Codec;
import org.bson2.codecs.BsonInt64Codec;
import org.bson2.codecs.BsonJavaScriptCodec;
import org.bson2.codecs.BsonJavaScriptWithScopeCodec;
import org.bson2.codecs.BsonMaxKeyCodec;
import org.bson2.codecs.BsonMinKeyCodec;
import org.bson2.codecs.BsonNullCodec;
import org.bson2.codecs.BsonObjectIdCodec;
import org.bson2.codecs.BsonRegularExpressionCodec;
import org.bson2.codecs.BsonStringCodec;
import org.bson2.codecs.BsonSymbolCodec;
import org.bson2.codecs.BsonTimestampCodec;
import org.bson2.codecs.BsonUndefinedCodec;
import org.bson2.codecs.Codec;
import org.bson2.codecs.Decoder;
import org.bson2.codecs.configuration.CodecProvider;
import org.bson2.codecs.configuration.CodecRegistry;

import java.util.HashMap;
import java.util.Map;

class CommandResultCodecProvider<P> implements CodecProvider {
    private final Map<Class<?>, Codec<?>> codecs = new HashMap<Class<?>, Codec<?>>();
    private final Decoder<P> payloadDecoder;
    private final String fieldContainingPayload;

    /**
     * Construct a new instance. with the default codec for each BSON type.
     *
     * @param payloadDecoder the specific decoder to use on the field.
     * @param fieldContainingPayload the field name to be decoded with the payloadDecoder.
     */
    public CommandResultCodecProvider(final Decoder<P> payloadDecoder, final String fieldContainingPayload) {
        this.payloadDecoder = payloadDecoder;
        this.fieldContainingPayload = fieldContainingPayload;
        addCodecs();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        if (codecs.containsKey(clazz)) {
            return (Codec<T>) codecs.get(clazz);
        }

        if (clazz == BsonArray.class) {
            return (Codec<T>) new BsonArrayCodec(registry);
        }

        if (clazz == BsonDocument.class) {
            return (Codec<T>) new CommandResultDocumentCodec<P>(registry, payloadDecoder, fieldContainingPayload);
        }

        return null;
    }

    private void addCodecs() {
        addCodec(new BsonNullCodec());
        addCodec(new BsonBinaryCodec());
        addCodec(new BsonBooleanCodec());
        addCodec(new BsonDateTimeCodec());
        addCodec(new BsonDBPointerCodec());
        addCodec(new BsonDoubleCodec());
        addCodec(new BsonInt32Codec());
        addCodec(new BsonInt64Codec());
        addCodec(new BsonDecimal128Codec());
        addCodec(new BsonMinKeyCodec());
        addCodec(new BsonMaxKeyCodec());
        addCodec(new BsonJavaScriptCodec());
        addCodec(new BsonObjectIdCodec());
        addCodec(new BsonRegularExpressionCodec());
        addCodec(new BsonStringCodec());
        addCodec(new BsonSymbolCodec());
        addCodec(new BsonTimestampCodec());
        addCodec(new BsonUndefinedCodec());
        addCodec(new BsonJavaScriptWithScopeCodec(new BsonDocumentCodec()));
    }

    private <T extends BsonValue> void addCodec(final Codec<T> codec) {
        codecs.put(codec.getEncoderClass(), codec);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CommandResultCodecProvider<?> that = (CommandResultCodecProvider) o;

        if (!fieldContainingPayload.equals(that.fieldContainingPayload)) {
            return false;
        }
        if (!payloadDecoder.getClass().equals(that.payloadDecoder.getClass())) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = payloadDecoder.getClass().hashCode();
        result = 31 * result + fieldContainingPayload.hashCode();
        return result;
    }
}
