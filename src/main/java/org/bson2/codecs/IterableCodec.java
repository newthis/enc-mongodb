/*
 * Copyright 2016 MongoDB, Inc.
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

package org.bson2.codecs;

import org.bson2.BsonBinarySubType;
import org.bson2.BsonReader;
import org.bson2.BsonType;
import org.bson2.BsonWriter;
import org.bson2.Transformer;
import org.bson2.codecs.configuration.CodecRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.bson2.assertions.Assertions.notNull;

/**
 * Encodes and decodes {@code Iterable} objects.
 *
 * @since 3.3
 */
@SuppressWarnings("rawtypes")
public class IterableCodec implements Codec<Iterable> {

    private final CodecRegistry registry;
    private final BsonTypeCodecMap bsonTypeCodecMap;
    private final Transformer valueTransformer;

    /**
     * Construct a new instance with the given {@code CodecRegistry} and {@code BsonTypeClassMap}.
     *
     * @param registry the non-null codec registry
     * @param bsonTypeClassMap the non-null BsonTypeClassMap
     */
    public IterableCodec(final CodecRegistry registry, final BsonTypeClassMap bsonTypeClassMap) {
        this(registry, bsonTypeClassMap, null);
    }

    /**
     * Construct a new instance with the given {@code CodecRegistry} and {@code BsonTypeClassMap}.
     *
     * @param registry the non-null codec registry
     * @param bsonTypeClassMap the non-null BsonTypeClassMap
     * @param valueTransformer the value Transformer
     */
    public IterableCodec(final CodecRegistry registry, final BsonTypeClassMap bsonTypeClassMap, final Transformer valueTransformer) {
        this.registry = notNull("registry", registry);
        this.bsonTypeCodecMap = new BsonTypeCodecMap(notNull("bsonTypeClassMap", bsonTypeClassMap), registry);
        this.valueTransformer = valueTransformer != null ? valueTransformer : new Transformer() {
            @Override
            public Object transform(final Object objectToTransform) {
                return objectToTransform;
            }
        };
    }

    @Override
    public Iterable decode(final BsonReader reader, final DecoderContext decoderContext) {
        reader.readStartArray();

        List<Object> list = new ArrayList<Object>();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            list.add(readValue(reader, decoderContext));
        }

        reader.readEndArray();

        return list;
    }

    @Override
    public void encode(final BsonWriter writer, final Iterable value, final EncoderContext encoderContext) {
        writer.writeStartArray();
        for (final Object cur : value) {
            writeValue(writer, encoderContext, cur);
        }
        writer.writeEndArray();
    }

    @Override
    public Class<Iterable> getEncoderClass() {
        return Iterable.class;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void writeValue(final BsonWriter writer, final EncoderContext encoderContext, final Object value) {
        if (value == null) {
            writer.writeNull();
        } else {
            Codec codec = registry.get(value.getClass());
            encoderContext.encodeWithChildContext(codec, writer, value);
        }
    }

    private Object readValue(final BsonReader reader, final DecoderContext decoderContext) {
        BsonType bsonType = reader.getCurrentBsonType();
        if (bsonType == BsonType.NULL) {
            reader.readNull();
            return null;
        } else if (bsonType == BsonType.BINARY && BsonBinarySubType.isUuid(reader.peekBinarySubType()) && reader.peekBinarySize() == 16) {
            return registry.get(UUID.class).decode(reader, decoderContext);
        }
        return valueTransformer.transform(bsonTypeCodecMap.get(bsonType).decode(reader, decoderContext));
    }
}
