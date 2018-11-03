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

package org.bson2.codecs;

import org.bson2.Document;
import org.bson2.Transformer;
import org.bson2.codecs.configuration.CodecProvider;
import org.bson2.codecs.configuration.CodecRegistry;
import org.bson2.types.CodeWithScope;

import static org.bson2.assertions.Assertions.notNull;

/**
 * A {@code CodecProvider} for the Document class and all the default Codec implementations on which it depends.
 *
 * @since 3.0
 */
public class DocumentCodecProvider implements CodecProvider {
    private final BsonTypeClassMap bsonTypeClassMap;
    private final Transformer valueTransformer;

    /**
     * Construct a new instance with a default {@code BsonTypeClassMap}.
     */
    public DocumentCodecProvider() {
        this(new BsonTypeClassMap());
    }

    /**
     * Construct a new instance with a default {@code BsonTypeClassMap} and the given {@code Transformer}.  The transformer is used by the
     * DocumentCodec as a last step when decoding values.
     *
     * @param valueTransformer the value transformer for decoded values
     * @see org.bson2.codecs.DocumentCodec#DocumentCodec(org.bson2.codecs.configuration.CodecRegistry, BsonTypeClassMap, org.bson2.Transformer)
     */
    public DocumentCodecProvider(final Transformer valueTransformer) {
        this(new BsonTypeClassMap(), valueTransformer);
    }

    /**
     * Construct a new instance with the given instance of {@code BsonTypeClassMap}.
     *
     * @param bsonTypeClassMap the non-null {@code BsonTypeClassMap} with which to construct instances of {@code DocumentCodec} and {@code
     *                         ListCodec}
     */
    public DocumentCodecProvider(final BsonTypeClassMap bsonTypeClassMap) {
        this(bsonTypeClassMap, null);
    }

    /**
     * Construct a new instance with the given instance of {@code BsonTypeClassMap}.
     *
     * @param bsonTypeClassMap the non-null {@code BsonTypeClassMap} with which to construct instances of {@code DocumentCodec} and {@code
     *                         ListCodec}.
     * @param valueTransformer the value transformer for decoded values
     */
    public DocumentCodecProvider(final BsonTypeClassMap bsonTypeClassMap, final Transformer valueTransformer) {
        this.bsonTypeClassMap = notNull("bsonTypeClassMap", bsonTypeClassMap);
        this.valueTransformer = valueTransformer;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        if (clazz == CodeWithScope.class) {
            return (Codec<T>) new CodeWithScopeCodec(registry.get(Document.class));
        }

        if (clazz == Document.class) {
            return (Codec<T>) new DocumentCodec(registry, bsonTypeClassMap, valueTransformer);
        }

        return null;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DocumentCodecProvider that = (DocumentCodecProvider) o;

        if (!bsonTypeClassMap.equals(that.bsonTypeClassMap)) {
            return false;
        }
        if (valueTransformer != null ? !valueTransformer.equals(that.valueTransformer) : that.valueTransformer != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = bsonTypeClassMap.hashCode();
        result = 31 * result + (valueTransformer != null ? valueTransformer.hashCode() : 0);
        return result;
    }
}
