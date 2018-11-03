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

package org.bson2.codecs.configuration;

import org.bson2.BsonReader;
import org.bson2.BsonWriter;
import org.bson2.codecs.Codec;
import org.bson2.codecs.DecoderContext;
import org.bson2.codecs.EncoderContext;

class LazyCodec<T> implements Codec<T> {
    private final CodecRegistry registry;
    private final Class<T> clazz;
    private volatile Codec<T> wrapped;

    public LazyCodec(final CodecRegistry registry, final Class<T> clazz) {
        this.registry = registry;
        this.clazz = clazz;
    }

    @Override
    public void encode(final BsonWriter writer, final T value, final EncoderContext encoderContext) {
        getWrapped().encode(writer, value, encoderContext);
    }

    @Override
    public Class<T> getEncoderClass() {
        return clazz;
    }

    @Override
    public T decode(final BsonReader reader, final DecoderContext decoderContext) {
        return getWrapped().decode(reader, decoderContext);
    }

    private Codec<T> getWrapped() {
        if (wrapped == null) {
            wrapped = registry.get(clazz);
        }

        return wrapped;
    }
}
