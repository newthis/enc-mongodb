/*
 * Copyright 2008-2016 MongoDB, Inc.
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

import org.bson2.codecs.configuration.CodecProvider;
import org.bson2.codecs.configuration.CodecRegistry;

import java.util.HashMap;
import java.util.Map;

/**
 * A Codec provider for dynamically-typed value classes.  Other providers are needed for containers for maps and arrays.  It provides the
 * following codecs:
 *
 * <ul>
 *     <li>{@link org.bson2.codecs.BinaryCodec}</li>
 *     <li>{@link org.bson2.codecs.BooleanCodec}</li>
 *     <li>{@link org.bson2.codecs.DateCodec}</li>
 *     <li>{@link org.bson2.codecs.DoubleCodec}</li>
 *     <li>{@link org.bson2.codecs.IntegerCodec}</li>
 *     <li>{@link org.bson2.codecs.LongCodec}</li>
 *     <li>{@link org.bson2.codecs.Decimal128Codec}</li>
 *     <li>{@link org.bson2.codecs.MinKeyCodec}</li>
 *     <li>{@link org.bson2.codecs.MaxKeyCodec}</li>
 *     <li>{@link org.bson2.codecs.CodeCodec}</li>
 *     <li>{@link org.bson2.codecs.ObjectIdCodec}</li>
 *     <li>{@link org.bson2.codecs.CharacterCodec}</li>
 *     <li>{@link org.bson2.codecs.StringCodec}</li>
 *     <li>{@link org.bson2.codecs.SymbolCodec}</li>
 *     <li>{@link org.bson2.codecs.UuidCodec}</li>
 *     <li>{@link org.bson2.codecs.ByteCodec}</li>
 *     <li>{@link org.bson2.codecs.ShortCodec}</li>
 *     <li>{@link org.bson2.codecs.ByteArrayCodec}</li>
 *     <li>{@link org.bson2.codecs.FloatCodec}</li>
 *     <li>{@link org.bson2.codecs.AtomicBooleanCodec}</li>
 *     <li>{@link org.bson2.codecs.AtomicIntegerCodec}</li>
 *     <li>{@link org.bson2.codecs.AtomicLongCodec}</li>
 * </ul>
 *
 * @since 3.0
 */
public class ValueCodecProvider implements CodecProvider {
    private final Map<Class<?>, Codec<?>> codecs = new HashMap<Class<?>, Codec<?>>();

    /**
     * A provider of Codecs for simple value types.
     */
    public ValueCodecProvider() {
        addCodecs();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        return (Codec<T>) codecs.get(clazz);
    }

    private void addCodecs() {
        addCodec(new BinaryCodec());
        addCodec(new BooleanCodec());
        addCodec(new DateCodec());
        addCodec(new DoubleCodec());
        addCodec(new IntegerCodec());
        addCodec(new LongCodec());
        addCodec(new MinKeyCodec());
        addCodec(new MaxKeyCodec());
        addCodec(new CodeCodec());
        addCodec(new Decimal128Codec());
        addCodec(new ObjectIdCodec());
        addCodec(new CharacterCodec());
        //addCodec(new StringCodec());

        addCodec(new SelfStringCodec()) ;
        addCodec(new SymbolCodec());
        addCodec(new UuidCodec());

        addCodec(new ByteCodec());
        addCodec(new PatternCodec());
        addCodec(new ShortCodec());
        addCodec(new ByteArrayCodec());
        addCodec(new FloatCodec());
        addCodec(new AtomicBooleanCodec());
        addCodec(new AtomicIntegerCodec());
        addCodec(new AtomicLongCodec());
    }

    private <T> void addCodec(final Codec<T> codec) {
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

        return true;
    }

    @Override
    public int hashCode() {
        return 0;
    }
}
