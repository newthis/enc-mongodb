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

import org.bson2.BsonWriter;

/**
 * Instances of this class are capable of encoding an instance of the type parameter {@code T} into a BSON value.
 * .
 * @param <T> the type that the instance can encode into BSON
 *
 * @since 3.0
 */
public interface Encoder<T> {

    /**
     * Encode an instance of the type parameter {@code T} into a BSON value.
     * @param writer the BSON writer to encode into
     * @param value the value to encode
     * @param encoderContext the encoder context
     */
    void encode(BsonWriter writer, T value, EncoderContext encoderContext);

    /**
     * Returns the Class instance that this encodes. This is necessary because Java does not reify generic types.
     *
     * @return the Class instance that this encodes.
     */
    Class<T> getEncoderClass();
}
