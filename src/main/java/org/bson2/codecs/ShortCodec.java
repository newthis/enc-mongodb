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

import org.bson2.BsonReader;
import org.bson2.BsonWriter;

/**
 * Encodes and decodes {@code Short} objects.
 *
 * @since 3.0
 */
public class ShortCodec implements Codec<Short> {
    @Override
    public void encode(final BsonWriter writer, final Short value, final EncoderContext encoderContext) {
        writer.writeInt32(value);
    }

    @Override
    public Short decode(final BsonReader reader, final DecoderContext decoderContext) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Class<Short> getEncoderClass() {
        return Short.class;
    }
}
