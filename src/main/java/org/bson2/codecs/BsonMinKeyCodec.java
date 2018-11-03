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

import org.bson2.BsonMinKey;
import org.bson2.BsonReader;
import org.bson2.BsonWriter;

/**
 * A codec for {@code BsonMinKey} instances.
 *
 * @since 3.0
 */
public class BsonMinKeyCodec implements Codec<BsonMinKey> {
    @Override
    public void encode(final BsonWriter writer, final BsonMinKey value, final EncoderContext encoderContext) {
        writer.writeMinKey();
    }

    @Override
    public BsonMinKey decode(final BsonReader reader, final DecoderContext decoderContext) {
        reader.readMinKey();
        return new BsonMinKey();
    }

    @Override
    public Class<BsonMinKey> getEncoderClass() {
        return BsonMinKey.class;
    }
}
