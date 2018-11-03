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

import org.bson2.BsonDouble;
import org.bson2.BsonReader;
import org.bson2.BsonWriter;

/**
 * A Codec for BsonDouble instances.
 *
 * @since 3.0
 */
public class BsonDoubleCodec implements Codec<BsonDouble> {
    @Override
    public BsonDouble decode(final BsonReader reader, final DecoderContext decoderContext) {
        return new BsonDouble(reader.readDouble());
    }

    @Override
    public void encode(final BsonWriter writer, final BsonDouble value, final EncoderContext encoderContext) {
        writer.writeDouble(value.getValue());
    }

    @Override
    public Class<BsonDouble> getEncoderClass() {
        return BsonDouble.class;
    }
}
