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

import org.bson2.BsonJavaScript;
import org.bson2.BsonReader;
import org.bson2.BsonWriter;

/**
 * A Codec for the {@code BsonJavaScript} type.
 *
 * @since 3.0
 */
public class BsonJavaScriptCodec implements Codec<BsonJavaScript> {
    @Override
    public BsonJavaScript decode(final BsonReader reader, final DecoderContext decoderContext) {
        return new BsonJavaScript(reader.readJavaScript());
    }

    @Override
    public void encode(final BsonWriter writer, final BsonJavaScript value, final EncoderContext encoderContext) {
        writer.writeJavaScript(value.getCode());
    }

    @Override
    public Class<BsonJavaScript> getEncoderClass() {
        return BsonJavaScript.class;
    }
}