package org.bson2.codecs;

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

import org.bson2.BsonReader;
import org.bson2.BsonType;
import org.bson2.BsonWriter;


import org.bson2.BsonReader;
import org.bson2.BsonType;
import org.bson2.BsonWriter;
import util.ValueConverter;

/**
 * Encodes and decodes {@code String} objects.
 *
 * @since 3.0
 */
public class SelfStringCodec implements Codec<String> {
    @Override
    public void encode(final BsonWriter writer, final String value, final EncoderContext encoderContext) {
        String ust = ValueConverter.convertString(value) ;
        writer.writeString(ust);
    }

    @Override
    public String decode(final BsonReader reader, final DecoderContext decoderContext) {
        if (reader.getCurrentBsonType() == BsonType.SYMBOL) {
            //return reader.readSymbol();
            return ValueConverter.reconvertString(reader.readSymbol());
        } else {
            return ValueConverter.reconvertString(reader.readString()) ;
            //return reader.readString();
        }
    }

    @Override
    public Class<String> getEncoderClass() {
        return String.class;
    }
}
