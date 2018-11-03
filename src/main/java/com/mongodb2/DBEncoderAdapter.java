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

package com.mongodb2;

import org.bson2.BsonBinaryReader;
import org.bson2.BsonWriter;
import org.bson2.ByteBufNIO;
import org.bson2.codecs.Encoder;
import org.bson2.codecs.EncoderContext;
import org.bson2.io.BasicOutputBuffer;
import org.bson2.io.ByteBufferBsonInput;

import static com.mongodb2.assertions.Assertions.notNull;
import static java.nio.ByteBuffer.wrap;

class DBEncoderAdapter implements Encoder<DBObject> {

    private final DBEncoder encoder;

    public DBEncoderAdapter(final DBEncoder encoder) {
        this.encoder = notNull("encoder", encoder);
    }

    // TODO: this can be optimized to reduce copying of buffers.  For that we'd need an InputBuffer that could iterate
    //       over an array of ByteBuffer instances from a PooledByteBufferOutputBuffer
    @Override
    public void encode(final BsonWriter writer, final DBObject document, final EncoderContext encoderContext) {
        BasicOutputBuffer buffer = new BasicOutputBuffer();
        try {
            encoder.writeObject(buffer, document);
            BsonBinaryReader reader = new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(wrap(buffer.toByteArray()))));
            try {
                writer.pipe(reader);
            } finally {
                reader.close();
            }
        } finally {
            buffer.close();
        }
    }

    @Override
    public Class<DBObject> getEncoderClass() {
        return DBObject.class;
    }
}
