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

package com.mongodb2.connection;

import com.mongodb2.MongoInternalException;
import org.bson2.BsonBinaryReader;
import org.bson2.codecs.Decoder;
import org.bson2.codecs.DecoderContext;
import org.bson2.io.BsonInput;
import org.bson2.io.ByteBufferBsonInput;

import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

/**
 * An OP_REPLY message.
 *
 * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-reply OP_REPLY
 * @param <T> the type of the result document
 */
class ReplyMessage<T> {

    private final ReplyHeader replyHeader;
    private final List<T> documents;

    /**
     * Construct an instance.
     *
     * @param responseBuffers the response buffers containing the reply
     * @param decoder the decoder for the result documents
     * @param requestId the expected request id that this is a reply to
     */
    public ReplyMessage(final ResponseBuffers responseBuffers, final Decoder<T> decoder, final long requestId) {
        this(responseBuffers.getReplyHeader(), requestId);

        if (replyHeader.getNumberReturned() > 0) {
            BsonInput bsonInput = new ByteBufferBsonInput(responseBuffers.getBodyByteBuffer());
            while (documents.size() < replyHeader.getNumberReturned()) {
                BsonBinaryReader reader = new BsonBinaryReader(bsonInput);
                try {
                    documents.add(decoder.decode(reader, DecoderContext.builder().build()));
                } finally {
                    reader.close();
                }
            }
        }
    }

    ReplyMessage(final ReplyHeader replyHeader, final long requestId) {
        if (requestId != replyHeader.getResponseTo()) {
            throw new MongoInternalException(format("The responseTo (%d) in the response does not match the requestId (%d) in the "
                                                    + "request", replyHeader.getResponseTo(), requestId));
        }

        this.replyHeader = replyHeader;

        documents = new ArrayList<T>(replyHeader.getNumberReturned());
    }

    /**
     * Gets the reply header.
     *
     * @return the reply header
     */
    public ReplyHeader getReplyHeader() {
        return replyHeader;
    }

    /**
     * Gets the documents.
     *
     * @return the documents
     */
    public List<T> getDocuments() {
        return documents;
    }
}
