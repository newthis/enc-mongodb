/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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

import com.mongodb2.WriteConcern;
import com.mongodb2.bulk.InsertRequest;
import com.mongodb2.internal.validator.CollectibleDocumentFieldNameValidator;
import com.mongodb2.internal.validator.NoOpFieldNameValidator;
import org.bson2.BsonDocument;
import org.bson2.FieldNameValidator;
import org.bson2.io.BsonOutput;

import java.util.List;

/**
 * An insert message.
 *
 * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-insert OP_INSERT
 */
class InsertMessage extends RequestMessage {

    private final boolean ordered;
    private final WriteConcern writeConcern;
    private final List<InsertRequest> insertRequestList;

    /**
     * Construct an instance.
     *
     * @param collectionName    the full name of the collection
     * @param ordered           whether the inserts are ordered
     * @param writeConcern      the write concern
     * @param insertRequestList the list of insert requests
     * @param settings          the message settings
     */
    public InsertMessage(final String collectionName, final boolean ordered, final WriteConcern writeConcern,
                         final List<InsertRequest> insertRequestList, final MessageSettings settings) {
        super(collectionName, OpCode.OP_INSERT, settings);
        this.ordered = ordered;
        this.writeConcern = writeConcern;
        this.insertRequestList = insertRequestList;
    }

    public List<InsertRequest> getInsertRequestList() {
        return insertRequestList;
    }

    @Override
    protected RequestMessage encodeMessageBody(final BsonOutput outputStream, final int messageStartPosition) {
        return encodeMessageBodyWithMetadata(outputStream, messageStartPosition).getNextMessage();
    }

    @Override
    protected EncodingMetadata encodeMessageBodyWithMetadata(final BsonOutput outputStream, final int messageStartPosition) {
        writeInsertPrologue(outputStream);
        int firstDocumentPosition = outputStream.getPosition();
        for (int i = 0; i < insertRequestList.size(); i++) {
            BsonDocument document = insertRequestList.get(i).getDocument();
            int pos = outputStream.getPosition();
            addCollectibleDocument(document, outputStream, createValidator());
            if (outputStream.getPosition() - messageStartPosition > getSettings().getMaxMessageSize()) {
                outputStream.truncateToPosition(pos);
                return new EncodingMetadata(new InsertMessage(getCollectionName(), ordered, writeConcern,
                                                          insertRequestList.subList(i, insertRequestList.size()), getSettings()),
                                            firstDocumentPosition);
            }
        }
        return new EncodingMetadata(null, firstDocumentPosition);
    }

    private FieldNameValidator createValidator() {
        if (getCollectionName().endsWith(".system.indexes")) {
            return new NoOpFieldNameValidator();
        } else {
            return new CollectibleDocumentFieldNameValidator();
        }
    }

    private void writeInsertPrologue(final BsonOutput outputStream) {
        int flags = 0;
        if (!ordered) {
            flags |= 1;
        }
        outputStream.writeInt32(flags);
        outputStream.writeCString(getCollectionName());
    }
}
