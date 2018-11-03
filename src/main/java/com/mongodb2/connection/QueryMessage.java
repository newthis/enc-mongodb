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

import com.mongodb2.internal.validator.NoOpFieldNameValidator;
import org.bson2.BsonDocument;
import org.bson2.io.BsonOutput;

/**
 * An OP_QUERY message for an actual query (not a command).
 *
 * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
 */
class QueryMessage extends BaseQueryMessage {
    private final BsonDocument queryDocument;
    private final BsonDocument fields;

    /**
     * Construct an instance.
     *
     * @param collectionName the collection name
     * @param skip           the number of documents to skip
     * @param numberToReturn the number to return
     * @param queryDocument  the query document
     * @param fields         the fields to return in the result documents
     * @param settings       the message settings
     */
    public QueryMessage(final String collectionName, final int skip,
                        final int numberToReturn, final BsonDocument queryDocument,
                        final BsonDocument fields, final MessageSettings settings) {
        super(collectionName, skip, numberToReturn, settings);
        this.queryDocument = queryDocument;
        this.fields = fields;
    }

    @Override
    protected RequestMessage encodeMessageBody(final BsonOutput bsonOutput, final int messageStartPosition) {
       return encodeMessageBodyWithMetadata(bsonOutput, messageStartPosition).getNextMessage();
    }

    @Override
    protected EncodingMetadata encodeMessageBodyWithMetadata(final BsonOutput bsonOutput, final int messageStartPosition) {
        writeQueryPrologue(bsonOutput);
        int firstDocumentStartPosition = bsonOutput.getPosition();
        addDocument(queryDocument, bsonOutput, new NoOpFieldNameValidator());
        if (fields != null) {
            addDocument(fields, bsonOutput, new NoOpFieldNameValidator());
        }
        return new EncodingMetadata(null, firstDocumentStartPosition);
    }
}
