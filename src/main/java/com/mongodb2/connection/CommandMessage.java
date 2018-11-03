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
import org.bson2.FieldNameValidator;
import org.bson2.io.BsonOutput;

/**
 * A command message that uses OP_QUERY to send the command.
 *
 * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
 */
class CommandMessage extends RequestMessage {
    private final boolean slaveOk;
    private final BsonDocument command;
    private final FieldNameValidator validator;

    /**
     * Construct an instance.
     *
     * @param collectionName the collection to execute the command in
     * @param command        the command
     * @param slaveOk        if querying of non-primary replica set members is allowed
     * @param settings       the message settings
     */
    public CommandMessage(final String collectionName, final BsonDocument command, final boolean slaveOk, final MessageSettings settings) {
        this(collectionName, command, slaveOk, new NoOpFieldNameValidator(), settings);
    }

    /**
     * Construct an instance.
     *
     * @param collectionName the collection to execute the command in
     * @param command        the command
     * @param slaveOk        if querying of non-primary replica set members is allowed
     * @param validator      the field name validator
     * @param settings       the message settings
     */
    public CommandMessage(final String collectionName, final BsonDocument command, final boolean slaveOk,
                          final FieldNameValidator validator, final MessageSettings settings) {
        super(collectionName, OpCode.OP_QUERY, settings);
        this.slaveOk = slaveOk;
        this.command = command;
        this.validator = validator;
    }

    @Override
    protected RequestMessage encodeMessageBody(final BsonOutput bsonOutput, final int messageStartPosition) {
        return encodeMessageBodyWithMetadata(bsonOutput, messageStartPosition).getNextMessage();
    }

    @Override
    protected EncodingMetadata encodeMessageBodyWithMetadata(final BsonOutput bsonOutput, final int messageStartPosition) {
        bsonOutput.writeInt32(slaveOk ? 1 << 2 : 0);
        bsonOutput.writeCString(getCollectionName());
        bsonOutput.writeInt32(0);
        bsonOutput.writeInt32(-1);
        int firstDocumentPosition = bsonOutput.getPosition();
        addDocument(command, bsonOutput, validator);
        return new EncodingMetadata(null, firstDocumentPosition);
    }
}
