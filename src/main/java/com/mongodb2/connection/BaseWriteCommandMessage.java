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

import com.mongodb2.MongoNamespace;
import com.mongodb2.WriteConcern;
import org.bson2.BsonBinaryWriter;
import org.bson2.BsonBinaryWriterSettings;
import org.bson2.BsonDocument;
import org.bson2.BsonWriterSettings;
import org.bson2.FieldNameValidator;
import org.bson2.codecs.EncoderContext;
import org.bson2.io.BsonOutput;

import static com.mongodb2.MongoNamespace.COMMAND_COLLECTION_NAME;

/**
 * Abstract base class for write command message.  Supports splitting into multiple messages.
 */
abstract class BaseWriteCommandMessage extends RequestMessage {
    // Server allows command document to exceed max document size by 16K, so that it can comfortably fit a stored document inside it
    private static final int HEADROOM = 16 * 1024;

    private final MongoNamespace writeNamespace;
    private final boolean ordered;
    private final WriteConcern writeConcern;
    private final Boolean bypassDocumentValidation;

    /**
     * Construct an instance.
     *
     * @param writeNamespace           the namespace
     * @param ordered                  whether the writes are ordered
     * @param writeConcern             the write concern
     * @param bypassDocumentValidation the bypass documentation validation flag
     * @param settings                 the message settings
     */
    public BaseWriteCommandMessage(final MongoNamespace writeNamespace, final boolean ordered, final WriteConcern writeConcern,
                                   final Boolean bypassDocumentValidation, final MessageSettings settings) {
        super(new MongoNamespace(writeNamespace.getDatabaseName(), COMMAND_COLLECTION_NAME).getFullName(), OpCode.OP_QUERY, settings);

        this.writeNamespace = writeNamespace;
        this.ordered = ordered;
        this.writeConcern = writeConcern;
        this.bypassDocumentValidation = bypassDocumentValidation;
    }

    /**
     * Gets the namespace to write to.
     *
     * @return the namespace
     */
    public MongoNamespace getWriteNamespace() {
        return writeNamespace;
    }

    /**
     * Gets the write concern.
     *
     * @return the write concern
     */
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    /**
     * Gets whether the writes are ordered.
     *
     * @return whether the writes are ordered
     */
    public boolean isOrdered() {
        return ordered;
    }

    /**
     * Gets the bypass document validation flag
     *
     * @return the bypass document validation flag
     */
    public Boolean getBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    @Override
    public BaseWriteCommandMessage encode(final BsonOutput outputStream) {
        return (BaseWriteCommandMessage) super.encode(outputStream);
    }

    /**
     * Gets the number of write requests left to encode.  Note that these may end up being split into multiple messages.
     *
     * @return the count of write requests left to encode
     */
    public abstract int getItemCount();

    @Override
    protected BaseWriteCommandMessage encodeMessageBody(final BsonOutput outputStream, final int messageStartPosition) {
        return (BaseWriteCommandMessage) encodeMessageBodyWithMetadata(outputStream, messageStartPosition).getNextMessage();
    }

    @Override
    protected EncodingMetadata encodeMessageBodyWithMetadata(final BsonOutput outputStream, final int messageStartPosition) {
        BaseWriteCommandMessage nextMessage = null;

        writeCommandHeader(outputStream);

        int commandStartPosition = outputStream.getPosition();
        int firstDocumentStartPosition = outputStream.getPosition();
        BsonBinaryWriter writer = new BsonBinaryWriter(new BsonWriterSettings(),
                new BsonBinaryWriterSettings(getSettings().getMaxDocumentSize() + HEADROOM),
                outputStream, getFieldNameValidator());
        try {
            writer.writeStartDocument();
            writeCommandPrologue(writer);
            nextMessage = writeTheWrites(outputStream, commandStartPosition, writer);
            writer.writeEndDocument();
        } finally {
            writer.close();
        }
        return new EncodingMetadata(nextMessage, firstDocumentStartPosition);
    }

    /**
     * Gets the field name validator to apply.
     *
     * @return the validator
     */
    protected abstract FieldNameValidator getFieldNameValidator();

    private void writeCommandHeader(final BsonOutput outputStream) {
        outputStream.writeInt32(0);
        outputStream.writeCString(getCollectionName());

        outputStream.writeInt32(0);
        outputStream.writeInt32(-1);
    }

    /**
     * Gets the name of the write command
     *
     * @return the name of the write command
     */
    protected abstract String getCommandName();

    /**
     * Write the list of writes to the output after the write command prologue has been written.
     *
     * @param bsonOutput           the BSON output
     * @param commandStartPosition the position in the output where the command document starts
     * @param writer               the writer
     * @return the next message to encode, if this one overflowed.  This may be null, which indicates that we're done
     */
    protected abstract BaseWriteCommandMessage writeTheWrites(final BsonOutput bsonOutput, final int commandStartPosition,
                                                              final BsonBinaryWriter writer);

    boolean exceedsLimits(final int batchLength, final int batchItemCount) {
        return (exceedsBatchLengthLimit(batchLength, batchItemCount) || exceedsBatchItemCountLimit(batchItemCount));
    }

    // make a special exception for a command with only a single item added to it.  It's allowed to exceed maximum document size so that
    // it's possible to, say, send a replacement document that is itself 16MB, which would push the size of the containing command
    // document to be greater than the maximum document size.
    private boolean exceedsBatchLengthLimit(final int batchLength, final int batchItemCount) {
        return batchLength > getSettings().getMaxDocumentSize() && batchItemCount > 1;
    }

    private boolean exceedsBatchItemCountLimit(final int batchItemCount) {
        return batchItemCount > getSettings().getMaxBatchCount();
    }

    private void writeCommandPrologue(final BsonBinaryWriter writer) {
        writer.writeString(getCommandName(), getWriteNamespace().getCollectionName());
        writer.writeBoolean("ordered", ordered);
        if (!getWriteConcern().isServerDefault()) {
            writer.writeName("writeConcern");
            BsonDocument document = getWriteConcern().asDocument();
            getCodec(document).encode(writer, document, EncoderContext.builder().build());
        }
        if (getBypassDocumentValidation() != null) {
            writer.writeBoolean("bypassDocumentValidation", getBypassDocumentValidation());
        }
    }
}
