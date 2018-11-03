/*
 * Copyright (c) 2008-2016 MongoDB, Inc.
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

package com.mongodb2.operation;

import com.mongodb2.WriteConcern;
import com.mongodb2.async.SingleResultCallback;
import com.mongodb2.binding.AsyncWriteBinding;
import com.mongodb2.binding.WriteBinding;
import com.mongodb2.connection.AsyncConnection;
import com.mongodb2.connection.Connection;
import com.mongodb2.connection.ConnectionDescription;
import org.bson2.BsonDocument;
import org.bson2.BsonInt32;

import static com.mongodb2.assertions.Assertions.notNull;
import static com.mongodb2.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb2.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb2.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb2.operation.OperationHelper.LOGGER;
import static com.mongodb2.operation.OperationHelper.releasingCallback;
import static com.mongodb2.operation.OperationHelper.withConnection;
import static com.mongodb2.operation.WriteConcernHelper.appendWriteConcernToCommand;
import static com.mongodb2.operation.WriteConcernHelper.writeConcernErrorTransformer;

/**
 * Operation to drop a database in MongoDB.  The {@code execute} method throws MongoCommandFailureException if something goes wrong, but
 * it will not throw an Exception if the collection does not exist before trying to drop it.
 *
 * @since 3.0
 */
public class DropDatabaseOperation implements AsyncWriteOperation<Void>, WriteOperation<Void> {
    private static final BsonDocument DROP_DATABASE = new BsonDocument("dropDatabase", new BsonInt32(1));
    private final String databaseName;
    private final WriteConcern writeConcern;

    /**
     * Construct a new instance.
     *
     * @param databaseName the name of the database for the operation.
     */
    @Deprecated
    public DropDatabaseOperation(final String databaseName) {
        this(databaseName, null);
    }

    /**
     * Construct a new instance.
     *
     * @param databaseName the name of the database for the operation.
     * @param writeConcern the write concern
     *
     * @since 3.4
     */
    public DropDatabaseOperation(final String databaseName, final WriteConcern writeConcern) {
        this.databaseName = notNull("databaseName", databaseName);
        this.writeConcern = writeConcern;
    }

    /**
     * Gets the write concern.
     *
     * @return the write concern, which may be null
     *
     * @since 3.4
     */
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    @Override
    public Void execute(final WriteBinding binding) {
        return withConnection(binding, new OperationHelper.CallableWithConnection<Void>() {
            @Override
            public Void call(final Connection connection) {
                executeWrappedCommandProtocol(binding, databaseName, getCommand(connection.getDescription()), connection,
                        writeConcernErrorTransformer());
                return null;
            }
        });
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<Void> callback) {
        withConnection(binding, new OperationHelper.AsyncCallableWithConnection() {
            @Override
            public void call(final AsyncConnection connection, final Throwable t) {
                SingleResultCallback<Void> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                } else {
                    executeWrappedCommandProtocolAsync(binding, databaseName, getCommand(connection.getDescription()), connection,
                            writeConcernErrorTransformer(), releasingCallback(errHandlingCallback, connection));

                }
            }
        });
    }

    private BsonDocument getCommand(final ConnectionDescription description) {
        BsonDocument commandDocument = new BsonDocument("dropDatabase", new BsonInt32(1));
        appendWriteConcernToCommand(writeConcern, commandDocument, description);
        return commandDocument;
    }
}
