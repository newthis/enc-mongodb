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

import com.mongodb2.MongoCommandException;
import com.mongodb2.MongoNamespace;
import com.mongodb2.WriteConcern;
import com.mongodb2.WriteConcernResult;
import com.mongodb2.async.SingleResultCallback;
import com.mongodb2.binding.AsyncWriteBinding;
import com.mongodb2.binding.WriteBinding;
import com.mongodb2.bulk.DeleteRequest;
import com.mongodb2.connection.AsyncConnection;
import com.mongodb2.connection.Connection;
import com.mongodb2.connection.ConnectionDescription;
import org.bson2.BsonDocument;
import org.bson2.BsonString;

import static com.mongodb2.assertions.Assertions.notNull;
import static com.mongodb2.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb2.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb2.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb2.operation.OperationHelper.AsyncCallableWithConnection;
import static com.mongodb2.operation.OperationHelper.CallableWithConnection;
import static com.mongodb2.operation.OperationHelper.LOGGER;
import static com.mongodb2.operation.OperationHelper.releasingCallback;
import static com.mongodb2.operation.OperationHelper.serverIsAtLeastVersionTwoDotSix;
import static com.mongodb2.operation.OperationHelper.withConnection;
import static com.mongodb2.operation.UserOperationHelper.translateUserCommandException;
import static com.mongodb2.operation.UserOperationHelper.userCommandCallback;
import static com.mongodb2.operation.WriteConcernHelper.appendWriteConcernToCommand;
import static com.mongodb2.operation.WriteConcernHelper.writeConcernErrorTransformer;
import static java.util.Arrays.asList;

/**
 * An operation to remove a user.
 *
 * @since 3.0
 */
public class DropUserOperation implements AsyncWriteOperation<Void>, WriteOperation<Void> {
    private final String databaseName;
    private final String userName;
    private final WriteConcern writeConcern;

    /**
     * Construct a new instance.
     *
     * @param databaseName the name of the database for the operation.
     * @param userName     the name of the user to be dropped.
     * @deprecated Prefer {@link #DropUserOperation(String, String, WriteConcern)}
     */
    @Deprecated
    public DropUserOperation(final String databaseName, final String userName) {
        this(databaseName, userName, null);
    }

    /**
     * Construct a new instance.
     *
     * @param databaseName the name of the database for the operation.
     * @param userName     the name of the user to be dropped.
     * @param writeConcern the write concern
     *
     * @since 3.4
     */
    public DropUserOperation(final String databaseName, final String userName, final WriteConcern writeConcern) {
        this.databaseName = notNull("databaseName", databaseName);
        this.userName = notNull("userName", userName);
        this.writeConcern = writeConcern;
    }

    @Override
    public Void execute(final WriteBinding binding) {
        return withConnection(binding, new CallableWithConnection<Void>() {
            @Override
            public Void call(final Connection connection) {
                if (serverIsAtLeastVersionTwoDotSix(connection.getDescription())) {
                    try {
                        executeWrappedCommandProtocol(binding, databaseName, getCommand(connection.getDescription()), connection,
                                writeConcernErrorTransformer());
                    } catch (MongoCommandException e) {
                        translateUserCommandException(e);
                    }
                } else {
                    connection.delete(getNamespace(), true, WriteConcern.ACKNOWLEDGED, asList(getDeleteRequest()));
                }
                return null;
            }
        });
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<Void> callback) {
        withConnection(binding, new AsyncCallableWithConnection() {
            @Override
            public void call(final AsyncConnection connection, final Throwable t) {
                SingleResultCallback<Void> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                } else {
                    final SingleResultCallback<Void> wrappedCallback = releasingCallback(errHandlingCallback, connection);

                    if (serverIsAtLeastVersionTwoDotSix(connection.getDescription())) {
                        executeWrappedCommandProtocolAsync(binding, databaseName, getCommand(connection.getDescription()), connection,
                                writeConcernErrorTransformer(), userCommandCallback(wrappedCallback));
                    } else {
                        connection.deleteAsync(getNamespace(), true, WriteConcern.ACKNOWLEDGED, asList(getDeleteRequest()),
                                               new SingleResultCallback<WriteConcernResult>() {
                                                   @Override
                                                   public void onResult(final WriteConcernResult result, final Throwable t) {
                                                       wrappedCallback.onResult(null, t);
                                                   }
                                               });
                    }
                }
            }
        });
    }

    private MongoNamespace getNamespace() {
        return new MongoNamespace(databaseName, "system.users");
    }

    private DeleteRequest getDeleteRequest() {
        return new DeleteRequest(new BsonDocument("user", new BsonString(userName)));
    }

    private BsonDocument getCommand(final ConnectionDescription description) {
        BsonDocument commandDocument = new BsonDocument("dropUser", new BsonString(userName));
        appendWriteConcernToCommand(writeConcern, commandDocument, description);
        return commandDocument;
    }
}
