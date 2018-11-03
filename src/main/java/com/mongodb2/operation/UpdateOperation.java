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

package com.mongodb2.operation;

import com.mongodb2.MongoNamespace;
import com.mongodb2.WriteConcern;
import com.mongodb2.WriteConcernResult;
import com.mongodb2.async.SingleResultCallback;
import com.mongodb2.bulk.BulkWriteResult;
import com.mongodb2.bulk.UpdateRequest;
import com.mongodb2.bulk.WriteRequest;
import com.mongodb2.connection.AsyncConnection;
import com.mongodb2.connection.Connection;

import java.util.List;

import static com.mongodb2.assertions.Assertions.notNull;
import static com.mongodb2.operation.OperationHelper.AsyncCallableWithConnection;
import static com.mongodb2.operation.OperationHelper.validateWriteRequestCollations;

/**
 * An operation that updates a document in a collection.
 *
 * @since 3.0
 */
public class UpdateOperation extends BaseWriteOperation {
    private final List<UpdateRequest> updates;

    /**
     * Construct an instance.
     *
     * @param namespace the database and collection namespace for the operation.
     * @param ordered whether the updates are ordered.
     * @param writeConcern the write concern for the operation.
     * @param updates the update requests.
     */
    public UpdateOperation(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                           final List<UpdateRequest> updates) {
        super(namespace, ordered, writeConcern);
        this.updates = notNull("update", updates);
    }

    /**
     * Gets the list of update requests.
     *
     * @return the update requests
     */
    public List<UpdateRequest> getUpdateRequests() {
        return updates;
    }

    @Override
    protected WriteConcernResult executeProtocol(final Connection connection) {
        validateWriteRequestCollations(connection, updates, getWriteConcern());
        return connection.update(getNamespace(), isOrdered(), getWriteConcern(), updates);
    }

    @Override
    protected void executeProtocolAsync(final AsyncConnection connection, final SingleResultCallback<WriteConcernResult> callback) {
        validateWriteRequestCollations(connection, updates, getWriteConcern(), new AsyncCallableWithConnection(){
            @Override
            public void call(final AsyncConnection connection, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    connection.updateAsync(getNamespace(), isOrdered(), getWriteConcern(), updates, callback);
                }
            }
        });
    }

    @Override
    protected BulkWriteResult executeCommandProtocol(final Connection connection) {
        validateWriteRequestCollations(connection, updates, getWriteConcern());
        return connection.updateCommand(getNamespace(), isOrdered(), getWriteConcern(), getBypassDocumentValidation(), updates);
    }

    @Override
    protected void executeCommandProtocolAsync(final AsyncConnection connection, final SingleResultCallback<BulkWriteResult> callback) {
        validateWriteRequestCollations(connection, updates, getWriteConcern(), new AsyncCallableWithConnection(){
            @Override
            public void call(final AsyncConnection connection, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    connection.updateCommandAsync(getNamespace(), isOrdered(), getWriteConcern(), getBypassDocumentValidation(), updates,
                            callback);
                }
            }
        });
    }

    @Override
    protected WriteRequest.Type getType() {
        return WriteRequest.Type.UPDATE;
    }

    @Override
    protected int getCount(final BulkWriteResult bulkWriteResult) {
        return bulkWriteResult.getMatchedCount() + bulkWriteResult.getUpserts().size();
    }

    @Override
    protected boolean getUpdatedExisting(final BulkWriteResult bulkWriteResult) {
        return bulkWriteResult.getMatchedCount() > 0;
    }
}
