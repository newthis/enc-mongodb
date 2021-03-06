/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the 'License');
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb2.operation;

import com.mongodb2.MongoNamespace;
import com.mongodb2.binding.WriteBinding;
import com.mongodb2.connection.Connection;
import com.mongodb2.operation.OperationHelper.CallableWithConnection;
import org.bson2.BsonDocument;
import org.bson2.BsonInt32;
import org.bson2.codecs.BsonDocumentCodec;

import static com.mongodb2.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb2.operation.OperationHelper.serverIsAtLeastVersionThreeDotTwo;
import static com.mongodb2.operation.OperationHelper.withConnection;

/**
 * Unlocks the MongoDB server, allowing write operations to go through. This may be asynchronous on the server, which means
 * there may be a small delay before the database becomes writable.
 *
 * @mongodb.driver.manual reference/command/fsyncUnlock/ fsyncUnlock command
 * @since 3.2
 */
public class FsyncUnlockOperation implements WriteOperation<BsonDocument> {
    @Override
    public BsonDocument execute(final WriteBinding binding) {
        return withConnection(binding, new CallableWithConnection<BsonDocument>() {
            @Override
            public BsonDocument call(final Connection connection) {
                if (serverIsAtLeastVersionThreeDotTwo(connection.getDescription())) {
                    return executeWrappedCommandProtocol(binding, "admin", new BsonDocument("fsyncUnlock", new BsonInt32(1)), connection);
                } else {
                    return connection.query(new MongoNamespace("admin", "$cmd.sys.unlock"), new BsonDocument(), null, 0, 1, 0,
                                           false, false, false, false, false, false,
                                           new BsonDocumentCodec()).getResults().get(0);
                }
            }
        });
    }
}
