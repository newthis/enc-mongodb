/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.mongodb2.operation;

import com.mongodb2.MongoWriteConcernException;
import com.mongodb2.ServerAddress;
import com.mongodb2.WriteConcern;
import com.mongodb2.WriteConcernResult;
import com.mongodb2.bulk.WriteConcernError;
import com.mongodb2.connection.ConnectionDescription;
import com.mongodb2.operation.CommandOperationHelper.CommandTransformer;
import org.bson2.BsonDocument;

import static com.mongodb2.operation.OperationHelper.serverIsAtLeastVersionThreeDotFour;

final class WriteConcernHelper {

    static void appendWriteConcernToCommand(final WriteConcern writeConcern, final BsonDocument commandDocument,
                                            final ConnectionDescription description) {
        if (writeConcern != null && !writeConcern.isServerDefault() && serverIsAtLeastVersionThreeDotFour(description)) {
            commandDocument.put("writeConcern", writeConcern.asDocument());
        }
    }

    static CommandTransformer<BsonDocument, Void> writeConcernErrorTransformer() {
        return new CommandTransformer<BsonDocument, Void>() {
            @Override
            public Void apply(final BsonDocument result, final ServerAddress serverAddress) {
                throwOnWriteConcernError(result, serverAddress);
                return null;
            }
        };
    }

    static void throwOnWriteConcernError(final BsonDocument result, final ServerAddress serverAddress) {
        if (hasWriteConcernError(result)) {
            throw createWriteConcernError(result, serverAddress); }

    }

    static boolean hasWriteConcernError(final BsonDocument result) {
        return result.containsKey("writeConcernError");
    }

    static MongoWriteConcernException createWriteConcernError(final BsonDocument result, final ServerAddress serverAddress) {
        return new MongoWriteConcernException(createWriteConcernError(result.getDocument("writeConcernError")),
                                                   WriteConcernResult.acknowledged(0, false, null), serverAddress);
    }

    static WriteConcernError createWriteConcernError(final BsonDocument writeConcernErrorDocument) {
        return new WriteConcernError(writeConcernErrorDocument.getNumber("code").intValue(),
                                            writeConcernErrorDocument.getString("errmsg").getValue(),
                                            writeConcernErrorDocument.getDocument("errInfo", new BsonDocument()));
    }

    private WriteConcernHelper() {
    }
}
