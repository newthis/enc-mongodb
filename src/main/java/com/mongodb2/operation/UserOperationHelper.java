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
import com.mongodb2.MongoCredential;
import com.mongodb2.async.SingleResultCallback;
import org.bson2.BsonArray;
import org.bson2.BsonBoolean;
import org.bson2.BsonDocument;
import org.bson2.BsonObjectId;
import org.bson2.BsonString;
import org.bson2.BsonValue;
import org.bson2.types.ObjectId;

import java.util.Arrays;

import static com.mongodb2.internal.authentication.NativeAuthenticationHelper.createAuthenticationHash;
import static com.mongodb2.operation.WriteConcernHelper.createWriteConcernError;
import static com.mongodb2.operation.WriteConcernHelper.hasWriteConcernError;

final class UserOperationHelper {

    static BsonDocument asCommandDocument(final MongoCredential credential, final boolean readOnly, final String commandName) {
        BsonDocument document = new BsonDocument();
        document.put(commandName, new BsonString(credential.getUserName()));
        document.put("pwd", new BsonString(createAuthenticationHash(credential.getUserName(),
                                                                    credential.getPassword())));
        document.put("digestPassword", BsonBoolean.FALSE);
        document.put("roles", new BsonArray(Arrays.<BsonValue>asList(new BsonString(getRoleName(credential, readOnly)))));
        return document;
    }

    private static String getRoleName(final MongoCredential credential, final boolean readOnly) {
        return credential.getSource().equals("admin")
               ? (readOnly ? "readAnyDatabase" : "root") : (readOnly ? "read" : "dbOwner");
    }

    static BsonDocument asCollectionQueryDocument(final MongoCredential credential) {
        return new BsonDocument("user", new BsonString(credential.getUserName()));
    }

    static BsonDocument asCollectionUpdateDocument(final MongoCredential credential, final boolean readOnly) {
        return asCollectionQueryDocument(credential)
               .append("pwd", new BsonString(createAuthenticationHash(credential.getUserName(), credential.getPassword())))
               .append("readOnly", BsonBoolean.valueOf(readOnly));
    }

    static BsonDocument asCollectionInsertDocument(final MongoCredential credential, final boolean readOnly) {
        return asCollectionUpdateDocument(credential, readOnly)
               .append("_id", new BsonObjectId(new ObjectId()));
    }


    static void translateUserCommandException(final MongoCommandException e) {
        if (e.getErrorCode() == 100 && hasWriteConcernError(e.getResponse())) {
            throw createWriteConcernError(e.getResponse(), e.getServerAddress());
        } else {
            throw e;
        }
    }

    static SingleResultCallback<Void> userCommandCallback(final SingleResultCallback<Void> wrappedCallback) {
        return new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                if (t != null) {
                    if (t instanceof MongoCommandException
                                && hasWriteConcernError(((MongoCommandException) t).getResponse())) {
                        wrappedCallback.onResult(null,
                                createWriteConcernError(((MongoCommandException) t).getResponse(),
                                        ((MongoCommandException) t).getServerAddress()));
                    } else {
                        wrappedCallback.onResult(null, t);
                    }
                } else {
                    wrappedCallback.onResult(null, null);
                }
            }
        };
    }

    private UserOperationHelper() {
    }
}
