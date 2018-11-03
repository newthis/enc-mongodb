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

package com.mongodb2.connection;

import com.mongodb2.MongoCommandException;
import com.mongodb2.MongoCredential;
import com.mongodb2.MongoSecurityException;
import com.mongodb2.async.SingleResultCallback;
import org.bson2.BsonDocument;
import org.bson2.BsonString;

import static com.mongodb2.connection.CommandHelper.executeCommand;
import static com.mongodb2.connection.CommandHelper.executeCommandAsync;
import static com.mongodb2.internal.authentication.NativeAuthenticationHelper.getAuthCommand;
import static com.mongodb2.internal.authentication.NativeAuthenticationHelper.getNonceCommand;

class NativeAuthenticator extends Authenticator {
    public NativeAuthenticator(final MongoCredential credential) {
        super(credential);
    }

    @Override
    public void authenticate(final InternalConnection connection, final ConnectionDescription connectionDescription) {
        try {
            BsonDocument nonceResponse = executeCommand(getCredential().getSource(),
                                                         getNonceCommand(),
                                                         connection);

            BsonDocument authCommand = getAuthCommand(getCredential().getUserName(),
                                                      getCredential().getPassword(),
                                                      ((BsonString) nonceResponse.get("nonce")).getValue());
            executeCommand(getCredential().getSource(), authCommand, connection);
        } catch (MongoCommandException e) {
            throw new MongoSecurityException(getCredential(), "Exception authenticating", e);
        }
    }

    @Override
    void authenticateAsync(final InternalConnection connection, final ConnectionDescription connectionDescription,
                           final SingleResultCallback<Void> callback) {
        executeCommandAsync(getCredential().getSource(), getNonceCommand(), connection,
                            new SingleResultCallback<BsonDocument>() {
                                @Override
                                public void onResult(final BsonDocument nonceResult, final Throwable t) {
                                    if (t != null) {
                                        callback.onResult(null, translateThrowable(t));
                                    } else {
                                        executeCommandAsync(getCredential().getSource(),
                                                            getAuthCommand(getCredential().getUserName(), getCredential().getPassword(),
                                                                           ((BsonString) nonceResult.get("nonce")).getValue()),
                                                            connection,
                                                            new SingleResultCallback<BsonDocument>() {
                                                                @Override
                                                                public void onResult(final BsonDocument result, final Throwable t) {
                                                                    if (t != null) {
                                                                        callback.onResult(null, translateThrowable(t));
                                                                    } else {
                                                                        callback.onResult(null, null);
                                                                    }
                                                                }
                                                            });
                                    }
                                }
                            });
    }

    private Throwable translateThrowable(final Throwable t) {
        if (t instanceof MongoCommandException) {
            return new MongoSecurityException(getCredential(), "Exception authenticating", t);
        } else {
            return t;
        }
    }
}
