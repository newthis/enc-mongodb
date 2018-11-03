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

import com.mongodb2.AuthenticationMechanism;
import com.mongodb2.MongoCommandException;
import com.mongodb2.MongoCredential;
import com.mongodb2.MongoSecurityException;
import com.mongodb2.async.SingleResultCallback;
import org.bson2.BsonDocument;
import org.bson2.BsonInt32;
import org.bson2.BsonString;

import static com.mongodb2.connection.CommandHelper.executeCommand;
import static com.mongodb2.connection.CommandHelper.executeCommandAsync;

class X509Authenticator extends Authenticator {
    X509Authenticator(final MongoCredential credential) {
        super(credential);
    }

    @Override
    void authenticate(final InternalConnection connection, final ConnectionDescription connectionDescription) {
        try {
            validateUserName(connectionDescription);
            BsonDocument authCommand = getAuthCommand(getCredential().getUserName());
            executeCommand(getCredential().getSource(), authCommand, connection);
        } catch (MongoCommandException e) {
            throw new MongoSecurityException(getCredential(), "Exception authenticating", e);
        }
    }

    @Override
    void authenticateAsync(final InternalConnection connection, final ConnectionDescription connectionDescription,
                           final SingleResultCallback<Void> callback) {
        try {
            validateUserName(connectionDescription);
            executeCommandAsync(getCredential().getSource(), getAuthCommand(getCredential().getUserName()), connection,
                                new SingleResultCallback<BsonDocument>() {
                                    @Override
                                    public void onResult(final BsonDocument nonceResult, final Throwable t) {
                                        if (t != null) {
                                            callback.onResult(null, translateThrowable(t));
                                        } else {
                                            callback.onResult(null, null);
                                        }
                                    }
                                });
        } catch (Throwable t) {
            callback.onResult(null, t);
        }
    }

    private BsonDocument getAuthCommand(final String userName) {
        BsonDocument cmd = new BsonDocument();

        cmd.put("authenticate", new BsonInt32(1));
        if (userName != null) {
            cmd.put("user", new BsonString(userName));
        }
        cmd.put("mechanism", new BsonString(AuthenticationMechanism.MONGODB_X509.getMechanismName()));

        return cmd;
    }

    private Throwable translateThrowable(final Throwable t) {
        if (t instanceof MongoCommandException) {
            return new MongoSecurityException(getCredential(), "Exception authenticating", t);
        } else {
            return t;
        }
    }

    private void validateUserName(final ConnectionDescription connectionDescription) {
        if (getCredential().getUserName() == null && connectionDescription.getServerVersion().compareTo(new ServerVersion(3, 4)) < 0) {
            throw new MongoSecurityException(getCredential(), "User name is required for the MONGODB-X509 authentication mechanism "
                                                                      + "on server versions less than 3.4");
        }
    }
}
