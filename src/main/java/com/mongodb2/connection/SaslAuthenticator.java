/*
 * Copyright 2008-2016 MongoDB, Inc.
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

import com.mongodb2.MongoCredential;
import com.mongodb2.MongoSecurityException;
import com.mongodb2.ServerAddress;
import com.mongodb2.async.SingleResultCallback;
import org.bson2.BsonBinary;
import org.bson2.BsonDocument;
import org.bson2.BsonInt32;
import org.bson2.BsonString;

import javax.security.auth.Subject;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import java.security.PrivilegedAction;

import static com.mongodb2.MongoCredential.JAVA_SUBJECT_KEY;
import static com.mongodb2.connection.CommandHelper.executeCommand;
import static com.mongodb2.connection.CommandHelper.executeCommandAsync;

abstract class SaslAuthenticator extends Authenticator {

    SaslAuthenticator(final MongoCredential credential) {
        super(credential);
    }

    public void authenticate(final InternalConnection connection, final ConnectionDescription connectionDescription) {
        doAsSubject(new PrivilegedAction<Void>() {
            @Override
            public Void run() {
                SaslClient saslClient = createSaslClient(connection.getDescription().getServerAddress());
                try {
                    byte[] response = (saslClient.hasInitialResponse() ? saslClient.evaluateChallenge(new byte[0]) : null);
                    BsonDocument res = sendSaslStart(response, connection);

                    BsonInt32 conversationId = res.getInt32("conversationId");

                    while (!(res.getBoolean("done")).getValue()) {
                        response = saslClient.evaluateChallenge((res.getBinary("payload")).getData());

                        if (response == null) {
                            throw new MongoSecurityException(getCredential(),
                                    "SASL protocol error: no client response to challenge for credential "
                                            + getCredential());
                        }

                        res = sendSaslContinue(conversationId, response, connection);
                    }
                } catch (Exception e) {
                    throw wrapInMongoSecurityException(e);
                } finally {
                    disposeOfSaslClient(saslClient);
                }
                return null;
            }
        });
    }

    @Override
    void authenticateAsync(final InternalConnection connection, final ConnectionDescription connectionDescription,
                           final SingleResultCallback<Void> callback) {
        try {
            doAsSubject(new PrivilegedAction<Void>() {
                @Override
                public Void run() {
                    final SaslClient saslClient = createSaslClient(connection.getDescription().getServerAddress());
                    try {
                        byte[] response = (saslClient.hasInitialResponse() ? saslClient.evaluateChallenge(new byte[0]) : null);
                        sendSaslStartAsync(response, connection, new SingleResultCallback<BsonDocument>() {
                            @Override
                            public void onResult(final BsonDocument result, final Throwable t) {
                                if (t != null) {
                                    callback.onResult(null, wrapInMongoSecurityException(t));
                                } else if (result.getBoolean("done").getValue()) {
                                    callback.onResult(null, null);
                                } else {
                                    new Continuator(saslClient, result, connection, callback).start();
                                }
                            }
                        });
                    } catch (SaslException e) {
                        throw wrapInMongoSecurityException(e);
                    }
                    return null;
                }
            });
        } catch (Throwable t) {
            callback.onResult(null, t);
        }
    }

    public abstract String getMechanismName();

    protected abstract SaslClient createSaslClient(final ServerAddress serverAddress);

    private Subject getSubject() {
        return getCredential().<Subject>getMechanismProperty(JAVA_SUBJECT_KEY, null);
    }

    private BsonDocument sendSaslStart(final byte[] outToken, final InternalConnection connection) {
        return executeCommand(getCredential().getSource(), createSaslStartCommandDocument(outToken), connection);
    }

    private BsonDocument sendSaslContinue(final BsonInt32 conversationId, final byte[] outToken, final InternalConnection connection) {
        return executeCommand(getCredential().getSource(), createSaslContinueDocument(conversationId, outToken), connection);
    }

    private void sendSaslStartAsync(final byte[] outToken, final InternalConnection connection,
                                    final SingleResultCallback<BsonDocument> callback) {
        executeCommandAsync(getCredential().getSource(), createSaslStartCommandDocument(outToken), connection,
                callback);
    }

    private void sendSaslContinueAsync(final BsonInt32 conversationId, final byte[] outToken, final InternalConnection connection,
                                       final SingleResultCallback<BsonDocument> callback) {
        executeCommandAsync(getCredential().getSource(), createSaslContinueDocument(conversationId, outToken), connection,
                callback);
    }

    private BsonDocument createSaslStartCommandDocument(final byte[] outToken) {
        return new BsonDocument("saslStart", new BsonInt32(1)).append("mechanism", new BsonString(getMechanismName()))
                .append("payload", new BsonBinary(outToken != null ? outToken : new byte[0]));
    }

    private BsonDocument createSaslContinueDocument(final BsonInt32 conversationId, final byte[] outToken) {
        return new BsonDocument("saslContinue", new BsonInt32(1)).append("conversationId", conversationId)
                .append("payload", new BsonBinary(outToken));
    }

    private void disposeOfSaslClient(final SaslClient saslClient) {
        try {
            saslClient.dispose();
        } catch (SaslException e) { // NOPMD
            // ignore
        }
    }

    private MongoSecurityException wrapInMongoSecurityException(final Throwable t) {
        return t instanceof MongoSecurityException
                ? (MongoSecurityException) t
                : new MongoSecurityException(getCredential(), "Exception authenticating " + getCredential(), t);
    }

    void doAsSubject(final java.security.PrivilegedAction<Void> action) {
        if (getSubject() == null) {
            action.run();
        } else {
            Subject.doAs(getSubject(), action);
        }
    }

    private final class Continuator implements SingleResultCallback<BsonDocument> {
        private final SaslClient saslClient;
        private final BsonDocument saslStartDocument;
        private final InternalConnection connection;
        private final SingleResultCallback<Void> callback;

        public Continuator(final SaslClient saslClient, final BsonDocument saslStartDocument, final InternalConnection connection,
                           final SingleResultCallback<Void> callback) {
            this.saslClient = saslClient;
            this.saslStartDocument = saslStartDocument;
            this.connection = connection;
            this.callback = callback;
        }

        @Override
        public void onResult(final BsonDocument result, final Throwable t) {
            if (t != null) {
                callback.onResult(null, wrapInMongoSecurityException(t));
                disposeOfSaslClient(saslClient);
            } else if (result.getBoolean("done").getValue()) {
                callback.onResult(null, null);
                disposeOfSaslClient(saslClient);
            } else {
                continueConversation(result);
            }
        }

        public void start() {
            continueConversation(saslStartDocument);
        }

        private void continueConversation(final BsonDocument result) {
            try {
                doAsSubject(new PrivilegedAction<Void>() {
                    @Override
                    public Void run() {
                        try {
                            sendSaslContinueAsync(saslStartDocument.getInt32("conversationId"),
                                    saslClient.evaluateChallenge((result.getBinary("payload")).getData()), connection, Continuator.this);
                        } catch (SaslException e) {
                            throw wrapInMongoSecurityException(e);
                        }
                        return null;
                    }
                });

            } catch (Throwable t) {
                callback.onResult(null, t);
                disposeOfSaslClient(saslClient);
            }
        }

    }

}

