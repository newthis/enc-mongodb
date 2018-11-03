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

import com.mongodb2.MongoClientException;
import com.mongodb2.MongoNamespace;
import com.mongodb2.ReadConcern;
import com.mongodb2.ServerAddress;
import com.mongodb2.WriteConcern;
import com.mongodb2.async.AsyncBatchCursor;
import com.mongodb2.async.SingleResultCallback;
import com.mongodb2.binding.AsyncConnectionSource;
import com.mongodb2.binding.AsyncReadBinding;
import com.mongodb2.binding.AsyncWriteBinding;
import com.mongodb2.binding.ConnectionSource;
import com.mongodb2.binding.ReadBinding;
import com.mongodb2.binding.ReferenceCounted;
import com.mongodb2.binding.WriteBinding;
import com.mongodb2.bulk.DeleteRequest;
import com.mongodb2.bulk.IndexRequest;
import com.mongodb2.bulk.UpdateRequest;
import com.mongodb2.bulk.WriteRequest;
import com.mongodb2.client.model.Collation;
import com.mongodb2.connection.AsyncConnection;
import com.mongodb2.connection.Connection;
import com.mongodb2.connection.ConnectionDescription;
import com.mongodb2.connection.QueryResult;
import com.mongodb2.connection.ServerVersion;
import com.mongodb2.diagnostics.logging.Logger;
import com.mongodb2.diagnostics.logging.Loggers;
import org.bson2.BsonDocument;
import org.bson2.BsonInt64;
import org.bson2.codecs.Decoder;

import java.util.Collections;
import java.util.List;

import static com.mongodb2.assertions.Assertions.notNull;
import static com.mongodb2.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

final class OperationHelper {
    public static final Logger LOGGER = Loggers.getLogger("operation");

    interface CallableWithConnection<T> {
        T call(Connection connection);
    }

    interface CallableWithConnectionAndSource<T> {
        T call(ConnectionSource source, Connection connection);
    }

    interface AsyncCallableWithConnection {
        void call(AsyncConnection connection, Throwable t);
    }

    interface AsyncCallableWithConnectionAndSource {
        void call(AsyncConnectionSource source, AsyncConnection connection, Throwable t);
    }

    static void validateReadConcern(final Connection connection, final ReadConcern readConcern) {
        if (!serverIsAtLeastVersionThreeDotTwo(connection.getDescription()) && !readConcern.isServerDefault()) {
            throw new IllegalArgumentException(format("ReadConcern not supported by server version: %s",
                    connection.getDescription().getServerVersion()));
        }
    }

    static void validateReadConcern(final AsyncConnection connection, final ReadConcern readConcern,
                                    final AsyncCallableWithConnection callable) {
        Throwable throwable = null;
        if (!serverIsAtLeastVersionThreeDotTwo(connection.getDescription()) && !readConcern.isServerDefault()) {
            throwable = new IllegalArgumentException(format("ReadConcern not supported by server version: %s",
                    connection.getDescription().getServerVersion()));
        }
        callable.call(connection, throwable);
    }

    static void validateReadConcern(final AsyncConnectionSource source, final AsyncConnection connection, final ReadConcern readConcern,
                                    final AsyncCallableWithConnectionAndSource callable) {
        validateReadConcern(connection, readConcern, new AsyncCallableWithConnection(){
            @Override
            public void call(final AsyncConnection connection, final Throwable t) {
                callable.call(source, connection, t);
            }
        });
    }

    static void validateCollation(final Connection connection, final Collation collation) {
        if (!serverIsAtLeastVersionThreeDotFour(connection.getDescription()) && collation != null) {
            throw new IllegalArgumentException(format("Collation not supported by server version: %s",
                    connection.getDescription().getServerVersion()));
        }
    }

    static void validateCollationAndWriteConcern(final Connection connection, final Collation collation,
                                                 final WriteConcern writeConcern) {
        if (!serverIsAtLeastVersionThreeDotFour(connection.getDescription()) && collation != null) {
            throw new IllegalArgumentException(format("Collation not supported by server version: %s",
                    connection.getDescription().getServerVersion()));
        } else if (collation != null && !writeConcern.isAcknowledged()) {
            throw new MongoClientException("Specifying collation with an unacknowledged WriteConcern is not supported");
        }
    }

    static void validateCollation(final AsyncConnection connection, final Collation collation,
                                  final AsyncCallableWithConnection callable) {
        Throwable throwable = null;
        if (!serverIsAtLeastVersionThreeDotFour(connection.getDescription()) && collation != null) {
            throwable = new IllegalArgumentException(format("Collation not supported by server version: %s",
                    connection.getDescription().getServerVersion()));
        }
        callable.call(connection, throwable);
    }

    static void validateCollationAndWriteConcern(final AsyncConnection connection, final Collation collation,
                                                 final WriteConcern writeConcern, final AsyncCallableWithConnection callable) {
        Throwable throwable = null;
        if (!serverIsAtLeastVersionThreeDotFour(connection.getDescription()) && collation != null) {
            throwable = new IllegalArgumentException(format("Collation not supported by server version: %s",
                    connection.getDescription().getServerVersion()));
        } else if (collation != null && !writeConcern.isAcknowledged()) {
            throwable = new MongoClientException("Specifying collation with an unacknowledged WriteConcern is not supported");
        }
        callable.call(connection, throwable);
    }

    static void validateCollation(final AsyncConnectionSource source, final AsyncConnection connection,
                                  final Collation collation, final AsyncCallableWithConnectionAndSource callable) {
        validateCollation(connection, collation, new AsyncCallableWithConnection(){
            @Override
            public void call(final AsyncConnection connection, final Throwable t) {
                callable.call(source, connection, t);
            }
        });
    }

    static void validateWriteRequestCollations(final Connection connection, final List<? extends WriteRequest> requests,
                                                 final WriteConcern writeConcern) {
        Collation collation = null;
        for (WriteRequest request : requests) {
            if (request instanceof UpdateRequest) {
                collation = ((UpdateRequest) request).getCollation();
            } else if (request instanceof DeleteRequest) {
                collation = ((DeleteRequest) request).getCollation();
            }
            if (collation != null) {
                break;
            }
        }
        validateCollationAndWriteConcern(connection, collation, writeConcern);
    }

    static void validateWriteRequestCollations(final AsyncConnection connection, final List<? extends WriteRequest> requests,
                                                 final WriteConcern writeConcern, final AsyncCallableWithConnection callable) {
        Collation collation = null;
        for (WriteRequest request : requests) {
            if (request instanceof UpdateRequest) {
                collation = ((UpdateRequest) request).getCollation();
            } else if (request instanceof DeleteRequest) {
                collation = ((DeleteRequest) request).getCollation();
            }
            if (collation != null) {
                break;
            }
        }
        validateCollationAndWriteConcern(connection, collation, writeConcern, new AsyncCallableWithConnection() {
            @Override
            public void call(final AsyncConnection connection, final Throwable t) {
                callable.call(connection, t);
            }
        });
    }

    static void validateWriteRequests(final Connection connection, final Boolean bypassDocumentValidation,
                                        final List<? extends WriteRequest> requests, final WriteConcern writeConcern) {
        checkBypassDocumentValidationIsSupported(connection, bypassDocumentValidation, writeConcern);
        validateWriteRequestCollations(connection, requests, writeConcern);
    }

    static void validateWriteRequests(final AsyncConnection connection, final Boolean bypassDocumentValidation,
                                        final List<? extends WriteRequest> requests, final WriteConcern writeConcern,
                                        final AsyncCallableWithConnection callable) {
        checkBypassDocumentValidationIsSupported(connection, bypassDocumentValidation, writeConcern, new AsyncCallableWithConnection() {
            @Override
            public void call(final AsyncConnection connection, final Throwable t) {
                if (t != null) {
                    callable.call(connection, t);
                } else {
                    validateWriteRequestCollations(connection, requests, writeConcern, callable);
                }
            }
        });
    }

    static void validateIndexRequestCollations(final Connection connection, final List<IndexRequest> requests) {
        for (IndexRequest request : requests) {
            if (request.getCollation() != null) {
                validateCollation(connection, request.getCollation());
                break;
            }
        }
    }

    static void validateIndexRequestCollations(final AsyncConnection connection, final List<IndexRequest> requests,
                                                 final AsyncCallableWithConnection callable) {
        boolean calledTheCallable = false;
        for (IndexRequest request : requests) {
            if (request.getCollation() != null) {
                calledTheCallable = true;
                validateCollation(connection, request.getCollation(), new AsyncCallableWithConnection() {
                    @Override
                    public void call(final AsyncConnection connection, final Throwable t) {
                        callable.call(connection, t);
                    }
                });
                break;
            }
        }
        if (!calledTheCallable) {
            callable.call(connection, null);
        }
    }

    static void validateReadConcernAndCollation(final Connection connection, final ReadConcern readConcern,
                                                  final Collation collation) {
        validateReadConcern(connection, readConcern);
        validateCollation(connection, collation);
    }

    static void validateReadConcernAndCollation(final AsyncConnection connection, final ReadConcern readConcern,
                                                  final Collation collation,
                                                  final AsyncCallableWithConnection callable) {
        validateReadConcern(connection, readConcern, new AsyncCallableWithConnection(){
            @Override
            public void call(final AsyncConnection connection, final Throwable t) {
                if (t != null) {
                    callable.call(connection, t);
                } else {
                    validateCollation(connection, collation, callable);
                }
            }
        });
    }

    static void validateReadConcernAndCollation(final AsyncConnectionSource source, final AsyncConnection connection,
                                                  final ReadConcern readConcern, final Collation collation,
                                                  final AsyncCallableWithConnectionAndSource callable) {
        validateReadConcernAndCollation(connection, readConcern, collation, new AsyncCallableWithConnection(){
            @Override
            public void call(final AsyncConnection connection, final Throwable t) {
                callable.call(source, connection, t);
            }
        });
    }

    static void checkBypassDocumentValidationIsSupported(final Connection connection, final Boolean bypassDocumentValidation,
                                             final WriteConcern writeConcern) {
        if (bypassDocumentValidation != null && serverIsAtLeastVersionThreeDotTwo(connection.getDescription())
                && !writeConcern.isAcknowledged()) {
            throw new MongoClientException("Specifying bypassDocumentValidation with an unacknowledged WriteConcern is not supported");
        }
    }

    static void checkBypassDocumentValidationIsSupported(final AsyncConnection connection, final Boolean bypassDocumentValidation,
                                                         final WriteConcern writeConcern, final AsyncCallableWithConnection callable) {
        Throwable throwable = null;
        if (bypassDocumentValidation != null && serverIsAtLeastVersionThreeDotTwo(connection.getDescription())
                && !writeConcern.isAcknowledged()) {
            throwable = new MongoClientException("Specifying bypassDocumentValidation with an unacknowledged WriteConcern is "
                    + "not supported");
        }
        callable.call(connection, throwable);
    }


    static <T> QueryBatchCursor<T> createEmptyBatchCursor(final MongoNamespace namespace, final Decoder<T> decoder,
                                                          final ServerAddress serverAddress, final int batchSize) {
        return new QueryBatchCursor<T>(new QueryResult<T>(namespace, Collections.<T>emptyList(), 0L,
                                                          serverAddress),
                                       0, batchSize, decoder);
    }

    static <T> AsyncBatchCursor<T> createEmptyAsyncBatchCursor(final MongoNamespace namespace, final Decoder<T> decoder,
                                                               final ServerAddress serverAddress, final int batchSize) {
        return new AsyncQueryBatchCursor<T>(new QueryResult<T>(namespace, Collections.<T>emptyList(), 0L, serverAddress), 0, batchSize,
                decoder);
    }

    static <T> BatchCursor<T> cursorDocumentToBatchCursor(final BsonDocument cursorDocument, final Decoder<T> decoder,
                                                          final ConnectionSource source, final int batchSize) {
        return new QueryBatchCursor<T>(OperationHelper.<T>cursorDocumentToQueryResult(cursorDocument,
                                                                                      source.getServerDescription().getAddress()),
                                       0, batchSize, decoder, source);
    }

    static <T> AsyncBatchCursor<T> cursorDocumentToAsyncBatchCursor(final BsonDocument cursorDocument, final Decoder<T> decoder,
                                                                    final AsyncConnectionSource source, final AsyncConnection connection,
                                                                    final int batchSize) {
        return new AsyncQueryBatchCursor<T>(OperationHelper.<T>cursorDocumentToQueryResult(cursorDocument,
                                                                                           source.getServerDescription().getAddress()),
                                            0, batchSize, 0, decoder, source, connection);
    }


    static <T> QueryResult<T> cursorDocumentToQueryResult(final BsonDocument cursorDocument, final ServerAddress serverAddress) {
        return cursorDocumentToQueryResult(cursorDocument, serverAddress, "firstBatch");
    }

    static <T> QueryResult<T> getMoreCursorDocumentToQueryResult(final BsonDocument cursorDocument, final ServerAddress serverAddress) {
        return cursorDocumentToQueryResult(cursorDocument, serverAddress, "nextBatch");
    }

    private static <T> QueryResult<T> cursorDocumentToQueryResult(final BsonDocument cursorDocument, final ServerAddress serverAddress,
                                                          final String fieldNameContainingBatch) {
        long cursorId = ((BsonInt64) cursorDocument.get("id")).getValue();
        MongoNamespace queryResultNamespace = new MongoNamespace(cursorDocument.getString("ns").getValue());
        return new QueryResult<T>(queryResultNamespace, BsonDocumentWrapperHelper.<T>toList(cursorDocument, fieldNameContainingBatch),
                                  cursorId, serverAddress);
    }

    static <T> SingleResultCallback<T> releasingCallback(final SingleResultCallback<T> wrapped, final AsyncConnection connection) {
        return new ReferenceCountedReleasingWrappedCallback<T>(wrapped, singletonList(connection));
    }

    static <T> SingleResultCallback<T> releasingCallback(final SingleResultCallback<T> wrapped, final AsyncConnectionSource source,
                                                         final AsyncConnection connection) {
        return new ReferenceCountedReleasingWrappedCallback<T>(wrapped, asList(connection, source));
    }

    static <T> SingleResultCallback<T> releasingCallback(final SingleResultCallback<T> wrapped,
                                                         final AsyncReadBinding readBinding,
                                                         final AsyncConnectionSource source,
                                                         final AsyncConnection connection) {
        return new ReferenceCountedReleasingWrappedCallback<T>(wrapped, asList(readBinding, connection, source));
    }

    private static class ReferenceCountedReleasingWrappedCallback<T> implements SingleResultCallback<T> {
        private final SingleResultCallback<T> wrapped;
        private final List<? extends ReferenceCounted> referenceCounted;

        ReferenceCountedReleasingWrappedCallback(final SingleResultCallback<T> wrapped,
                                                 final List<? extends ReferenceCounted> referenceCounted) {
            this.wrapped = wrapped;
            this.referenceCounted = notNull("referenceCounted", referenceCounted);
        }

        @Override
        public void onResult(final T result, final Throwable t) {
            for (ReferenceCounted cur : referenceCounted) {
                cur.release();
            }
            wrapped.onResult(result, t);
        }
    }

    static boolean serverIsAtLeastVersionTwoDotSix(final ConnectionDescription description) {
        return serverIsAtLeastVersion(description, new ServerVersion(2, 6));
    }

    static boolean serverIsAtLeastVersionThreeDotZero(final ConnectionDescription description) {
        return serverIsAtLeastVersion(description, new ServerVersion(3, 0));
    }

    static boolean serverIsAtLeastVersionThreeDotTwo(final ConnectionDescription description) {
        return serverIsAtLeastVersion(description, new ServerVersion(3, 2));
    }

    static boolean serverIsAtLeastVersionThreeDotFour(final ConnectionDescription description) {
        return serverIsAtLeastVersion(description, new ServerVersion(3, 4));
    }

    static boolean serverIsAtLeastVersionThreeDotSix(final ConnectionDescription description) {
        return serverIsAtLeastVersion(description, new ServerVersion(3, 5));
    }

    static boolean serverIsAtLeastVersion(final ConnectionDescription description, final ServerVersion serverVersion) {
        return description.getServerVersion().compareTo(serverVersion) >= 0;
    }

    static <T> T withConnection(final ReadBinding binding, final CallableWithConnection<T> callable) {
        ConnectionSource source = binding.getReadConnectionSource();
        try {
            return withConnectionSource(source, callable);
        } finally {
            source.release();
        }
    }

    static <T> T withConnection(final ReadBinding binding, final CallableWithConnectionAndSource<T> callable) {
        ConnectionSource source = binding.getReadConnectionSource();
        try {
            return withConnectionSource(source, callable);
        } finally {
            source.release();
        }
    }

    static <T> T withConnection(final WriteBinding binding, final CallableWithConnection<T> callable) {
        ConnectionSource source = binding.getWriteConnectionSource();
        try {
            return withConnectionSource(source, callable);
        } finally {
            source.release();
        }
    }

    static <T> T withConnectionSource(final ConnectionSource source, final CallableWithConnection<T> callable) {
        Connection connection = source.getConnection();
        try {
            return callable.call(connection);
        } finally {
            connection.release();
        }
    }

    static <T> T withConnectionSource(final ConnectionSource source, final CallableWithConnectionAndSource<T> callable) {
        Connection connection = source.getConnection();
        try {
            return callable.call(source, connection);
        } finally {
            connection.release();
        }
    }

    static void withConnection(final AsyncWriteBinding binding, final AsyncCallableWithConnection callable) {
        binding.getWriteConnectionSource(errorHandlingCallback(new AsyncCallableWithConnectionCallback(callable), LOGGER));
    }

    static void withConnection(final AsyncReadBinding binding, final AsyncCallableWithConnection callable) {
        binding.getReadConnectionSource(errorHandlingCallback(new AsyncCallableWithConnectionCallback(callable), LOGGER));
    }

    static void withConnection(final AsyncReadBinding binding, final AsyncCallableWithConnectionAndSource callable) {
        binding.getReadConnectionSource(errorHandlingCallback(new AsyncCallableWithConnectionAndSourceCallback(callable), LOGGER));
    }

    private static class AsyncCallableWithConnectionCallback implements SingleResultCallback<AsyncConnectionSource> {
        private final AsyncCallableWithConnection callable;
        public AsyncCallableWithConnectionCallback(final AsyncCallableWithConnection callable) {
            this.callable = callable;
        }
        @Override
        public void onResult(final AsyncConnectionSource source, final Throwable t) {
            if (t != null) {
                callable.call(null, t);
            } else {
                withConnectionSource(source, callable);
            }
        }
    }

    private static void withConnectionSource(final AsyncConnectionSource source, final AsyncCallableWithConnection callable) {
        source.getConnection(new SingleResultCallback<AsyncConnection>() {
            @Override
            public void onResult(final AsyncConnection connection, final Throwable t) {
                source.release();
                if (t != null) {
                    callable.call(null, t);
                } else {
                    callable.call(connection, null);
                }
            }
        });
    }

    private static void withConnectionSource(final AsyncConnectionSource source, final AsyncCallableWithConnectionAndSource callable) {
        source.getConnection(new SingleResultCallback<AsyncConnection>() {
            @Override
            public void onResult(final AsyncConnection result, final Throwable t) {
                callable.call(source, result, t);
            }
        });
    }

    private static class AsyncCallableWithConnectionAndSourceCallback implements SingleResultCallback<AsyncConnectionSource> {
        private final AsyncCallableWithConnectionAndSource callable;

        public AsyncCallableWithConnectionAndSourceCallback(final AsyncCallableWithConnectionAndSource callable) {
            this.callable = callable;
        }

        @Override
        public void onResult(final AsyncConnectionSource source, final Throwable t) {
            if (t != null) {
                callable.call(null, null, t);
            } else {
                withConnectionSource(source, callable);
            }
        }
    }

    private OperationHelper() {
    }
}
