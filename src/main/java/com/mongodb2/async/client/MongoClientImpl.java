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

package com.mongodb2.async.client;

import com.mongodb2.Function;
import com.mongodb2.ReadPreference;
import com.mongodb2.async.SingleResultCallback;
import com.mongodb2.binding.AsyncClusterBinding;
import com.mongodb2.binding.AsyncReadBinding;
import com.mongodb2.binding.AsyncReadWriteBinding;
import com.mongodb2.binding.AsyncWriteBinding;
import com.mongodb2.connection.Cluster;
import com.mongodb2.diagnostics.logging.Logger;
import com.mongodb2.diagnostics.logging.Loggers;
import com.mongodb2.operation.AsyncOperationExecutor;
import com.mongodb2.operation.AsyncReadOperation;
import com.mongodb2.operation.AsyncWriteOperation;
import org.bson2.BsonDocument;
import org.bson2.Document;

import java.io.Closeable;
import java.io.IOException;

import static com.mongodb2.assertions.Assertions.notNull;
import static com.mongodb2.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;

class MongoClientImpl implements MongoClient {
    private static final Logger LOGGER = Loggers.getLogger("client");
    private final Cluster cluster;
    private final MongoClientSettings settings;
    private final AsyncOperationExecutor executor;
    private final Closeable externalResourceCloser;

    MongoClientImpl(final MongoClientSettings settings, final Cluster cluster, final Closeable externalResourceCloser) {
        this(settings, cluster, createOperationExecutor(settings, cluster), externalResourceCloser);
    }

    MongoClientImpl(final MongoClientSettings settings, final Cluster cluster, final AsyncOperationExecutor executor) {
        this(settings, cluster, executor, null);
    }

    MongoClientImpl(final MongoClientSettings settings, final Cluster cluster, final AsyncOperationExecutor executor,
                    final Closeable externalResourceCloser) {
        this.settings = notNull("settings", settings);
        this.cluster = notNull("cluster", cluster);
        this.executor = notNull("executor", executor);
        this.externalResourceCloser = externalResourceCloser;
    }

    @Override
    public MongoDatabase getDatabase(final String name) {
        return new MongoDatabaseImpl(name, settings.getCodecRegistry(), settings.getReadPreference(), settings.getWriteConcern(),
                settings.getReadConcern(), executor);
    }

    @Override
    public void close() {
        cluster.close();
        if (externalResourceCloser != null) {
            try {
                externalResourceCloser.close();
            } catch (IOException e) {
                LOGGER.warn("Exception closing resource", e);
            }
        }

    }

    @Override
    public MongoClientSettings getSettings() {
        return settings;
    }

    @Override
    public MongoIterable<String> listDatabaseNames() {
        return new ListDatabasesIterableImpl<BsonDocument>(BsonDocument.class, MongoClients.getDefaultCodecRegistry(),
                                                           ReadPreference.primary(), executor).map(new Function<BsonDocument, String>() {
            @Override
            public String apply(final BsonDocument document) {
                return document.getString("name").getValue();
            }
        });
    }

    @Override
    public ListDatabasesIterable<Document> listDatabases() {
        return listDatabases(Document.class);
    }

    @Override
    public <T> ListDatabasesIterable<T> listDatabases(final Class<T> resultClass) {
        return new ListDatabasesIterableImpl<T>(resultClass, settings.getCodecRegistry(), ReadPreference.primary(), executor);
    }

    Cluster getCluster() {
        return cluster;
    }

    private static AsyncOperationExecutor createOperationExecutor(final MongoClientSettings settings, final Cluster cluster) {
        return new AsyncOperationExecutor(){
            @Override
            public <T> void execute(final AsyncReadOperation<T> operation, final ReadPreference readPreference,
                                    final SingleResultCallback<T> callback) {
                notNull("operation", operation);
                notNull("readPreference", readPreference);
                notNull("callback", callback);
                final SingleResultCallback<T> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
                final AsyncReadBinding binding = getReadWriteBinding(readPreference, cluster);
                operation.executeAsync(binding, new SingleResultCallback<T>() {
                    @Override
                    public void onResult(final T result, final Throwable t) {
                        try {
                            errHandlingCallback.onResult(result, t);
                        } finally {
                            binding.release();
                        }
                    }
                });
            }

            @Override
            public <T> void execute(final AsyncWriteOperation<T> operation, final SingleResultCallback<T> callback) {
                notNull("operation", operation);
                notNull("callback", callback);
                final AsyncWriteBinding binding = getReadWriteBinding(ReadPreference.primary(), cluster);
                operation.executeAsync(binding, new SingleResultCallback<T>() {
                    @Override
                    public void onResult(final T result, final Throwable t) {
                        try {
                            errorHandlingCallback(callback, LOGGER).onResult(result, t);
                        } finally {
                            binding.release();
                        }
                    }
                });
            }
        };
    }

    private static AsyncReadWriteBinding getReadWriteBinding(final ReadPreference readPreference, final Cluster cluster) {
        notNull("readPreference", readPreference);
        return new AsyncClusterBinding(cluster, readPreference);
    }
}
