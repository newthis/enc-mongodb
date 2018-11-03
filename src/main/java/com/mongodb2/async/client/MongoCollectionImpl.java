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

package com.mongodb2.async.client;

import com.mongodb2.MongoBulkWriteException;
import com.mongodb2.MongoInternalException;
import com.mongodb2.MongoNamespace;
import com.mongodb2.MongoWriteConcernException;
import com.mongodb2.MongoWriteException;
import com.mongodb2.ReadConcern;
import com.mongodb2.ReadPreference;
import com.mongodb2.WriteConcern;
import com.mongodb2.WriteConcernResult;
import com.mongodb2.WriteError;
import com.mongodb2.async.SingleResultCallback;
import com.mongodb2.bulk.BulkWriteResult;
import com.mongodb2.bulk.DeleteRequest;
import com.mongodb2.bulk.IndexRequest;
import com.mongodb2.bulk.InsertRequest;
import com.mongodb2.bulk.UpdateRequest;
import com.mongodb2.bulk.WriteRequest;
import com.mongodb2.client.model.BulkWriteOptions;
import com.mongodb2.client.model.CountOptions;
import com.mongodb2.client.model.DeleteManyModel;
import com.mongodb2.client.model.DeleteOneModel;
import com.mongodb2.client.model.DeleteOptions;
import com.mongodb2.client.model.FindOneAndDeleteOptions;
import com.mongodb2.client.model.FindOneAndReplaceOptions;
import com.mongodb2.client.model.FindOneAndUpdateOptions;
import com.mongodb2.client.model.FindOptions;
import com.mongodb2.client.model.IndexModel;
import com.mongodb2.client.model.IndexOptions;
import com.mongodb2.client.model.InsertManyOptions;
import com.mongodb2.client.model.InsertOneModel;
import com.mongodb2.client.model.InsertOneOptions;
import com.mongodb2.client.model.RenameCollectionOptions;
import com.mongodb2.client.model.ReplaceOneModel;
import com.mongodb2.client.model.ReturnDocument;
import com.mongodb2.client.model.UpdateManyModel;
import com.mongodb2.client.model.UpdateOneModel;
import com.mongodb2.client.model.UpdateOptions;
import com.mongodb2.client.model.WriteModel;
import com.mongodb2.client.result.DeleteResult;
import com.mongodb2.client.result.UpdateResult;
import com.mongodb2.diagnostics.logging.Logger;
import com.mongodb2.diagnostics.logging.Loggers;
import com.mongodb2.operation.AsyncOperationExecutor;
import com.mongodb2.operation.CountOperation;
import com.mongodb2.operation.CreateIndexesOperation;
import com.mongodb2.operation.DropCollectionOperation;
import com.mongodb2.operation.DropIndexOperation;
import com.mongodb2.operation.FindAndDeleteOperation;
import com.mongodb2.operation.FindAndReplaceOperation;
import com.mongodb2.operation.FindAndUpdateOperation;
import com.mongodb2.operation.MixedBulkWriteOperation;
import com.mongodb2.operation.RenameCollectionOperation;
import org.bson2.BsonDocument;
import org.bson2.BsonDocumentWrapper;
import org.bson2.BsonString;
import org.bson2.BsonValue;
import org.bson2.Document;
import org.bson2.codecs.Codec;
import org.bson2.codecs.CollectibleCodec;
import org.bson2.codecs.configuration.CodecRegistry;
import org.bson2.conversions.Bson;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb2.assertions.Assertions.notNull;
import static com.mongodb2.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class MongoCollectionImpl<TDocument> implements MongoCollection<TDocument> {
    private static final Logger LOGGER = Loggers.getLogger("client");
    private final MongoNamespace namespace;
    private final Class<TDocument> documentClass;
    private final ReadPreference readPreference;
    private final CodecRegistry codecRegistry;
    private final WriteConcern writeConcern;
    private final ReadConcern readConcern;
    private final AsyncOperationExecutor executor;

    MongoCollectionImpl(final MongoNamespace namespace, final Class<TDocument> documentClass, final CodecRegistry codecRegistry,
                        final ReadPreference readPreference, final WriteConcern writeConcern, final ReadConcern readConcern,
                        final AsyncOperationExecutor executor) {
        this.namespace = notNull("namespace", namespace);
        this.documentClass = notNull("documentClass", documentClass);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.readPreference = notNull("readPreference", readPreference);
        this.writeConcern = notNull("writeConcern", writeConcern);
        this.readConcern = notNull("readConcern", readConcern);
        this.executor = notNull("executor", executor);
    }

    @Override
    public MongoNamespace getNamespace() {
        return namespace;
    }

    @Override
    public Class<TDocument> getDocumentClass() {
        return documentClass;
    }

    @Override
    public CodecRegistry getCodecRegistry() {
        return codecRegistry;
    }

    @Override
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    @Override
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    @Override
    public ReadConcern getReadConcern() {
        return readConcern;
    }

    @Override
    public <NewTDocument> MongoCollection<NewTDocument> withDocumentClass(final Class<NewTDocument> newDocumentClass) {
        return new MongoCollectionImpl<NewTDocument>(namespace, newDocumentClass, codecRegistry, readPreference, writeConcern, readConcern,
                executor);
    }

    @Override
    public MongoCollection<TDocument> withCodecRegistry(final CodecRegistry codecRegistry) {
        return new MongoCollectionImpl<TDocument>(namespace, documentClass, codecRegistry, readPreference, writeConcern, readConcern,
                executor);
    }

    @Override
    public MongoCollection<TDocument> withReadPreference(final ReadPreference readPreference) {
        return new MongoCollectionImpl<TDocument>(namespace, documentClass, codecRegistry, readPreference, writeConcern, readConcern,
                executor);
    }

    @Override
    public MongoCollection<TDocument> withWriteConcern(final WriteConcern writeConcern) {
        return new MongoCollectionImpl<TDocument>(namespace, documentClass, codecRegistry, readPreference, writeConcern, readConcern,
                executor);
    }

    @Override
    public MongoCollection<TDocument> withReadConcern(final ReadConcern readConcern) {
        return new MongoCollectionImpl<TDocument>(namespace, documentClass, codecRegistry, readPreference, writeConcern, readConcern,
                executor);
    }

    @Override
    public void count(final SingleResultCallback<Long> callback) {
        count(new BsonDocument(), new CountOptions(), callback);
    }

    @Override
    public void count(final Bson filter, final SingleResultCallback<Long> callback) {
        count(filter, new CountOptions(), callback);
    }

    @Override
    public void count(final Bson filter, final CountOptions options, final SingleResultCallback<Long> callback) {
        CountOperation operation = new CountOperation(namespace)
                                   .filter(toBsonDocument(filter))
                                   .skip(options.getSkip())
                                   .limit(options.getLimit())
                                   .maxTime(options.getMaxTime(MILLISECONDS), MILLISECONDS)
                                   .collation(options.getCollation());
        if (options.getHint() != null) {
            operation.hint(toBsonDocument(options.getHint()));
        } else if (options.getHintString() != null) {
            operation.hint(new BsonString(options.getHintString()));
        }
        executor.execute(operation, readPreference, callback);
    }

    @Override
    public <TResult> DistinctIterable<TResult> distinct(final String fieldName, final Class<TResult> resultClass) {
        return distinct(fieldName, new BsonDocument(), resultClass);
    }

    @Override
    public <TResult> DistinctIterable<TResult> distinct(final String fieldName, final Bson filter, final Class<TResult> resultClass) {
        return new DistinctIterableImpl<TDocument, TResult>(namespace, documentClass, resultClass, codecRegistry, readPreference,
                readConcern, executor, fieldName, filter);
    }

    @Override
    public FindIterable<TDocument> find() {
        return find(new BsonDocument(), documentClass);
    }

    @Override
    public <TResult> FindIterable<TResult> find(final Class<TResult> resultClass) {
        return find(new BsonDocument(), resultClass);
    }

    @Override
    public FindIterable<TDocument> find(final Bson filter) {
        return find(filter, documentClass);
    }

    @Override
    public <TResult> FindIterable<TResult> find(final Bson filter, final Class<TResult> resultClass) {
        return new FindIterableImpl<TDocument, TResult>(namespace, documentClass, resultClass, codecRegistry, readPreference, readConcern,
                executor, filter, new FindOptions());
    }

    @Override
    public AggregateIterable<TDocument> aggregate(final List<? extends Bson> pipeline) {
        return aggregate(pipeline, documentClass);
    }

    @Override
    public <TResult> AggregateIterable<TResult> aggregate(final List<? extends Bson> pipeline, final Class<TResult> resultClass) {
        return new AggregateIterableImpl<TDocument, TResult>(namespace, documentClass, resultClass, codecRegistry, readPreference,
                readConcern, writeConcern, executor, pipeline);
    }

    @Override
    public MapReduceIterable<TDocument> mapReduce(final String mapFunction, final String reduceFunction) {
        return mapReduce(mapFunction, reduceFunction, documentClass);
    }

    @Override
    public <TResult> MapReduceIterable<TResult> mapReduce(final String mapFunction, final String reduceFunction,
                                                          final Class<TResult> resultClass) {
        return new MapReduceIterableImpl<TDocument, TResult>(namespace, documentClass, resultClass, codecRegistry, readPreference,
                readConcern, writeConcern, executor, mapFunction, reduceFunction);
    }

    @Override
    public void bulkWrite(final List<? extends WriteModel<? extends TDocument>> requests,
                          final SingleResultCallback<BulkWriteResult> callback) {
        bulkWrite(requests, new BulkWriteOptions(), callback);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void bulkWrite(final List<? extends WriteModel<? extends TDocument>> requests, final BulkWriteOptions options,
                          final SingleResultCallback<BulkWriteResult> callback) {
        notNull("requests", requests);
        List<WriteRequest> writeRequests = new ArrayList<WriteRequest>(requests.size());
        for (WriteModel<? extends TDocument> writeModel : requests) {
            WriteRequest writeRequest;
            if (writeModel == null) {
                throw new IllegalArgumentException("requests can not contain a null value");
            } else if (writeModel instanceof InsertOneModel) {
                TDocument document = ((InsertOneModel<TDocument>) writeModel).getDocument();
                if (getCodec() instanceof CollectibleCodec) {
                    document = ((CollectibleCodec<TDocument>) getCodec()).generateIdIfAbsentFromDocument(document);
                }
                writeRequest = new InsertRequest(documentToBsonDocument(document));
            } else if (writeModel instanceof ReplaceOneModel) {
                ReplaceOneModel<TDocument> replaceOneModel = (ReplaceOneModel<TDocument>) writeModel;
                writeRequest = new UpdateRequest(toBsonDocument(replaceOneModel.getFilter()), documentToBsonDocument(replaceOneModel
                        .getReplacement()),
                        WriteRequest.Type.REPLACE)
                        .upsert(replaceOneModel.getOptions().isUpsert())
                        .collation(replaceOneModel.getOptions().getCollation());
            } else if (writeModel instanceof UpdateOneModel) {
                UpdateOneModel<TDocument> updateOneModel = (UpdateOneModel<TDocument>) writeModel;
                writeRequest = new UpdateRequest(toBsonDocument(updateOneModel.getFilter()), toBsonDocument(updateOneModel.getUpdate()),
                        WriteRequest.Type.UPDATE)
                        .multi(false)
                        .upsert(updateOneModel.getOptions().isUpsert())
                        .collation(updateOneModel.getOptions().getCollation());
            } else if (writeModel instanceof UpdateManyModel) {
                UpdateManyModel<TDocument> updateManyModel = (UpdateManyModel<TDocument>) writeModel;
                writeRequest = new UpdateRequest(toBsonDocument(updateManyModel.getFilter()), toBsonDocument(updateManyModel.getUpdate()),
                        WriteRequest.Type.UPDATE)
                        .multi(true)
                        .upsert(updateManyModel.getOptions().isUpsert())
                        .collation(updateManyModel.getOptions().getCollation());
            } else if (writeModel instanceof DeleteOneModel) {
                DeleteOneModel<TDocument> deleteOneModel = (DeleteOneModel<TDocument>) writeModel;
                writeRequest = new DeleteRequest(toBsonDocument(deleteOneModel.getFilter())).multi(false)
                        .collation(deleteOneModel.getOptions().getCollation());
            } else if (writeModel instanceof DeleteManyModel) {
                DeleteManyModel<TDocument> deleteManyModel = (DeleteManyModel<TDocument>) writeModel;
                writeRequest = new DeleteRequest(toBsonDocument(deleteManyModel.getFilter())).multi(true)
                        .collation(deleteManyModel.getOptions().getCollation());
            } else {
                throw new UnsupportedOperationException(format("WriteModel of type %s is not supported", writeModel.getClass()));
            }

            writeRequests.add(writeRequest);
        }

        executor.execute(new MixedBulkWriteOperation(namespace, writeRequests, options.isOrdered(), writeConcern)
                .bypassDocumentValidation(options.getBypassDocumentValidation()), callback);
    }

    @Override
    public void insertOne(final TDocument document, final SingleResultCallback<Void> callback) {
        insertOne(document, new InsertOneOptions(), callback);
    }

    @Override
    public void insertOne(final TDocument document, final InsertOneOptions options, final SingleResultCallback<Void> callback) {
        TDocument insertDocument = document;
        if (getCodec() instanceof CollectibleCodec) {
            insertDocument = ((CollectibleCodec<TDocument>) getCodec()).generateIdIfAbsentFromDocument(insertDocument);
        }
        executeSingleWriteRequest(new InsertRequest(documentToBsonDocument(insertDocument)), options.getBypassDocumentValidation(),
                new SingleResultCallback<BulkWriteResult>() {
                    @Override
                    public void onResult(final BulkWriteResult result, final Throwable t) {
                        callback.onResult(null, t);
                    }
                });
    }

    @Override
    public void insertMany(final List<? extends TDocument> documents, final SingleResultCallback<Void> callback) {
        insertMany(documents, new InsertManyOptions(), callback);
    }

    @Override
    public void insertMany(final List<? extends TDocument> documents, final InsertManyOptions options,
                           final SingleResultCallback<Void> callback) {
        notNull("documents", documents);
        List<InsertRequest> requests = new ArrayList<InsertRequest>(documents.size());
        for (TDocument document : documents) {
            if (document == null) {
                throw new IllegalArgumentException("documents can not contain a null value");
            }
            if (getCodec() instanceof CollectibleCodec) {
                document = ((CollectibleCodec<TDocument>) getCodec()).generateIdIfAbsentFromDocument(document);
            }
            requests.add(new InsertRequest(documentToBsonDocument(document)));
        }
        executor.execute(new MixedBulkWriteOperation(namespace, requests, options.isOrdered(), writeConcern)
                .bypassDocumentValidation(options.getBypassDocumentValidation()), errorHandlingCallback(
                new SingleResultCallback<BulkWriteResult>() {
                    @Override
                    public void onResult(final BulkWriteResult result, final Throwable t) {
                        callback.onResult(null, t);
                    }
                }, LOGGER));
    }

    @Override
    public void deleteOne(final Bson filter, final SingleResultCallback<DeleteResult> callback) {
        deleteOne(filter, new DeleteOptions(), callback);
    }

    @Override
    public void deleteOne(final Bson filter, final DeleteOptions options, final SingleResultCallback<DeleteResult> callback) {
        delete(filter, options, false, callback);
    }

    @Override
    public void deleteMany(final Bson filter, final SingleResultCallback<DeleteResult> callback) {
        deleteMany(filter, new DeleteOptions(), callback);
    }

    @Override
    public void deleteMany(final Bson filter, final DeleteOptions options, final SingleResultCallback<DeleteResult> callback) {
        delete(filter, options, true, callback);
    }

    @Override
    public void replaceOne(final Bson filter, final TDocument replacement, final SingleResultCallback<UpdateResult> callback) {
        replaceOne(filter, replacement, new UpdateOptions(), callback);
    }

    @Override
    public void replaceOne(final Bson filter, final TDocument replacement, final UpdateOptions options,
                           final SingleResultCallback<UpdateResult> callback) {
        executeSingleWriteRequest(new UpdateRequest(toBsonDocument(filter), documentToBsonDocument(replacement), WriteRequest.Type.REPLACE)
                                  .upsert(options.isUpsert()).collation(options.getCollation()), options.getBypassDocumentValidation(),
                                  new SingleResultCallback<BulkWriteResult>() {
                                      @Override
                                      public void onResult(final BulkWriteResult result, final Throwable t) {
                                          if (t != null) {
                                              callback.onResult(null, t);
                                          } else {
                                              callback.onResult(toUpdateResult(result), null);
                                          }
                                      }
                                  });
    }

    @Override
    public void updateOne(final Bson filter, final Bson update, final SingleResultCallback<UpdateResult> callback) {
        updateOne(filter, update, new UpdateOptions(), callback);
    }

    @Override
    public void updateOne(final Bson filter, final Bson update, final UpdateOptions options,
                          final SingleResultCallback<UpdateResult> callback) {
        update(filter, update, options, false, callback);
    }

    @Override
    public void updateMany(final Bson filter, final Bson update, final SingleResultCallback<UpdateResult> callback) {
        updateMany(filter, update, new UpdateOptions(), callback);
    }

    @Override
    public void updateMany(final Bson filter, final Bson update, final UpdateOptions options,
                           final SingleResultCallback<UpdateResult> callback) {
        update(filter, update, options, true, callback);
    }

    @Override
    public void findOneAndDelete(final Bson filter, final SingleResultCallback<TDocument> callback) {
        findOneAndDelete(filter, new FindOneAndDeleteOptions(), callback);
    }

    @Override
    public void findOneAndDelete(final Bson filter, final FindOneAndDeleteOptions options, final SingleResultCallback<TDocument> callback) {
        executor.execute(new FindAndDeleteOperation<TDocument>(namespace, writeConcern, getCodec())
                         .filter(toBsonDocument(filter))
                         .projection(toBsonDocument(options.getProjection()))
                         .sort(toBsonDocument(options.getSort()))
                         .maxTime(options.getMaxTime(MILLISECONDS), MILLISECONDS)
                         .collation(options.getCollation()), callback);
    }

    @Override
    public void findOneAndReplace(final Bson filter, final TDocument replacement, final SingleResultCallback<TDocument> callback) {
        findOneAndReplace(filter, replacement, new FindOneAndReplaceOptions(), callback);
    }

    @Override
    public void findOneAndReplace(final Bson filter, final TDocument replacement, final FindOneAndReplaceOptions options,
                                  final SingleResultCallback<TDocument> callback) {
        executor.execute(new FindAndReplaceOperation<TDocument>(namespace, writeConcern, getCodec(), documentToBsonDocument(replacement))
                         .filter(toBsonDocument(filter))
                         .projection(toBsonDocument(options.getProjection()))
                         .sort(toBsonDocument(options.getSort()))
                         .returnOriginal(options.getReturnDocument() == ReturnDocument.BEFORE)
                         .upsert(options.isUpsert())
                         .maxTime(options.getMaxTime(MILLISECONDS), MILLISECONDS)
                         .bypassDocumentValidation(options.getBypassDocumentValidation())
                         .collation(options.getCollation()), callback);
    }

    @Override
    public void findOneAndUpdate(final Bson filter, final Bson update, final SingleResultCallback<TDocument> callback) {
        findOneAndUpdate(filter, update, new FindOneAndUpdateOptions(), callback);
    }

    @Override
    public void findOneAndUpdate(final Bson filter, final Bson update, final FindOneAndUpdateOptions options,
                                 final SingleResultCallback<TDocument> callback) {
        executor.execute(new FindAndUpdateOperation<TDocument>(namespace, writeConcern, getCodec(), toBsonDocument(update))
                .filter(toBsonDocument(filter))
                .projection(toBsonDocument(options.getProjection()))
                .sort(toBsonDocument(options.getSort()))
                .returnOriginal(options.getReturnDocument() == ReturnDocument.BEFORE)
                .upsert(options.isUpsert())
                .maxTime(options.getMaxTime(MILLISECONDS), MILLISECONDS)
                .bypassDocumentValidation(options.getBypassDocumentValidation())
                .collation(options.getCollation()), callback);
    }

    @Override
    public void drop(final SingleResultCallback<Void> callback) {
        executor.execute(new DropCollectionOperation(namespace, writeConcern), callback);
    }

    @Override
    public void createIndex(final Bson key, final SingleResultCallback<String> callback) {
        createIndex(key, new IndexOptions(), callback);
    }

    @Override
    public void createIndex(final Bson key, final IndexOptions indexOptions, final SingleResultCallback<String> callback) {
        createIndexes(singletonList(new IndexModel(key, indexOptions)), new SingleResultCallback<List<String>>() {
            @Override
            public void onResult(final List<String> result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    callback.onResult(result.get(0), null);
                }
            }
        });
    }

    @Override
    public void createIndexes(final List<IndexModel> indexes, final SingleResultCallback<List<String>> callback) {
        notNull("indexes", indexes);

        List<IndexRequest> indexRequests = new ArrayList<IndexRequest>(indexes.size());
        for (IndexModel model : indexes) {
            if (model == null) {
                throw new IllegalArgumentException("indexes can not contain a null value");
            }
            indexRequests.add(new IndexRequest(toBsonDocument(model.getKeys()))
                              .name(model.getOptions().getName())
                              .background(model.getOptions().isBackground())
                              .unique(model.getOptions().isUnique())
                              .sparse(model.getOptions().isSparse())
                              .expireAfter(model.getOptions().getExpireAfter(TimeUnit.SECONDS), TimeUnit.SECONDS)
                              .version(model.getOptions().getVersion())
                              .weights(toBsonDocument(model.getOptions().getWeights()))
                              .defaultLanguage(model.getOptions().getDefaultLanguage())
                              .languageOverride(model.getOptions().getLanguageOverride())
                              .textVersion(model.getOptions().getTextVersion())
                              .sphereVersion(model.getOptions().getSphereVersion())
                              .bits(model.getOptions().getBits())
                              .min(model.getOptions().getMin())
                              .max(model.getOptions().getMax())
                              .bucketSize(model.getOptions().getBucketSize())
                              .storageEngine(toBsonDocument(model.getOptions().getStorageEngine()))
                              .partialFilterExpression(toBsonDocument(model.getOptions().getPartialFilterExpression()))
                              .collation(model.getOptions().getCollation()));
        }
        final CreateIndexesOperation createIndexesOperation = new CreateIndexesOperation(getNamespace(), indexRequests, writeConcern);
        executor.execute(createIndexesOperation, new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    callback.onResult(createIndexesOperation.getIndexNames(), null);
                }
            }
        });
    }

    @Override
    public ListIndexesIterable<Document> listIndexes() {
        return listIndexes(Document.class);
    }

    @Override
    public <TResult> ListIndexesIterable<TResult> listIndexes(final Class<TResult> resultClass) {
        return new ListIndexesIterableImpl<TResult>(namespace, resultClass, codecRegistry, readPreference, executor);
    }

    @Override
    public void dropIndex(final String indexName, final SingleResultCallback<Void> callback) {
        executor.execute(new DropIndexOperation(namespace, indexName, writeConcern), callback);
    }

    @Override
    public void dropIndex(final Bson keys, final SingleResultCallback<Void> callback) {
        executor.execute(new DropIndexOperation(namespace, keys.toBsonDocument(BsonDocument.class, codecRegistry), writeConcern), callback);
    }

    @Override
    public void dropIndexes(final SingleResultCallback<Void> callback) {
        dropIndex("*", callback);
    }

    @Override
    public void renameCollection(final MongoNamespace newCollectionNamespace, final SingleResultCallback<Void> callback) {
        renameCollection(newCollectionNamespace, new RenameCollectionOptions(), callback);
    }

    @Override
    public void renameCollection(final MongoNamespace newCollectionNamespace, final RenameCollectionOptions options,
                                 final SingleResultCallback<Void> callback) {
        executor.execute(new RenameCollectionOperation(getNamespace(), newCollectionNamespace, writeConcern)
                         .dropTarget(options.isDropTarget()), callback);
    }

    private void delete(final Bson filter, final DeleteOptions options, final boolean multi,
                        final SingleResultCallback<DeleteResult> callback) {
        executeSingleWriteRequest(new DeleteRequest(toBsonDocument(filter)).multi(multi).collation(options.getCollation()), null,
                new SingleResultCallback<BulkWriteResult>() {
                    @Override
                    public void onResult(final BulkWriteResult result, final Throwable t) {
                        if (t != null) {
                            callback.onResult(null, t);
                        } else {
                            if (result.wasAcknowledged()) {
                                callback.onResult(DeleteResult.acknowledged(result.getDeletedCount()), null);
                            } else {
                                callback.onResult(DeleteResult.unacknowledged(), null);
                            }

                        }
                    }
                });
    }

    private void update(final Bson filter, final Bson update, final UpdateOptions options, final boolean multi,
                        final SingleResultCallback<UpdateResult> callback) {
        executeSingleWriteRequest(new UpdateRequest(toBsonDocument(filter), toBsonDocument(update), WriteRequest.Type.UPDATE)
                        .upsert(options.isUpsert()).multi(multi).collation(options.getCollation()), options.getBypassDocumentValidation(),
                new SingleResultCallback<BulkWriteResult>() {
                    @Override
                    public void onResult(final BulkWriteResult result, final Throwable t) {
                        if (t != null) {
                            callback.onResult(null, t);
                        } else {
                            callback.onResult(toUpdateResult(result), null);
                        }
                    }
                });
    }

    private void executeSingleWriteRequest(final WriteRequest request, final Boolean bypassDocumentValidation,
                                           final SingleResultCallback<BulkWriteResult> callback) {
        executor.execute(new MixedBulkWriteOperation(namespace, singletonList(request), true, writeConcern)
                         .bypassDocumentValidation(bypassDocumentValidation),
                         new SingleResultCallback<BulkWriteResult>() {
                             @Override
                             public void onResult(final BulkWriteResult result, final Throwable t) {
                                 if (t instanceof MongoBulkWriteException) {
                                     MongoBulkWriteException e = (MongoBulkWriteException) t;
                                     if (e.getWriteErrors().isEmpty()) {
                                         callback.onResult(null,
                                                           new MongoWriteConcernException(e.getWriteConcernError(),
                                                                                          translateBulkWriteResult(request,
                                                                                                                   e.getWriteResult()),
                                                                                          e.getServerAddress()));
                                     } else {
                                         callback.onResult(null, new MongoWriteException(new WriteError(e.getWriteErrors().get(0)),
                                                                                         e.getServerAddress()));
                                     }
                                 } else {
                                     callback.onResult(result, t);
                                 }
                             }
                         });
    }

    private WriteConcernResult translateBulkWriteResult(final WriteRequest request, final BulkWriteResult writeResult) {
        switch (request.getType()) {
            case INSERT:
                return WriteConcernResult.acknowledged(writeResult.getInsertedCount(), false, null);
            case DELETE:
                return WriteConcernResult.acknowledged(writeResult.getDeletedCount(), false, null);
            case UPDATE:
            case REPLACE:
                return WriteConcernResult.acknowledged(writeResult.getMatchedCount() + writeResult.getUpserts().size(),
                                                       writeResult.getMatchedCount() > 0,
                                                       writeResult.getUpserts().isEmpty()
                                                       ? null : writeResult.getUpserts().get(0).getId());
            default:
                throw new MongoInternalException("Unhandled write request type: " + request.getType());
        }
    }

    private UpdateResult toUpdateResult(final com.mongodb2.bulk.BulkWriteResult result) {
        if (result.wasAcknowledged()) {
            Long modifiedCount = result.isModifiedCountAvailable() ? (long) result.getModifiedCount() : null;
            BsonValue upsertedId = result.getUpserts().isEmpty() ? null : result.getUpserts().get(0).getId();
            return UpdateResult.acknowledged(result.getMatchedCount(), modifiedCount, upsertedId);
        } else {
            return UpdateResult.unacknowledged();
        }
    }

    private Codec<TDocument> getCodec() {
        return getCodec(documentClass);
    }

    private <TResult> Codec<TResult> getCodec(final Class<TResult> resultClass) {
        return codecRegistry.get(resultClass);
    }

    private BsonDocument documentToBsonDocument(final TDocument document) {
        return BsonDocumentWrapper.asBsonDocument(document, codecRegistry);
    }

    private BsonDocument toBsonDocument(final Bson document) {
        return document == null ? null : document.toBsonDocument(documentClass, codecRegistry);
    }
}
