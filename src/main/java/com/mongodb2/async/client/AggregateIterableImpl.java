/*
 * Copyright 2015-2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb2.async.client;

import com.mongodb2.Block;
import com.mongodb2.Function;
import com.mongodb2.MongoNamespace;
import com.mongodb2.ReadConcern;
import com.mongodb2.ReadPreference;
import com.mongodb2.WriteConcern;
import com.mongodb2.async.AsyncBatchCursor;
import com.mongodb2.async.SingleResultCallback;
import com.mongodb2.client.model.Collation;
import com.mongodb2.client.model.FindOptions;
import com.mongodb2.operation.AggregateOperation;
import com.mongodb2.operation.AggregateToCollectionOperation;
import com.mongodb2.operation.AsyncOperationExecutor;
import org.bson2.BsonDocument;
import org.bson2.BsonValue;
import org.bson2.codecs.configuration.CodecRegistry;
import org.bson2.conversions.Bson;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb2.ReadPreference.primary;
import static com.mongodb2.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;


class AggregateIterableImpl<TDocument, TResult> implements AggregateIterable<TResult> {
    private final MongoNamespace namespace;
    private final Class<TDocument> documentClass;
    private final Class<TResult> resultClass;
    private final ReadPreference readPreference;
    private final ReadConcern readConcern;
    private final WriteConcern writeConcern;
    private final CodecRegistry codecRegistry;
    private final AsyncOperationExecutor executor;
    private final List<? extends Bson> pipeline;

    private Boolean allowDiskUse;
    private Integer batchSize;
    private long maxTimeMS;
    private Boolean useCursor;
    private Boolean bypassDocumentValidation;
    private Collation collation;

    AggregateIterableImpl(final MongoNamespace namespace, final Class<TDocument> documentClass, final Class<TResult> resultClass,
                          final CodecRegistry codecRegistry, final ReadPreference readPreference, final ReadConcern readConcern,
                          final WriteConcern writeConcern, final AsyncOperationExecutor executor, final List<? extends Bson> pipeline) {
        this.namespace = notNull("namespace", namespace);
        this.documentClass = notNull("documentClass", documentClass);
        this.resultClass = notNull("resultClass", resultClass);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.readPreference = notNull("readPreference", readPreference);
        this.readConcern = notNull("readConcern", readConcern);
        this.writeConcern = notNull("writeConcern", writeConcern);
        this.executor = notNull("executor", executor);
        this.pipeline = notNull("pipeline", pipeline);
    }

    @Override
    public AggregateIterable<TResult> allowDiskUse(final Boolean allowDiskUse) {
        this.allowDiskUse = allowDiskUse;
        return this;
    }

    @Override
    public AggregateIterable<TResult> batchSize(final int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    @Override
    public AggregateIterable<TResult> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    @Override
    public AggregateIterable<TResult> useCursor(final Boolean useCursor) {
        this.useCursor = useCursor;
        return this;
    }

    @Override
    public void toCollection(final SingleResultCallback<Void> callback) {
        List<BsonDocument> aggregateList = createBsonDocumentList();
        BsonValue outCollection = getAggregateOutCollection(aggregateList);

        if (outCollection == null) {
            throw new IllegalStateException("The last stage of the aggregation pipeline must be $out");
        }

        executor.execute(new AggregateToCollectionOperation(namespace, aggregateList, writeConcern)
                .maxTime(maxTimeMS, MILLISECONDS)
                .allowDiskUse(allowDiskUse)
                .collation(collation), callback);
    }

    @Override
    public void first(final SingleResultCallback<TResult> callback) {
        notNull("callback", callback);
        execute().first(callback);
    }

    @Override
    public void forEach(final Block<? super TResult> block, final SingleResultCallback<Void> callback) {
        notNull("block", block);
        notNull("callback", callback);
        execute().forEach(block, callback);
    }

    @Override
    public <A extends Collection<? super TResult>> void into(final A target, final SingleResultCallback<A> callback) {
        notNull("target", target);
        notNull("callback", callback);
        execute().into(target, callback);
    }

    @Override
    public <U> MongoIterable<U> map(final Function<TResult, U> mapper) {
        return new MappingIterable<TResult, U>(this, mapper);
    }

    @Override
    public void batchCursor(final SingleResultCallback<AsyncBatchCursor<TResult>> callback) {
        notNull("callback", callback);
        execute().batchCursor(callback);
    }

    @Override
    public AggregateIterable<TResult> bypassDocumentValidation(final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    @Override
    public AggregateIterable<TResult> collation(final Collation collation) {
        this.collation = collation;
        return this;
    }

    private MongoIterable<TResult> execute() {
        List<BsonDocument> aggregateList = createBsonDocumentList();
        BsonValue outCollection = getAggregateOutCollection(aggregateList);

        if (outCollection != null) {
            AggregateToCollectionOperation operation = new AggregateToCollectionOperation(namespace, aggregateList, writeConcern)
                    .maxTime(maxTimeMS, MILLISECONDS)
                    .allowDiskUse(allowDiskUse)
                    .bypassDocumentValidation(bypassDocumentValidation)
                    .collation(collation);
            MongoIterable<TResult> delegated = new FindIterableImpl<TDocument, TResult>(new MongoNamespace(namespace.getDatabaseName(),
                    outCollection.asString().getValue()), documentClass, resultClass, codecRegistry, primary(), readConcern,
                    executor, new BsonDocument(), new FindOptions().collation(collation));
            if (batchSize != null) {
                delegated.batchSize(batchSize);
            }
            return new AwaitingWriteOperationIterable<TResult, Void>(operation, executor, delegated);
        } else {
            return new OperationIterable<TResult>(new AggregateOperation<TResult>(namespace, aggregateList, codecRegistry.get(resultClass))
                    .maxTime(maxTimeMS, MILLISECONDS)
                    .allowDiskUse(allowDiskUse)
                    .batchSize(batchSize)
                    .useCursor(useCursor)
                    .readConcern(readConcern)
                    .collation(collation),
                    readPreference,
                    executor);
        }
    }

    private BsonValue getAggregateOutCollection(final List<BsonDocument> aggregateList) {
        return aggregateList.size() == 0 ? null : aggregateList.get(aggregateList.size() - 1).get("$out");
    }

    private List<BsonDocument> createBsonDocumentList() {
        List<BsonDocument> aggregateList = new ArrayList<BsonDocument>(pipeline.size());
        for (Bson document : pipeline) {
            if (document == null) {
                throw new IllegalArgumentException("pipeline can not contain a null value");
            }
            aggregateList.add(document.toBsonDocument(documentClass, codecRegistry));
        }
        return aggregateList;
    }
}
