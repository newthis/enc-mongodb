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

package com.mongodb2;

import com.mongodb2.client.FindIterable;
import com.mongodb2.client.MongoCursor;
import com.mongodb2.client.MongoIterable;
import com.mongodb2.client.model.Collation;
import com.mongodb2.client.model.FindOptions;
import com.mongodb2.operation.BatchCursor;
import com.mongodb2.operation.FindOperation;
import com.mongodb2.operation.OperationExecutor;
import org.bson2.BsonDocument;
import org.bson2.codecs.configuration.CodecRegistry;
import org.bson2.conversions.Bson;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static com.mongodb2.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

final class FindIterableImpl<TDocument, TResult> implements FindIterable<TResult> {
    private final MongoNamespace namespace;
    private final Class<TDocument> documentClass;
    private final Class<TResult> resultClass;
    private final ReadPreference readPreference;
    private final ReadConcern readConcern;
    private final CodecRegistry codecRegistry;
    private final OperationExecutor executor;
    private final FindOptions findOptions;

    private Bson filter;

    FindIterableImpl(final MongoNamespace namespace, final Class<TDocument> documentClass, final Class<TResult> resultClass,
                     final CodecRegistry codecRegistry, final ReadPreference readPreference, final ReadConcern readConcern,
                     final OperationExecutor executor, final Bson filter, final FindOptions findOptions) {
        this.namespace = notNull("namespace", namespace);
        this.documentClass = notNull("documentClass", documentClass);
        this.resultClass = notNull("resultClass", resultClass);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.readPreference = notNull("readPreference", readPreference);
        this.readConcern = notNull("readConcern", readConcern);
        this.executor = notNull("executor", executor);
        this.filter = notNull("filter", filter);
        this.findOptions = notNull("findOptions", findOptions);
    }

    @Override
    public FindIterable<TResult> filter(final Bson filter) {
        this.filter = filter;
        return this;
    }

    @Override
    public FindIterable<TResult> limit(final int limit) {
        findOptions.limit(limit);
        return this;
    }

    @Override
    public FindIterable<TResult> skip(final int skip) {
        findOptions.skip(skip);
        return this;
    }

    @Override
    public FindIterable<TResult> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        findOptions.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public FindIterable<TResult> maxAwaitTime(final long maxAwaitTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        findOptions.maxAwaitTime(maxAwaitTime, timeUnit);
        return this;
    }

    @Override
    public FindIterable<TResult> batchSize(final int batchSize) {
        findOptions.batchSize(batchSize);
        return this;
    }

    @Override
    public FindIterable<TResult> collation(final Collation collation) {
        findOptions.collation(collation);
        return this;
    }

    @Override
    public FindIterable<TResult> modifiers(final Bson modifiers) {
        findOptions.modifiers(modifiers);
        return this;
    }

    @Override
    public FindIterable<TResult> projection(final Bson projection) {
        findOptions.projection(projection);
        return this;
    }

    @Override
    public FindIterable<TResult> sort(final Bson sort) {
        findOptions.sort(sort);
        return this;
    }

    @Override
    public FindIterable<TResult> noCursorTimeout(final boolean noCursorTimeout) {
        findOptions.noCursorTimeout(noCursorTimeout);
        return this;
    }

    @Override
    public FindIterable<TResult> oplogReplay(final boolean oplogReplay) {
        findOptions.oplogReplay(oplogReplay);
        return this;
    }

    @Override
    public FindIterable<TResult> partial(final boolean partial) {
        findOptions.partial(partial);
        return this;
    }

    @Override
    public FindIterable<TResult> cursorType(final CursorType cursorType) {
        findOptions.cursorType(cursorType);
        return this;
    }

    @Override
    public MongoCursor<TResult> iterator() {
        return execute().iterator();
    }

    @Override
    public TResult first() {
        return execute().first();
    }

    @Override
    public <U> MongoIterable<U> map(final Function<TResult, U> mapper) {
        return new MappingIterable<TResult, U>(this, mapper);
    }

    @Override
    public void forEach(final Block<? super TResult> block) {
        execute().forEach(block);
    }

    @Override
    public <A extends Collection<? super TResult>> A into(final A target) {
        return execute().into(target);
    }

    private MongoIterable<TResult> execute() {
        return new FindOperationIterable(createQueryOperation(), readPreference, executor);
    }

    private FindOperation<TResult> createQueryOperation() {
        return new FindOperation<TResult>(namespace, codecRegistry.get(resultClass))
                   .filter(filter.toBsonDocument(documentClass, codecRegistry))
                   .batchSize(findOptions.getBatchSize())
                   .skip(findOptions.getSkip())
                   .limit(findOptions.getLimit())
                   .maxTime(findOptions.getMaxTime(MILLISECONDS), MILLISECONDS)
                   .maxAwaitTime(findOptions.getMaxAwaitTime(MILLISECONDS), MILLISECONDS)
                   .modifiers(toBsonDocument(findOptions.getModifiers()))
                   .projection(toBsonDocument(findOptions.getProjection()))
                   .sort(toBsonDocument(findOptions.getSort()))
                   .cursorType(findOptions.getCursorType())
                   .noCursorTimeout(findOptions.isNoCursorTimeout())
                   .oplogReplay(findOptions.isOplogReplay())
                   .partial(findOptions.isPartial())
                   .slaveOk(readPreference.isSlaveOk())
                   .readConcern(readConcern)
                   .collation(findOptions.getCollation());
    }

    private BsonDocument toBsonDocument(final Bson document) {
        return document == null ? null : document.toBsonDocument(documentClass, codecRegistry);
    }

    private final class FindOperationIterable extends OperationIterable<TResult> {
        private final ReadPreference readPreference;
        private final OperationExecutor executor;

        FindOperationIterable(final FindOperation<TResult> operation, final ReadPreference readPreference,
                              final OperationExecutor executor) {
            super(operation, readPreference, executor);
            this.readPreference = readPreference;
            this.executor = executor;
        }

        @Override
        public TResult first() {
            FindOperation<TResult> findFirstOperation = createQueryOperation().batchSize(0).limit(-1);
            BatchCursor<TResult> batchCursor = executor.execute(findFirstOperation, readPreference);
            return batchCursor.hasNext() ? batchCursor.next().iterator().next() : null;
        }
    }
}
