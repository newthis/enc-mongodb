/*
 * Copyright 2015 MongoDB, Inc.
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

import com.mongodb2.client.ListIndexesIterable;
import com.mongodb2.client.MongoCursor;
import com.mongodb2.client.MongoIterable;
import com.mongodb2.operation.ListIndexesOperation;
import com.mongodb2.operation.OperationExecutor;
import org.bson2.codecs.configuration.CodecRegistry;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static com.mongodb2.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

final class ListIndexesIterableImpl<TResult> implements ListIndexesIterable<TResult> {
    private final MongoNamespace namespace;
    private final Class<TResult> resultClass;
    private final ReadPreference readPreference;
    private final CodecRegistry codecRegistry;
    private final OperationExecutor executor;

    private int batchSize;
    private long maxTimeMS;

    ListIndexesIterableImpl(final MongoNamespace namespace, final Class<TResult> resultClass, final CodecRegistry codecRegistry,
                            final ReadPreference readPreference, final OperationExecutor executor) {
        this.namespace = notNull("namespace", namespace);
        this.resultClass = notNull("resultClass", resultClass);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.readPreference = notNull("readPreference", readPreference);
        this.executor = notNull("executor", executor);
    }

    @Override
    public ListIndexesIterable<TResult> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    @Override
    public ListIndexesIterable<TResult> batchSize(final int batchSize) {
        this.batchSize = batchSize;
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
        return new OperationIterable<TResult>(createListIndexesOperation(), readPreference, executor);
    }

    private ListIndexesOperation<TResult> createListIndexesOperation() {
        return new ListIndexesOperation<TResult>(namespace, codecRegistry.get(resultClass))
                .batchSize(batchSize)
                .maxTime(maxTimeMS, MILLISECONDS);
    }

}
