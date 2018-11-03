/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb2.async.client.gridfs;

import com.mongodb2.Block;
import com.mongodb2.Function;
import com.mongodb2.async.AsyncBatchCursor;
import com.mongodb2.async.SingleResultCallback;
import com.mongodb2.async.client.FindIterable;
import com.mongodb2.async.client.MongoIterable;
import com.mongodb2.client.gridfs.model.GridFSFile;
import com.mongodb2.client.model.Collation;
import org.bson2.conversions.Bson;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

final class GridFSFindIterableImpl implements GridFSFindIterable {
    private final FindIterable<GridFSFile> underlying;

    public GridFSFindIterableImpl(final FindIterable<GridFSFile> underlying) {
        this.underlying = underlying;
    }

    @Override
    public GridFSFindIterable sort(final Bson sort) {
        underlying.sort(sort);
        return this;
    }

    @Override
    public GridFSFindIterable skip(final int skip) {
        underlying.skip(skip);
        return this;
    }

    @Override
    public GridFSFindIterable limit(final int limit) {
        underlying.limit(limit);
        return this;
    }

    @Override
    public GridFSFindIterable filter(final Bson filter) {
        underlying.filter(filter);
        return this;
    }

    @Override
    public GridFSFindIterable maxTime(final long maxTime, final TimeUnit timeUnit) {
        underlying.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public GridFSFindIterable noCursorTimeout(final boolean noCursorTimeout) {
        underlying.noCursorTimeout(noCursorTimeout);
        return this;
    }

    @Override
    public void first(final SingleResultCallback<GridFSFile> callback) {
        underlying.first(callback);
    }

    @Override
    public void forEach(final Block<? super GridFSFile> block, final SingleResultCallback<Void> callback) {
        underlying.forEach(block, callback);
    }

    @Override
    public <A extends Collection<? super GridFSFile>> void into(final A target, final SingleResultCallback<A> callback) {
        underlying.into(target, callback);
    }

    @Override
    public <U> MongoIterable<U> map(final Function<GridFSFile, U> mapper) {
        return underlying.map(mapper);
    }

    @Override
    public GridFSFindIterable batchSize(final int batchSize) {
        underlying.batchSize(batchSize);
        return this;
    }

    @Override
    public GridFSFindIterable collation(final Collation collation) {
        underlying.collation(collation);
        return this;
    }

    @Override
    public void batchCursor(final SingleResultCallback<AsyncBatchCursor<GridFSFile>> callback) {
        underlying.batchCursor(callback);
    }

}
