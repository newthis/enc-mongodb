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

import com.mongodb2.MongoGridFSException;
import com.mongodb2.ReadConcern;
import com.mongodb2.ReadPreference;
import com.mongodb2.WriteConcern;
import com.mongodb2.async.SingleResultCallback;
import com.mongodb2.async.client.MongoClients;
import com.mongodb2.async.client.MongoCollection;
import com.mongodb2.async.client.MongoDatabase;
import com.mongodb2.client.gridfs.model.GridFSDownloadOptions;
import com.mongodb2.client.gridfs.model.GridFSFile;
import com.mongodb2.client.gridfs.model.GridFSUploadOptions;
import com.mongodb2.client.result.DeleteResult;
import com.mongodb2.client.result.UpdateResult;
import com.mongodb2.diagnostics.logging.Logger;
import com.mongodb2.diagnostics.logging.Loggers;
import org.bson2.BsonDocument;
import org.bson2.BsonObjectId;
import org.bson2.BsonString;
import org.bson2.BsonValue;
import org.bson2.Document;
import org.bson2.conversions.Bson;
import org.bson2.types.ObjectId;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.mongodb2.assertions.Assertions.notNull;
import static com.mongodb2.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static java.lang.String.format;
import static org.bson2.codecs.configuration.CodecRegistries.fromRegistries;


final class GridFSBucketImpl implements GridFSBucket {
    private static final Logger LOGGER = Loggers.getLogger("client.gridfs");
    private static final int DEFAULT_CHUNKSIZE_BYTES = 255 * 1024;
    private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024 * 4;
    private final String bucketName;
    private final int chunkSizeBytes;
    private final MongoCollection<GridFSFile> filesCollection;
    private final MongoCollection<Document> chunksCollection;

    GridFSBucketImpl(final MongoDatabase database) {
        this(database, "fs");
    }

    GridFSBucketImpl(final MongoDatabase database, final String bucketName) {
        this(notNull("bucketName", bucketName), DEFAULT_CHUNKSIZE_BYTES,
                getFilesCollection(notNull("database", database), bucketName),
                getChunksCollection(database, bucketName));
    }

    GridFSBucketImpl(final String bucketName, final int chunkSizeBytes, final MongoCollection<GridFSFile> filesCollection,
                     final MongoCollection<Document> chunksCollection) {
        this.bucketName = notNull("bucketName", bucketName);
        this.chunkSizeBytes = chunkSizeBytes;
        this.filesCollection = notNull("filesCollection", filesCollection);
        this.chunksCollection = notNull("chunksCollection", chunksCollection);
    }

    @Override
    public String getBucketName() {
        return bucketName;
    }

    @Override
    public int getChunkSizeBytes() {
        return chunkSizeBytes;
    }

    @Override
    public ReadPreference getReadPreference() {
        return filesCollection.getReadPreference();
    }

    @Override
    public WriteConcern getWriteConcern() {
        return filesCollection.getWriteConcern();
    }

    @Override
    public ReadConcern getReadConcern() {
        return filesCollection.getReadConcern();
    }

    @Override
    public GridFSBucket withChunkSizeBytes(final int chunkSizeBytes) {
        return new GridFSBucketImpl(bucketName, chunkSizeBytes, filesCollection, chunksCollection);
    }

    @Override
    public GridFSBucket withReadPreference(final ReadPreference readPreference) {
        notNull("readPreference", readPreference);
        return new GridFSBucketImpl(bucketName, chunkSizeBytes, filesCollection.withReadPreference(readPreference),
                chunksCollection.withReadPreference(readPreference));
    }

    @Override
    public GridFSBucket withWriteConcern(final WriteConcern writeConcern) {
        notNull("writeConcern", writeConcern);
        return new GridFSBucketImpl(bucketName, chunkSizeBytes, filesCollection.withWriteConcern(writeConcern),
                chunksCollection.withWriteConcern(writeConcern));
    }

    @Override
    public GridFSBucket withReadConcern(final ReadConcern readConcern) {
        notNull("readConcern", readConcern);
        return new GridFSBucketImpl(bucketName, chunkSizeBytes, filesCollection.withReadConcern(readConcern),
                chunksCollection.withReadConcern(readConcern));
    }

    @Override
    public GridFSUploadStream openUploadStream(final String filename) {
        return openUploadStream(new BsonObjectId(), filename);
    }

    @Override
    public GridFSUploadStream openUploadStream(final String filename, final GridFSUploadOptions options) {
        return openUploadStream(new BsonObjectId(), filename, options);
    }

    @Override
    public GridFSUploadStream openUploadStream(final BsonValue id, final String filename) {
        return openUploadStream(id, filename, new GridFSUploadOptions());
    }

    @Override
    public GridFSUploadStream openUploadStream(final BsonValue id, final String filename, final GridFSUploadOptions options) {
        notNull("filename", filename);
        int chunkSize = options.getChunkSizeBytes() == null ? chunkSizeBytes : options.getChunkSizeBytes();
        return new GridFSUploadStreamImpl(filesCollection, chunksCollection, id, filename, chunkSize, options.getMetadata(),
                new GridFSIndexCheckImpl(filesCollection, chunksCollection));
    }

    @Override
    public void uploadFromStream(final String filename, final AsyncInputStream source, final SingleResultCallback<ObjectId> callback) {
        uploadFromStream(filename, source, new GridFSUploadOptions(), callback);
    }

    @Override
    public void uploadFromStream(final String filename, final AsyncInputStream source, final GridFSUploadOptions options,
                                 final SingleResultCallback<ObjectId> callback) {
        final BsonObjectId id = new BsonObjectId();
        uploadFromStream(id, filename, source, options, new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    callback.onResult(id.getValue(), null);
                }
            }
        });
    }

    @Override
    public void uploadFromStream(final BsonValue id, final String filename, final AsyncInputStream source,
                                 final SingleResultCallback<Void> callback) {
        uploadFromStream(id, filename, source, new GridFSUploadOptions(), callback);
    }

    @Override
    public void uploadFromStream(final BsonValue id, final String filename, final AsyncInputStream source,
                                 final GridFSUploadOptions options, final SingleResultCallback<Void> callback) {
        notNull("filename", filename);
        notNull("source", source);
        notNull("options", options);
        notNull("callback", callback);
        int chunkSize = options.getChunkSizeBytes() == null ? chunkSizeBytes : options.getChunkSizeBytes();
        readAndWriteInputStream(source, openUploadStream(id, filename, options), ByteBuffer.allocate(chunkSize),
                errorHandlingCallback(callback, LOGGER));
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final ObjectId id) {
        notNull("id", id);
        return new GridFSDownloadStreamImpl(find(new Document("_id", id)), chunksCollection);
    }

    @Override
    public void downloadToStream(final ObjectId id, final AsyncOutputStream destination, final SingleResultCallback<Long> callback) {
        notNull("id", id);
        notNull("destination", destination);
        notNull("callback", callback);
        downloadToAsyncOutputStream(openDownloadStream(id), destination, errorHandlingCallback(callback, LOGGER));
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final BsonValue id) {
        notNull("id", id);
        return new GridFSDownloadStreamImpl(find(new Document("_id", id)), chunksCollection);
    }

    @Override
    public void downloadToStream(final BsonValue id, final AsyncOutputStream destination, final SingleResultCallback<Long> callback) {
        notNull("id", id);
        notNull("destination", destination);
        notNull("callback", callback);
        downloadToAsyncOutputStream(openDownloadStream(id), destination, errorHandlingCallback(callback, LOGGER));
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final String filename) {
        return openDownloadStream(filename, new GridFSDownloadOptions());
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final String filename, final GridFSDownloadOptions options) {
        notNull("filename", filename);
        notNull("options", options);
        return new GridFSDownloadStreamImpl(findFileByName(filename, options), chunksCollection);
    }

    @Override
    public void downloadToStream(final String filename, final AsyncOutputStream destination,
                                 final SingleResultCallback<Long> callback) {
        downloadToStream(filename, destination, new GridFSDownloadOptions(), callback);
    }

    @Override
    public void downloadToStream(final String filename, final AsyncOutputStream destination,
                                 final GridFSDownloadOptions options, final SingleResultCallback<Long> callback) {
        notNull("filename", filename);
        notNull("destination", destination);
        notNull("options", options);
        notNull("callback", callback);
        downloadToAsyncOutputStream(openDownloadStream(filename, options), destination, errorHandlingCallback(callback, LOGGER));
    }

    @Override
    public GridFSFindIterable find() {
        return new GridFSFindIterableImpl(filesCollection.find());
    }

    @Override
    public GridFSFindIterable find(final Bson filter) {
        notNull("filter", filter);
        return new GridFSFindIterableImpl(filesCollection.find(filter));
    }

    @Override
    public void delete(final ObjectId id, final SingleResultCallback<Void> callback) {
        delete(new BsonObjectId(id), callback);
    }

    @Override
    public void delete(final BsonValue id, final SingleResultCallback<Void> callback) {
        notNull("id", id);
        notNull("callback", callback);
        final SingleResultCallback<Void> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
        filesCollection.deleteOne(new BsonDocument("_id", id), new SingleResultCallback<DeleteResult>() {
            @Override
            public void onResult(final DeleteResult filesResult, final Throwable t) {
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                } else {
                    chunksCollection.deleteMany(new BsonDocument("files_id", id),
                            new SingleResultCallback<DeleteResult>() {
                                @Override
                                public void onResult(final DeleteResult chunksResult, final Throwable t) {
                                    if (t != null) {
                                        errHandlingCallback.onResult(null, t);
                                    } else if (filesResult.wasAcknowledged() && filesResult.getDeletedCount() == 0) {
                                        errHandlingCallback.onResult(null,
                                                new MongoGridFSException(format("No file found with the ObjectId: %s", id)));
                                    } else {
                                        errHandlingCallback.onResult(null, null);
                                    }
                                }
                            });
                }
            }
        });
    }

    @Override
    public void rename(final ObjectId id, final String newFilename, final SingleResultCallback<Void> callback) {
        rename(new BsonObjectId(id), newFilename, callback);
    }

    @Override
    public void rename(final BsonValue id, final String newFilename, final SingleResultCallback<Void> callback) {
        notNull("id", id);
        notNull("newFilename", newFilename);
        notNull("callback", callback);
        final SingleResultCallback<Void> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
        filesCollection.updateOne(new BsonDocument("_id", id), new BsonDocument("$set",
                new BsonDocument("filename", new BsonString(newFilename))),
                new SingleResultCallback<UpdateResult>() {

                    @Override
                    public void onResult(final UpdateResult result, final Throwable t) {
                        if (t != null) {
                            errHandlingCallback.onResult(null, t);
                        } else if (result.wasAcknowledged() && result.getMatchedCount() == 0) {
                            errHandlingCallback.onResult(null, new MongoGridFSException(format("No file found with the ObjectId: %s",
                                    id)));
                        } else {
                            errHandlingCallback.onResult(null, null);
                        }
                    }
                });
    }

    @Override
    public void drop(final SingleResultCallback<Void> callback) {
        notNull("callback", callback);
        final SingleResultCallback<Void> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
        filesCollection.drop(new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                } else {
                    chunksCollection.drop(errHandlingCallback);
                }
            }
        });
    }

    private GridFSFindIterable findFileByName(final String filename, final GridFSDownloadOptions options) {
        int revision = options.getRevision();
        int skip;
        int sort;
        if (revision >= 0) {
            skip = revision;
            sort = 1;
        } else {
            skip = (-revision) - 1;
            sort = -1;
        }

        return new GridFSFindIterableImpl(filesCollection.find(new Document("filename", filename)).skip(skip)
                .sort(new Document("uploadDate", sort)));
    }

    private static MongoCollection<GridFSFile> getFilesCollection(final MongoDatabase database, final String bucketName) {
        return database.getCollection(bucketName + ".files", GridFSFile.class).withCodecRegistry(
                fromRegistries(database.getCodecRegistry(), MongoClients.getDefaultCodecRegistry())
        );
    }

    private static MongoCollection<Document> getChunksCollection(final MongoDatabase database, final String bucketName) {
        return database.getCollection(bucketName + ".chunks").withCodecRegistry(MongoClients.getDefaultCodecRegistry());
    }

    private void downloadToAsyncOutputStream(final GridFSDownloadStream downloadStream, final AsyncOutputStream destination,
                                             final SingleResultCallback<Long> callback) {
        downloadStream.getGridFSFile(new SingleResultCallback<GridFSFile>() {
            @Override
            public void onResult(final GridFSFile result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    int bufferSize = DEFAULT_BUFFER_SIZE > result.getLength() ? (int) result.getLength() : DEFAULT_BUFFER_SIZE;
                    readAndWriteOutputStream(destination, downloadStream, ByteBuffer.allocate(bufferSize), 0, callback);
                }
            }
        });
    }

    private void readAndWriteInputStream(final AsyncInputStream source, final GridFSUploadStream uploadStream, final ByteBuffer buffer,
                                         final SingleResultCallback<Void> callback) {
        buffer.clear();
        source.read(buffer, new SingleResultCallback<Integer>() {
            @Override
            public void onResult(final Integer result, final Throwable t) {
                if (t != null) {
                    if (t instanceof IOException) {
                        uploadStream.abort(new SingleResultCallback<Void>() {
                            @Override
                            public void onResult(final Void result, final Throwable abortException) {
                                if (abortException != null) {
                                    callback.onResult(null, abortException);
                                } else {
                                    callback.onResult(null, new MongoGridFSException("IOException when reading from the InputStream", t));
                                }
                            }
                        });
                    } else {
                        callback.onResult(null, t);
                    }
                } else if (result > 0) {
                    buffer.flip();
                    uploadStream.write(buffer, new SingleResultCallback<Integer>() {
                        @Override
                        public void onResult(final Integer result, final Throwable t) {
                            if (t != null) {
                                callback.onResult(null, t);
                            } else {
                                readAndWriteInputStream(source, uploadStream, buffer, callback);
                            }
                        }
                    });
                } else {
                    uploadStream.close(callback);
                }
            }
        });
    }

    private void readAndWriteOutputStream(final AsyncOutputStream destination, final GridFSDownloadStream downloadStream,
                                          final ByteBuffer buffer, final long amountRead, final SingleResultCallback<Long> callback) {
        buffer.clear();
        downloadStream.read(buffer, new SingleResultCallback<Integer>() {
            @Override
            public void onResult(final Integer readResult, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else if (readResult > 0) {
                    buffer.flip();
                    destination.write(buffer, new SingleResultCallback<Integer>() {
                        @Override
                        public void onResult(final Integer writeResult, final Throwable t) {
                            if (t != null) {
                                callback.onResult(null, t);
                            } else {
                                readAndWriteOutputStream(destination, downloadStream, buffer, amountRead + writeResult, callback);
                            }
                        }
                    });
                } else {
                    callback.onResult(amountRead, null);
                }
            }
        });
    }

}
