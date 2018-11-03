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

package com.mongodb2.client.gridfs;

import com.mongodb2.ReadConcern;
import com.mongodb2.ReadPreference;
import com.mongodb2.WriteConcern;
import com.mongodb2.annotations.ThreadSafe;
import com.mongodb2.client.gridfs.model.GridFSDownloadOptions;
import com.mongodb2.client.gridfs.model.GridFSUploadOptions;
import org.bson2.BsonValue;
import org.bson2.conversions.Bson;
import org.bson2.types.ObjectId;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Represents a GridFS Bucket
 *
 * @since 3.1
 */
@ThreadSafe
public interface GridFSBucket {

    /**
     * The bucket name.
     *
     * @return the bucket name
     */
    String getBucketName();

    /**
     * Sets the chunk size in bytes. Defaults to 255.
     *
     * @return the chunk size in bytes.
     */
    int getChunkSizeBytes();

    /**
     * Get the write concern for the GridFSBucket.
     *
     * @return the {@link com.mongodb2.WriteConcern}
     */
    WriteConcern getWriteConcern();

    /**
     * Get the read preference for the GridFSBucket.
     *
     * @return the {@link com.mongodb2.ReadPreference}
     */
    ReadPreference getReadPreference();

    /**
     * Get the read concern for the GridFSBucket.
     *
     * @return the {@link com.mongodb2.ReadConcern}
     * @since 3.2
     * @mongodb.server.release 3.2
     * @mongodb.driver.manual reference/readConcern/ Read Concern
     */
    ReadConcern getReadConcern();

    /**
     *  Create a new GridFSBucket instance with a new chunk size in bytes.
     *
     * @param chunkSizeBytes the new chunk size in bytes.
     * @return a new GridFSBucket instance with the different chunk size in bytes
     */
    GridFSBucket withChunkSizeBytes(int chunkSizeBytes);

    /**
     * Create a new GridFSBucket instance with a different read preference.
     *
     * @param readPreference the new {@link ReadPreference} for the GridFSBucket
     * @return a new GridFSBucket instance with the different readPreference
     */
    GridFSBucket withReadPreference(ReadPreference readPreference);

    /**
     * Create a new GridFSBucket instance with a different write concern.
     *
     * @param writeConcern the new {@link WriteConcern} for the GridFSBucket
     * @return a new GridFSBucket instance with the different writeConcern
     */
    GridFSBucket withWriteConcern(WriteConcern writeConcern);

    /**
     * Create a new GridFSBucket instance with a different read concern.
     *
     * @param readConcern the new {@link ReadConcern} for the GridFSBucket
     * @return a new GridFSBucket instance with the different ReadConcern
     * @since 3.2
     * @mongodb.server.release 3.2
     * @mongodb.driver.manual reference/readConcern/ Read Concern
     */
    GridFSBucket withReadConcern(ReadConcern readConcern);

    /**
     * Opens a Stream that the application can write the contents of the file to.
     *<p>
     * As the application writes the contents to the returned Stream, the contents are uploaded as chunks in the chunks collection. When
     * the application signals it is done writing the contents of the file by calling close on the returned Stream, a files collection
     * document is created in the files collection.
     *</p>
     *
     * @param filename the filename for the stream
     * @return the GridFSUploadStream that provides the ObjectId for the file to be uploaded and the Stream to which the
     *          application will write the contents.
     */
    GridFSUploadStream openUploadStream(String filename);

    /**
     * Opens a Stream that the application can write the contents of the file to.
     *<p>
     * As the application writes the contents to the returned Stream, the contents are uploaded as chunks in the chunks collection. When
     * the application signals it is done writing the contents of the file by calling close on the returned Stream, a files collection
     * document is created in the files collection.
     *</p>
     * @param filename the filename for the stream
     * @param options the GridFSUploadOptions
     * @return the GridFSUploadStream that provides the ObjectId for the file to be uploaded and the Stream to which the
     *          application will write the contents.
     */
    GridFSUploadStream openUploadStream(String filename, GridFSUploadOptions options);

    /**
     * Opens a Stream that the application can write the contents of the file to.
     *<p>
     * As the application writes the contents to the returned Stream, the contents are uploaded as chunks in the chunks collection. When
     * the application signals it is done writing the contents of the file by calling close on the returned Stream, a files collection
     * document is created in the files collection.
     *</p>
     *
     * @param id the custom id value of the file
     * @param filename the filename for the stream
     * @return the GridFSUploadStream that provides the ObjectId for the file to be uploaded and the Stream to which the
     *          application will write the contents.
     * @since 3.3
     */
    GridFSUploadStream openUploadStream(BsonValue id, String filename);

    /**
     * Opens a Stream that the application can write the contents of the file to.
     *<p>
     * As the application writes the contents to the returned Stream, the contents are uploaded as chunks in the chunks collection. When
     * the application signals it is done writing the contents of the file by calling close on the returned Stream, a files collection
     * document is created in the files collection.
     *</p>
     *
     * @param id the custom id value of the file
     * @param filename the filename for the stream
     * @param options the GridFSUploadOptions
     * @return the GridFSUploadStream that includes the _id for the file to be uploaded and the Stream to which the
     *          application will write the contents.
     * @since 3.3
     */
    GridFSUploadStream openUploadStream(BsonValue id, String filename, GridFSUploadOptions options);

    /**
     * Uploads the contents of the given {@code InputStream} to a GridFS bucket.
     *<p>
     * Reads the contents of the user file from the {@code Stream} and uploads it as chunks in the chunks collection. After all the
     * chunks have been uploaded, it creates a files collection document for {@code filename} in the files collection.
     *</p>
     *
     * @param filename the filename for the stream
     * @param source the Stream providing the file data
     * @return the ObjectId of the uploaded file.
     */
    ObjectId uploadFromStream(String filename, InputStream source);

    /**
     * Uploads the contents of the given {@code InputStream} to a GridFS bucket.
     * <p>
     * Reads the contents of the user file from the {@code Stream} and uploads it as chunks in the chunks collection. After all the
     * chunks have been uploaded, it creates a files collection document for {@code filename} in the files collection.
     * </p>
     *
     * @param filename the filename for the stream
     * @param source the Stream providing the file data
     * @param options the GridFSUploadOptions
     * @return the ObjectId of the uploaded file.
     */
    ObjectId uploadFromStream(String filename, InputStream source, GridFSUploadOptions options);

    /**
     * Uploads the contents of the given {@code InputStream} to a GridFS bucket.
     *<p>
     * Reads the contents of the user file from the {@code Stream} and uploads it as chunks in the chunks collection. After all the
     * chunks have been uploaded, it creates a files collection document for {@code filename} in the files collection.
     *</p>
     *
     * @param id the custom id value of the file
     * @param filename the filename for the stream
     * @param source the Stream providing the file data
     * @since 3.3
     */
    void uploadFromStream(BsonValue id, String filename, InputStream source);

    /**
     * Uploads the contents of the given {@code InputStream} to a GridFS bucket.
     * <p>
     * Reads the contents of the user file from the {@code Stream} and uploads it as chunks in the chunks collection. After all the
     * chunks have been uploaded, it creates a files collection document for {@code filename} in the files collection.
     * </p>
     *
     * @param id the custom id value of the file
     * @param filename the filename for the stream
     * @param source the Stream providing the file data
     * @param options the GridFSUploadOptions
     * @since 3.3
     */
    void uploadFromStream(BsonValue id, String filename, InputStream source, GridFSUploadOptions options);

    /**
     * Opens a Stream from which the application can read the contents of the stored file specified by {@code id}.
     *
     * @param id the ObjectId of the file to be put into a stream.
     * @return the stream
     */
    GridFSDownloadStream openDownloadStream(ObjectId id);

    /**
     * Downloads the contents of the stored file specified by {@code id} and writes the contents to the {@code destination} Stream.
     *
     * @param id the ObjectId of the file to be written to the destination stream
     * @param destination the destination stream
     */
    void downloadToStream(ObjectId id, OutputStream destination);

    /**
     * Opens a Stream from which the application can read the contents of the stored file specified by {@code id}.
     *
     * @param id the custom id value of the file, to be put into a stream.
     * @return the stream
     */
    GridFSDownloadStream openDownloadStream(BsonValue id);

    /**
     * Downloads the contents of the stored file specified by {@code id} and writes the contents to the {@code destination} Stream.
     *
     * @param id the custom id of the file, to be written to the destination stream
     * @param destination the destination stream
     */
    void downloadToStream(BsonValue id, OutputStream destination);

    /**
     * Downloads the contents of the latest version of the stored file specified by {@code filename} and writes the contents to
     * the {@code destination} Stream.
     *
     * @param filename the name of the file to be downloaded
     * @param destination the destination stream
     * @since 3.3
     */
    void downloadToStream(String filename, OutputStream destination);

    /**
     * Downloads the contents of the stored file specified by {@code filename} and by the revision in {@code options} and writes the
     * contents to the {@code destination} Stream.
     *
     * @param filename the name of the file to be downloaded
     * @param destination the destination stream
     * @param options the download options
     * @since 3.3
     */
    void downloadToStream(String filename, OutputStream destination, GridFSDownloadOptions options);

    /**
     * Opens a Stream from which the application can read the contents of the latest version of the stored file specified by the
     * {@code filename}.
     *
     * @param filename the name of the file to be downloaded
     * @return the stream
     * @since 3.3
     */
    GridFSDownloadStream openDownloadStream(String filename);

    /**
     * Opens a Stream from which the application can read the contents of the stored file specified by {@code filename} and the revision
     * in {@code options}.
     *
     * @param filename the name of the file to be downloaded
     * @param options the download options
     * @return the stream
     * @since 3.3
     */
    GridFSDownloadStream openDownloadStream(String filename, GridFSDownloadOptions options);

    /**
     * Finds all documents in the files collection.
     *
     * @return the GridFS find iterable interface
     * @mongodb.driver.manual tutorial/query-documents/ Find
     */
    GridFSFindIterable find();

    /**
     * Finds all documents in the collection that match the filter.
     *
     * <p>
     *     Below is an example of filtering against the filename and some nested metadata that can also be stored along with the file data:
     *  <pre>
     *  {@code
     *      Filters.and(Filters.eq("filename", "mongodb.png"), Filters.eq("metadata.contentType", "image/png"));
     *  }
     *  </pre>
     *
     * @param filter the query filter
     * @return the GridFS find iterable interface
     * @see com.mongodb2.client.model.Filters
     */
    GridFSFindIterable find(Bson filter);

    /**
     * Given a {@code id}, delete this stored file's files collection document and associated chunks from a GridFS bucket.
     * @param id the ObjectId of the file to be deleted
     */
    void delete(ObjectId id);

    /**
     * Given a {@code id}, delete this stored file's files collection document and associated chunks from a GridFS bucket.
     * @param id the id of the file to be deleted
     * @since 3.3
     */
    void delete(BsonValue id);

    /**
     * Renames the stored file with the specified {@code id}.
     *
     * @param id the id of the file in the files collection to rename
     * @param newFilename the new filename for the file
     */
    void rename(ObjectId id, String newFilename);

    /**
     * Renames the stored file with the specified {@code id}.
     *
     * @param id the id of the file in the files collection to rename
     * @param newFilename the new filename for the file
     * @since 3.3
     */
    void rename(BsonValue id, String newFilename);

    /**
     * Drops the data associated with this bucket from the database.
     */
    void drop();

    // Deprecated APIs

    /**
     * Opens a Stream from which the application can read the contents of the latest version of the stored file specified by the
     * {@code filename}.
     *
     * @param filename the name of the file to be downloaded
     * @deprecated use {@link #openDownloadStream(String)} instead.
     * @return the stream
     */
    @Deprecated
    GridFSDownloadStream openDownloadStreamByName(String filename);

    /**
     * Opens a Stream from which the application can read the contents of the stored file specified by {@code filename} and the revision
     * in {@code options}.
     *
     * @param filename the name of the file to be downloaded
     * @param options the download options
     * @deprecated use {@link #openDownloadStream(String, GridFSDownloadOptions)} instead.
     * @return the stream
     */
    @Deprecated
    @SuppressWarnings("deprecation")
    GridFSDownloadStream openDownloadStreamByName(String filename, com.mongodb2.client.gridfs.model.GridFSDownloadByNameOptions options);

    /**
     * Downloads the contents of the latest version of the stored file specified by {@code filename} and writes the contents to
     * the {@code destination} Stream.
     *
     * @param filename the name of the file to be downloaded
     * @param destination the destination stream
     * @deprecated use {@link #downloadToStream(String, OutputStream)} instead.
     */
    @Deprecated
    void downloadToStreamByName(String filename, OutputStream destination);

    /**
     * Downloads the contents of the stored file specified by {@code filename} and by the revision in {@code options} and writes the
     * contents to the {@code destination} Stream.
     *
     * @param filename the name of the file to be downloaded
     * @param destination the destination stream
     * @param options the download options
     * @deprecated use {@link #downloadToStream(String, OutputStream, GridFSDownloadOptions)} instead.
     */
    @Deprecated
    @SuppressWarnings("deprecation")
    void downloadToStreamByName(String filename, OutputStream destination,
                                com.mongodb2.client.gridfs.model.GridFSDownloadByNameOptions options);

}
