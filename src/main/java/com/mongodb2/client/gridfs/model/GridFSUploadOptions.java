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

package com.mongodb2.client.gridfs.model;

import org.bson2.Document;

/**
 * GridFS upload options
 *
 * Customizable options used when uploading files into GridFS
 *
 * @since 3.1
 */
public final class GridFSUploadOptions {
    private Integer chunkSizeBytes;
    private Document metadata;

    /**
     * Construct a new instance.
     */
    public GridFSUploadOptions() {
    }

    /**
     * The number of bytes per chunk of this file.
     *
     * <p>If no value has been set then, the chunkSizeBytes from the GridFSBucket will be used.</p>
     *
     * @return number of bytes per chunk if set or null
     */
    public Integer getChunkSizeBytes() {
        return chunkSizeBytes;
    }

    /**
     * Sets the chunk size in bytes.
     *
     * @param chunkSizeBytes the number of bytes per chunk for the uploaded file
     * @return this
     */
    public GridFSUploadOptions chunkSizeBytes(final Integer chunkSizeBytes) {
        this.chunkSizeBytes = chunkSizeBytes;
        return this;
    }

    /**
     * Returns any user provided data for the 'metadata' field of the files collection document.
     *
     * @return the user provided metadata for the file if set or null
     */
    public Document getMetadata() {
        return metadata;
    }

    /**
     * Sets metadata to stored alongside the filename in the files collection
     *
     * @param metadata the metadata to be stored
     * @return this
     */
    public GridFSUploadOptions metadata(final Document metadata) {
        this.metadata = metadata;
        return this;
    }

}
