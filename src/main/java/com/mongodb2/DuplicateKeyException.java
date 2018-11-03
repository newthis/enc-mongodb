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

import org.bson2.BsonDocument;

/**
 * Subclass of {@link WriteConcernException} representing a duplicate key exception
 *
 * @since 2.12
 */
public class DuplicateKeyException extends WriteConcernException {

    private static final long serialVersionUID = -4415279469780082174L;

    /**
     * Construct an instance.
     *
     * @param response the response from the server
     * @param address the server address
     * @param writeConcernResult the write concern result
     */
    public DuplicateKeyException(final BsonDocument response, final ServerAddress address, final WriteConcernResult writeConcernResult) {
        super(response, address, writeConcernResult);
    }
}
