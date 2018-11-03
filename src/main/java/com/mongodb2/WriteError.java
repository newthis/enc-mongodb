/*
 * Copyright 2014-2015 MongoDB, Inc.
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

import static com.mongodb2.assertions.Assertions.notNull;

/**
 * Represents the details of a write error , e.g. a duplicate key error
 *
 * @since 3.0
 */
public class WriteError {
    private final int code;
    private final String message;
    private final BsonDocument details;

    /**
     * Constructs a new instance.
     *
     * @param code    the error code
     * @param message the error message
     * @param details details about the error
     */
    public WriteError(final int code, final String message, final BsonDocument details) {
        this.code = code;
        this.message = notNull("message", message);
        this.details = notNull("details", details);
    }

    /**
     * Construct an instance that is a shallow copy of the given instance.
     *
     * @param writeError the write error to copy
     */
    public WriteError(final WriteError writeError) {
        this.code = writeError.code;
        this.message = writeError.message;
        this.details = writeError.details;
    }

    /**
     * Gets the category of this error.
     *
     * @return the category of this write error
     */
    public ErrorCategory getCategory() {
        return ErrorCategory.fromErrorCode(code);
    }

    /**
     * Gets the code associated with this error.
     *
     * @return the code
     */
    public int getCode() {
        return code;
    }

    /**
     * Gets the message associated with this error.
     *
     * @return the message
     */
    public String getMessage() {
        return message;
    }

    /**
     * Gets the details associated with this error.  This document will not be null, but may be empty.
     *
     * @return the details
     */
    public BsonDocument getDetails() {
        return details;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        WriteError that = (WriteError) o;

        if (code != that.code) {
            return false;
        }
        if (!details.equals(that.details)) {
            return false;
        }
        if (!message.equals(that.message)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = code;
        result = 31 * result + message.hashCode();
        result = 31 * result + details.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "WriteError{"
               + "code=" + code
               + ", message='" + message + '\''
               + ", details=" + details
               + '}';
    }
}
