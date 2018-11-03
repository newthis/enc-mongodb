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

package org.bson2.codecs;

/**
 * The context for decoding values to BSON.  Currently this is a placeholder, as there is nothing needed yet.
 *
 * @see org.bson2.codecs.Decoder
 * @since 3.0
 */
public final class DecoderContext {
    /**
     * Create a builder.
     *
     * @return the builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder for {@code DecoderContext} instances.
     */
    public static final class Builder {
        private Builder() {
        }

        /**
         * Build an instance of {@code DecoderContext}.
         * @return the decoder context
         */
        public DecoderContext build() {
            return new DecoderContext(this);
        }
    }

    private DecoderContext(final Builder builder) {
    }
}
