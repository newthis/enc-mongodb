/*
 * Copyright 2016 MongoDB, Inc.
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

package com.mongodb2.client.model;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

/**
 * The options for a $bucketAuto aggregation pipeline stage
 *
 * @mongodb.driver.manual reference/operator/aggregation/bucketAuto/ $bucketAuto
 * @mongodb.server.release 3.4
 * @since 3.4
 */
public class BucketAutoOptions {
    private List<BsonField> output;
    private BucketGranularity granularity;

    /**
     * @return the granularity of the bucket definitions
     */
    public BucketGranularity getGranularity() {
        return granularity;
    }

    /**
     * @return the output document definition
     */
    public List<BsonField> getOutput() {
        return output == null ? null : new ArrayList<BsonField>(output);
    }

    /**
     * Specifies the granularity of the bucket definitions.
     *
     * @param granularity the granularity of the bucket definitions
     * @return this
     * @see <a href="https://en.wikipedia.org/wiki/Preferred_number">Preferred numbers</a>
     * @see BucketGranularity
     */
    public BucketAutoOptions granularity(final BucketGranularity granularity) {
        this.granularity = granularity;
        return this;
    }

    /**
     * The definition of the output document in each bucket
     *
     * @param output the output document definition
     * @return this
     */
    public BucketAutoOptions output(final BsonField... output) {
        this.output = asList(output);
        return this;
    }

    /**
     * The definition of the output document in each bucket
     *
     * @param output the output document definition
     * @return this
     */
    public BucketAutoOptions output(final List<BsonField> output) {
        this.output = output;
        return this;
    }

}
