/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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

package org.bson2;

/**
 * Represent the maximum key value regardless of the key's type
 */
public final class BsonMaxKey extends BsonValue {

    @Override
    public BsonType getBsonType() {
        return BsonType.MAX_KEY;
    }

    @Override
    public boolean equals(final Object o) {
        return o instanceof BsonMaxKey;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public String toString() {
        return "BsonMaxKey";
    }

}
