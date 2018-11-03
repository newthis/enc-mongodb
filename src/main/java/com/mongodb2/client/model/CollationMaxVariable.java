/*
 * Copyright 2016 MongoDB, Inc.
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

package com.mongodb2.client.model;

import static java.lang.String.format;

/**
 * Collation support allows the specific configuration of whether or not spaces and punctuation are considered base characters.
 *
 * <p>{@code CollationMaxVariable} controls which characters are affected by {@link CollationAlternate#SHIFTED}.</p>
 *
 * @see CollationAlternate#SHIFTED
 * @since 3.4
 * @mongodb.server.release 3.4
 */
public enum CollationMaxVariable {
    /**
     * Punct
     *
     * <p>Both punctuation and spaces are affected.</p>
     */
    PUNCT("punct"),

    /**
     * Shifted
     *
     * <p>Only spaces are affected.</p>
     */
    SPACE("space");

    private final String value;
    CollationMaxVariable(final String caseFirst) {
        this.value = caseFirst;
    }

    /**
     * @return the String representation of the collation case first value
     */
    public String getValue() {
        return value;
    }

    /**
     * Returns the CollationMaxVariable from the string value.
     *
     * @param collationMaxVariable the string value.
     * @return the read concern
     */
    public static CollationMaxVariable fromString(final String collationMaxVariable) {
        if (collationMaxVariable != null) {
            for (CollationMaxVariable maxVariable : CollationMaxVariable.values()) {
                if (collationMaxVariable.equals(maxVariable.value)) {
                    return maxVariable;
                }
            }
        }
        throw new IllegalArgumentException(format("'%s' is not a valid collationMaxVariable", collationMaxVariable));
    }
}
