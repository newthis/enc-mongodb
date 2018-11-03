/*
 * Copyright 2015 MongoDB, Inc.
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

package com.mongodb2.client.model.geojson;

/**
 * An enumeration of the GeoJSON coordinate reference system types.
 *
 * @since 3.1
 */
public enum CoordinateReferenceSystemType {
    /**
     *  A coordinate reference system that is specifed by name
     */
    NAME("name"),

    /**
     *  A coordinate reference system that is specifed by a dereferenceable URI
     */
    LINK("link");

    /**
     * Gets the GeoJSON-defined name for the type.
     *
     * @return the GeoJSON-defined type name
     */
    public String getTypeName() {
        return typeName;
    }

    private final String typeName;

    CoordinateReferenceSystemType(final String typeName) {
        this.typeName = typeName;
    }
}
