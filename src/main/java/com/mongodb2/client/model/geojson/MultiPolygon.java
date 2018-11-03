/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb2.client.model.geojson;

import java.util.Collections;
import java.util.List;

import static com.mongodb2.assertions.Assertions.isTrueArgument;
import static com.mongodb2.assertions.Assertions.notNull;

/**
 * A representation of a GeoJSON MultiPolygon.
 *
 * @since 3.1
 */
public final class MultiPolygon extends Geometry {

    private final List<PolygonCoordinates> coordinates;

    /**
     * Construct an instance.
     *
     * @param coordinates the coordinates
     */
    public MultiPolygon(final List<PolygonCoordinates> coordinates) {
        this(null, coordinates);
    }

    /**
     * Construct an instance.
     *
     * @param coordinateReferenceSystem the coordinate reference system
     * @param coordinates the coordinates
     */
    public MultiPolygon(final CoordinateReferenceSystem coordinateReferenceSystem, final List<PolygonCoordinates> coordinates) {
        super(coordinateReferenceSystem);
        notNull("coordinates", coordinates);
        isTrueArgument("coordinates has no null elements", !coordinates.contains(null));
        this.coordinates = Collections.unmodifiableList(coordinates);
    }

    @Override
    public GeoJsonObjectType getType() {
        return GeoJsonObjectType.MULTI_POLYGON;
    }

    /**
     * Gets the coordinates.
     *
     * @return the coordinates
     */
    public List<PolygonCoordinates> getCoordinates() {
        return coordinates;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        MultiPolygon that = (MultiPolygon) o;

        if (!coordinates.equals(that.coordinates)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + coordinates.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "MultiPolygon{"
               + "coordinates=" + coordinates
               + ((getCoordinateReferenceSystem() == null) ? "" : ", coordinateReferenceSystem=" + getCoordinateReferenceSystem())
               + '}';
    }
}
