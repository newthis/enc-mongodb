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

import java.util.Collections;
import java.util.List;

import static com.mongodb2.assertions.Assertions.isTrueArgument;
import static com.mongodb2.assertions.Assertions.notNull;

/**
 * A representation of a GeoJSON MultiPoint.
 *
 * @since 3.1
 */
public final class MultiPoint extends Geometry {

    private final List<Position> coordinates;

    /**
     * Construct an instance with the given coordinates.
     *
     * @param coordinates the coordinates
     */
    public MultiPoint(final List<Position> coordinates) {
        this(null, coordinates);
    }

    /**
     * Construct an instance with the given coordinates and coordinate reference system.
     *
     * @param coordinateReferenceSystem the coordinate reference system
     * @param coordinates               the coordinates
     */
    public MultiPoint(final CoordinateReferenceSystem coordinateReferenceSystem, final List<Position> coordinates) {
        super(coordinateReferenceSystem);
        notNull("coordinates", coordinates);
        isTrueArgument("coordinates contains only non-null positions", !coordinates.contains(null));

        this.coordinates = Collections.unmodifiableList(coordinates);
    }

    @Override
    public GeoJsonObjectType getType() {
        return GeoJsonObjectType.MULTI_POINT;
    }

    /**
     * Gets the GeoJSON coordinates of this MultiPoint.
     *
     * @return the coordinates
     */
    public List<Position> getCoordinates() {
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

        MultiPoint multiPoint = (MultiPoint) o;

        if (!coordinates.equals(multiPoint.coordinates)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        return 31 * result + coordinates.hashCode();
    }

    @Override
    public String toString() {
        return "MultiPoint{"
               + "coordinates=" + coordinates
               + ((getCoordinateReferenceSystem() == null) ? "" : ", coordinateReferenceSystem=" + getCoordinateReferenceSystem())
               + '}';
    }
}
