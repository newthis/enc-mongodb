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

package com.mongodb2.client.model.geojson.codecs;

import com.mongodb2.client.model.geojson.Point;
import org.bson2.BsonReader;
import org.bson2.BsonWriter;
import org.bson2.codecs.Codec;
import org.bson2.codecs.DecoderContext;
import org.bson2.codecs.EncoderContext;
import org.bson2.codecs.configuration.CodecRegistry;

import static com.mongodb2.assertions.Assertions.notNull;
import static com.mongodb2.client.model.geojson.codecs.GeometryCodecHelper.encodeGeometry;
import static com.mongodb2.client.model.geojson.codecs.GeometryCodecHelper.encodePosition;

/**
 * A Codec for a GeoJSON point.
 *
 * @since 3.1
 */
public class PointCodec implements Codec<Point> {
    private final CodecRegistry registry;

    /**
     * Constructs a new instance.
     *
     * @param registry the registry
     */
    public PointCodec(final CodecRegistry registry) {
        this.registry = notNull("registry", registry);
    }

    @Override
    public void encode(final BsonWriter writer, final Point value, final EncoderContext encoderContext) {
        encodeGeometry(writer, value, encoderContext, registry, new Runnable() {
            @Override
            @SuppressWarnings({"unchecked", "rawtypes"})
            public void run() {
                encodePosition(writer, value.getPosition());
            }
        });
    }

    @Override
    public Class<Point> getEncoderClass() {
        return Point.class;
    }

    @Override
    public Point decode(final BsonReader reader, final DecoderContext decoderContext) {
        throw new UnsupportedOperationException("Not implemented yet!");
    }
}
