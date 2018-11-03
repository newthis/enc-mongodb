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

package com.mongodb2.client.model;

import org.bson2.BsonBoolean;
import org.bson2.BsonDocument;
import org.bson2.BsonDocumentWriter;
import org.bson2.BsonInt32;
import org.bson2.BsonString;
import org.bson2.codecs.configuration.CodecRegistry;
import org.bson2.conversions.Bson;

import java.util.List;

import static java.util.Arrays.asList;
import static org.bson2.assertions.Assertions.notNull;

/**
 * Builders for aggregation pipeline stages.
 *
 * @mongodb.driver.manual core/aggregation-pipeline/ Aggregation pipeline
 * @mongodb.server.release 2.2
 * @since 3.1
 */
public final class Aggregates {

    /**
     * Creates an $addFields pipeline stage
     *
     * @param fields        the fields to add
     * @return the $addFields pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/addFields/ $addFields
     * @mongodb.server.release 3.4
     * @since 3.4
     */
    public static Bson addFields(final Field<?>... fields) {
        return addFields(asList(fields));
    }

    /**
     * Creates an $addFields pipeline stage
     *
     * @param fields        the fields to add
     * @return the $addFields pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/addFields/ $addFields
     * @mongodb.server.release 3.4
     * @since 3.4
     */
    public static Bson addFields(final List<Field<?>> fields) {
        return new AddFieldsStage(fields);
    }

    /**
     * Creates a $bucket pipeline stage
     *
     * @param <TExpression> the groupBy expression type
     * @param <Boundary>    the boundary type
     * @param groupBy       the criteria to group By
     * @param boundaries    the boundaries of the buckets
     * @return the $bucket pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/bucket/ $bucket
     * @mongodb.server.release 3.4
     * @since 3.4
     */
    public static <TExpression, Boundary> Bson bucket(final TExpression groupBy, final List<Boundary> boundaries) {
        return bucket(groupBy, boundaries, new BucketOptions());
    }

    /**
     * Creates a $bucket pipeline stage
     *
     * @param <TExpression> the groupBy expression type
     * @param <TBoundary>    the boundary type
     * @param groupBy       the criteria to group By
     * @param boundaries    the boundaries of the buckets
     * @param options       the optional values for the $bucket stage
     * @return the $bucket pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/bucket/ $bucket
     * @mongodb.server.release 3.4
     * @since 3.4
     */
    public static <TExpression, TBoundary> Bson bucket(final TExpression groupBy, final List<TBoundary> boundaries,
                                                       final BucketOptions options) {
        return new BucketStage<TExpression, TBoundary>(groupBy, boundaries, options);
    }

    /**
     * Creates a $bucketAuto pipeline stage
     *
     * @param <TExpression> the groupBy expression type
     * @param groupBy       the criteria to group By
     * @param buckets       the number of the buckets
     * @return the $bucketAuto pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/bucketAuto/ $bucketAuto
     * @mongodb.server.release 3.4
     * @since 3.4
     */
    public static <TExpression> Bson bucketAuto(final TExpression groupBy, final int buckets) {
        return bucketAuto(groupBy, buckets, new BucketAutoOptions());
    }

    /**
     * Creates a $bucketAuto pipeline stage
     *
     * @param <TExpression> the groupBy expression type
     * @param groupBy       the criteria to group By
     * @param buckets       the number of the buckets
     * @param options       the optional values for the $bucketAuto stage
     * @return the $bucketAuto pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/bucketAuto/ $bucketAuto
     * @mongodb.server.release 3.4
     * @since 3.4
     */
    public static <TExpression> Bson bucketAuto(final TExpression groupBy, final int buckets, final BucketAutoOptions options) {
        return new BucketAutoStage<TExpression>(groupBy, buckets, options);
    }

    /**
     * Creates a $count pipeline stage using the field name "count" to store the result
     *
     * @return the $count pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/count/ $count
     * @mongodb.server.release 3.4
     * @since 3.4
     */
    public static Bson count() {
        return count("count");
    }

    /**
     * Creates a $count pipeline stage using the named field to store the result
     *
     * @param field the field in which to store the count
     * @return the $count pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/count/ $count
     * @mongodb.server.release 3.4
     * @since 3.4
     */
    public static Bson count(final String field) {
        return new BsonDocument("$count", new BsonString(field));
    }

    /**
     * Creates a $match pipeline stage for the specified filter
     *
     * @param filter the filter to match
     * @return the $match pipeline stage
     * @see Filters
     * @mongodb.driver.manual reference/operator/aggregation/match/ $match
     */
    public static Bson match(final Bson filter) {
        return new SimplePipelineStage("$match", filter);
    }

    /**
     * Creates a $project pipeline stage for the specified projection
     *
     * @param projection the projection
     * @return the $project pipeline stage
     * @see Projections
     * @mongodb.driver.manual reference/operator/aggregation/project/ $project
     */
    public static Bson project(final Bson projection) {
        return new SimplePipelineStage("$project", projection);
    }

    /**
     * Creates a $sort pipeline stage for the specified sort specification
     *
     * @param sort the sort specification
     * @return the $sort pipeline stage
     * @see Sorts
     * @mongodb.driver.manual reference/operator/aggregation/sort/#sort-aggregation $sort
     */
    public static Bson sort(final Bson sort) {
        return new SimplePipelineStage("$sort", sort);
    }

    /**
     * Creates a $sortByCount pipeline stage for the specified filter
     *
     * @param <TExpression> the expression type
     * @param filter        the filter specification
     * @return the $sortByCount pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/sortByCount/ $sortByCount
     * @mongodb.server.release 3.4
     * @since 3.4
     */
    public static <TExpression> Bson sortByCount(final TExpression filter) {
        return new SortByCountStage<TExpression>(filter);
    }

    /**
     * Creates a $skip pipeline stage
     *
     * @param skip the number of documents to skip
     * @return the $skip pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/skip/ $skip
     */
    public static Bson skip(final int skip) {
        return new BsonDocument("$skip", new BsonInt32(skip));
    }

    /**
     * Creates a $limit pipeline stage for the specified filter
     *
     * @param limit the limit
     * @return the $limit pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/limit/  $limit
     */
    public static Bson limit(final int limit) {
        return new BsonDocument("$limit", new BsonInt32(limit));
    }

    /**
     * Creates a $lookup pipeline stage for the specified filter
     *
     * @param from         the name of the collection in the same database to perform the join with.
     * @param localField   specifies the field from the local collection to match values against.
     * @param foreignField specifies the field in the from collection to match values against.
     * @param as           the name of the new array field to add to the input documents.
     * @return the $lookup pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/lookup/ $lookup
     * @mongodb.server.release 3.2
     * @since 3.2
     */
    public static Bson lookup(final String from, final String localField, final String foreignField, final String as) {
        return new BsonDocument("$lookup", new BsonDocument("from", new BsonString(from))
                                                   .append("localField", new BsonString(localField))
                                                   .append("foreignField", new BsonString(foreignField))
                                                   .append("as", new BsonString(as)));
    }

    /**
     * Creates a facet pipeline stage
     *
     * @param facets the facets to use
     * @return the new pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/facet/ $facet
     * @mongodb.server.release 3.4
     * @since 3.4
     */
    public static Bson facet(final List<Facet> facets) {
        return new FacetStage(facets);
    }

    /**
     * Creates a facet pipeline stage
     *
     * @param facets the facets to use
     * @return the new pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/facet/ $facet
     * @mongodb.server.release 3.4
     * @since 3.4
     */
    public static Bson facet(final Facet... facets) {
        return new FacetStage(asList(facets));
    }

    /**
     * Creates a graphLookup pipeline stage for the specified filter
     *
     * @param <TExpression>     the expression type
     * @param from             the collection to query
     * @param startWith        the expression to start the graph lookup with
     * @param connectFromField the from field
     * @param connectToField   the to field
     * @param as               name of field in output document
     * @return the $graphLookup pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/graphLookup/ $graphLookup
     * @mongodb.server.release 3.4
     * @since 3.4
     */
    public static <TExpression> Bson graphLookup(final String from, final TExpression startWith, final String connectFromField,
                                                 final String connectToField, final String as) {
        return graphLookup(from, startWith, connectFromField, connectToField, as, new GraphLookupOptions());
    }

    /**
     * Creates a graphLookup pipeline stage for the specified filter
     *
     * @param <TExpression>    the expression type
     * @param from             the collection to query
     * @param startWith        the expression to start the graph lookup with
     * @param connectFromField the from field
     * @param connectToField   the to field
     * @param as               name of field in output document
     * @param options          optional values for the graphLookup
     * @return the $graphLookup pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/graphLookup/ $graphLookup
     * @mongodb.server.release 3.4
     * @since 3.4
     */
    public static <TExpression> Bson graphLookup(final String from, final TExpression startWith, final String connectFromField,
                                                 final String connectToField, final String as, final GraphLookupOptions options) {
        notNull("options", options);
        return new GraphLookupStage<TExpression>(from, startWith, connectFromField, connectToField, as, options);
    }

    /**
     * Creates a $group pipeline stage for the specified filter
     *
     * @param <TExpression>     the expression type
     * @param id                the id expression for the group
     * @param fieldAccumulators zero or more field accumulator pairs
     * @return the $group pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/group/ $group
     * @mongodb.driver.manual meta/aggregation-quick-reference/#aggregation-expressions Expressions
     */
    public static <TExpression> Bson group(final TExpression id, final BsonField... fieldAccumulators) {
        return group(id, asList(fieldAccumulators));
    }

    /**
     * Creates a $group pipeline stage for the specified filter
     *
     * @param <TExpression>     the expression type
     * @param id                the id expression for the group
     * @param fieldAccumulators zero or more field accumulator pairs
     * @return the $group pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/group/ $group
     * @mongodb.driver.manual meta/aggregation-quick-reference/#aggregation-expressions Expressions
     */
    public static <TExpression> Bson group(final TExpression id, final List<BsonField> fieldAccumulators) {
        return new GroupStage<TExpression>(id, fieldAccumulators);
    }

    /**
     * Creates a $unwind pipeline stage for the specified field name, which must be prefixed by a {@code '$'} sign.
     *
     * @param fieldName the field name, prefixed by a {@code '$' sign}
     * @return the $unwind pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/unwind/ $unwind
     */
    public static Bson unwind(final String fieldName) {
        return new BsonDocument("$unwind", new BsonString(fieldName));
    }

    /**
     * Creates a $unwind pipeline stage for the specified field name, which must be prefixed by a {@code '$'} sign.
     *
     * @param fieldName     the field name, prefixed by a {@code '$' sign}
     * @param unwindOptions options for the unwind pipeline stage
     * @return the $unwind pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/unwind/ $unwind
     * @mongodb.server.release 3.2
     * @since 3.2
     */
    public static Bson unwind(final String fieldName, final UnwindOptions unwindOptions) {
        notNull("unwindOptions", unwindOptions);
        BsonDocument options = new BsonDocument("path", new BsonString(fieldName));
        if (unwindOptions.isPreserveNullAndEmptyArrays() != null) {
            options.append("preserveNullAndEmptyArrays", BsonBoolean.valueOf(unwindOptions.isPreserveNullAndEmptyArrays()));
        }
        if (unwindOptions.getIncludeArrayIndex() != null) {
            options.append("includeArrayIndex", new BsonString(unwindOptions.getIncludeArrayIndex()));
        }
        return new BsonDocument("$unwind", options);
    }

    /**
     * Creates a $out pipeline stage for the specified filter
     *
     * @param collectionName the collection name
     * @return the $out pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/out/  $out
     * @mongodb.server.release 2.6
     */
    public static Bson out(final String collectionName) {
        return new BsonDocument("$out", new BsonString(collectionName));
    }

    /**
     * Creates a $replaceRoot pipeline stage
     *
     * @param <TExpression> the new root type
     * @param value         the new root value
     * @return the $replaceRoot pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/replaceRoot/ $replaceRoot
     * @mongodb.server.release 3.4
     * @since 3.4
     */
    public static <TExpression> Bson replaceRoot(final TExpression value) {
        return new ReplaceRootStage<TExpression>(value);
    }

    /**
     * Creates a $sample pipeline stage with the specified sample size
     *
     * @param size the sample size
     * @return the $sample pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/sample/  $sample
     * @mongodb.server.release 3.2
     * @since 3.2
     */
    public static Bson sample(final int size) {
        return new BsonDocument("$sample", new BsonDocument("size", new BsonInt32(size)));
    }

    static void writeBucketOutput(final CodecRegistry codecRegistry, final BsonDocumentWriter writer, final List<BsonField> output) {
        if (output != null) {
            writer.writeName("output");
            writer.writeStartDocument();
            for (BsonField field : output) {
                writer.writeName(field.getName());
                BuildersHelper.encodeValue(writer, field.getValue(), codecRegistry);
            }
            writer.writeEndDocument();
        }
    }

    private static class SimplePipelineStage implements Bson {
        private final String name;
        private final Bson value;

        public SimplePipelineStage(final String name, final Bson value) {
            this.name = name;
            this.value = value;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
            return new BsonDocument(name, value.toBsonDocument(documentClass, codecRegistry));
        }

        @Override
        public String toString() {
            return "Stage{"
                           + "name='" + name + '\''
                           + ", value=" + value
                           + '}';
        }
    }

    private static final class BucketStage<TExpression, TBoundary> implements Bson {

        private final TExpression groupBy;
        private final List<TBoundary> boundaries;
        private final BucketOptions options;

        BucketStage(final TExpression groupBy, final List<TBoundary> boundaries, final BucketOptions options) {
            notNull("options", options);
            this.groupBy = groupBy;
            this.boundaries = boundaries;
            this.options = options;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> tDocumentClass, final CodecRegistry codecRegistry) {
            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());

            writer.writeStartDocument();

            writer.writeStartDocument("$bucket");

            writer.writeName("groupBy");
            BuildersHelper.encodeValue(writer, groupBy, codecRegistry);

            writer.writeStartArray("boundaries");
            for (TBoundary boundary : boundaries) {
                BuildersHelper.encodeValue(writer, boundary, codecRegistry);
            }
            writer.writeEndArray();
            if (options.getDefaultBucket() != null) {
                writer.writeName("default");
                BuildersHelper.encodeValue(writer, options.getDefaultBucket(), codecRegistry);
            }
            writeBucketOutput(codecRegistry, writer, options.getOutput());
            writer.writeEndDocument();

            return writer.getDocument();
        }

        @Override
        public String toString() {
            return "Stage{"
                + "name='$bucket'"
                + "boundaries=" + boundaries
                + ", groupBy=" + groupBy
                + ", options=" + options
                + '}';
        }
    }

    private static final class BucketAutoStage<TExpression> implements Bson {

        private final TExpression groupBy;
        private final int buckets;
        private final BucketAutoOptions options;

        BucketAutoStage(final TExpression groupBy, final int buckets, final BucketAutoOptions options) {
            notNull("options", options);
            this.groupBy = groupBy;
            this.buckets = buckets;
            this.options = options;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> tDocumentClass, final CodecRegistry codecRegistry) {
            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());

            writer.writeStartDocument();

            writer.writeStartDocument("$bucketAuto");

            writer.writeName("groupBy");
            BuildersHelper.encodeValue(writer, groupBy, codecRegistry);

            writer.writeInt32("buckets", buckets);

            writeBucketOutput(codecRegistry, writer, options.getOutput());

            if (options.getGranularity() != null) {
                writer.writeString("granularity", options.getGranularity().getValue());
            }
            writer.writeEndDocument();

            return writer.getDocument();
        }

        @Override
        public String toString() {
            return "Stage{"
                + "name='$bucketAuto'"
                + "buckets=" + buckets
                + ", groupBy=" + groupBy
                + ", options=" + options
                + '}';
        }
    }

    private static final class GraphLookupStage<TExpression> implements Bson {
        private final String from;
        private final TExpression startWith;
        private final String connectFromField;
        private final String connectToField;
        private final String as;
        private final GraphLookupOptions options;

        private GraphLookupStage(final String from, final TExpression startWith, final String connectFromField, final String connectToField,
                                 final String as, final GraphLookupOptions options) {
            this.from = from;
            this.startWith = startWith;
            this.connectFromField = connectFromField;
            this.connectToField = connectToField;
            this.as = as;
            this.options = options;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> tDocumentClass, final CodecRegistry codecRegistry) {
            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());

            writer.writeStartDocument();

            writer.writeStartDocument("$graphLookup");

            writer.writeString("from", from);
            writer.writeName("startWith");
            BuildersHelper.encodeValue(writer, startWith, codecRegistry);

            writer.writeString("connectFromField", connectFromField);
            writer.writeString("connectToField", connectToField);
            writer.writeString("as", as);
            if (options.getMaxDepth() != null) {
                writer.writeInt32("maxDepth", options.getMaxDepth());
            }
            if (options.getDepthField() != null) {
                writer.writeString("depthField", options.getDepthField());
            }

            writer.writeEndDocument();

            return writer.getDocument();
        }

        @Override
        public String toString() {
            return "Stage{"
                + "name='$graphLookup'"
                + "as='" + as + '\''
                + ", connectFromField='" + connectFromField + '\''
                + ", connectToField='" + connectToField + '\''
                + ", from='" + from + '\''
                + ", options=" + options
                + ", startWith=" + startWith
                + '}';
        }
    }

    private static class GroupStage<TExpression> implements Bson {
        private final TExpression id;
        private final List<BsonField> fieldAccumulators;

        GroupStage(final TExpression id, final List<BsonField> fieldAccumulators) {
            this.id = id;
            this.fieldAccumulators = fieldAccumulators;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> tDocumentClass, final CodecRegistry codecRegistry) {
            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());

            writer.writeStartDocument();

            writer.writeStartDocument("$group");

            writer.writeName("_id");
            BuildersHelper.encodeValue(writer, id, codecRegistry);

            for (BsonField fieldAccumulator : fieldAccumulators) {
                writer.writeName(fieldAccumulator.getName());
                BuildersHelper.encodeValue(writer, fieldAccumulator.getValue(), codecRegistry);
            }

            writer.writeEndDocument();
            writer.writeEndDocument();

            return writer.getDocument();
        }

        @Override
        public String toString() {
            return "Stage{"
                           + "name='$group'"
                           + ", id=" + id
                           + ", fieldAccumulators=" + fieldAccumulators
                           + '}';
        }
    }

    private static class SortByCountStage<TExpression> implements Bson {
        private final TExpression filter;

        SortByCountStage(final TExpression filter) {
            this.filter = filter;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> tDocumentClass, final CodecRegistry codecRegistry) {
            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());

            writer.writeStartDocument();

            writer.writeName("$sortByCount");
            BuildersHelper.encodeValue(writer, filter, codecRegistry);

            writer.writeEndDocument();

            return writer.getDocument();
        }

        @Override
        public String toString() {
            return "Stage{"
                + "name='$sortByCount'"
                + ", id=" + filter
                + '}';
        }
    }

    private static class FacetStage implements Bson {

        private final List<Facet> facets;
        FacetStage(final List<Facet> facets) {
            this.facets = facets;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> tDocumentClass, final CodecRegistry codecRegistry) {
            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
            writer.writeStartDocument();
            writer.writeName("$facet");
            writer.writeStartDocument();
            for (Facet facet : facets) {
                writer.writeName(facet.getName());
                writer.writeStartArray();
                for (Bson bson : facet.getPipeline()) {
                    BuildersHelper.encodeValue(writer, bson, codecRegistry);
                }
                writer.writeEndArray();
            }
            writer.writeEndDocument();
            writer.writeEndDocument();

            return writer.getDocument();
        }

        @Override
        public String toString() {
            return "Stage{"
                + "name='$fact', "
                + "facets=" + facets + '}';
        }

    }

    private static class AddFieldsStage implements Bson {
        private final List<Field<?>> fields;

        AddFieldsStage(final List<Field<?>> fields) {
            this.fields = fields;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> tDocumentClass, final CodecRegistry codecRegistry) {
            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
            writer.writeStartDocument();
            writer.writeName("$addFields");
            writer.writeStartDocument();
            for (Field<?> field : fields) {
                writer.writeName(field.getName());
                BuildersHelper.encodeValue(writer, field.getValue(), codecRegistry);
            }
            writer.writeEndDocument();
            writer.writeEndDocument();

            return writer.getDocument();
        }

        @Override
        public String toString() {
            return "Stage{"
                + "name='$addFields', "
                + "fields=" + fields
                + '}';
        }
    }

    private static class ReplaceRootStage<TExpression> implements Bson {
        private final TExpression value;

        ReplaceRootStage(final TExpression value) {
            this.value = value;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> tDocumentClass, final CodecRegistry codecRegistry) {
            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
            writer.writeStartDocument();
            writer.writeName("$replaceRoot");
            writer.writeStartDocument();
            writer.writeName("newRoot");
            BuildersHelper.encodeValue(writer, value, codecRegistry);
            writer.writeEndDocument();
            writer.writeEndDocument();

            return writer.getDocument();
        }

        @Override
        public String toString() {
            return "Stage{"
                + "name='$replaceRoot', "
                + "value=" + value
                + '}';
        }
    }

    private Aggregates() {
    }
}
