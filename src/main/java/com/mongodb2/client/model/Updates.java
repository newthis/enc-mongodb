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

import org.bson2.BsonDocument;
import org.bson2.BsonDocumentWriter;
import org.bson2.BsonInt32;
import org.bson2.BsonInt64;
import org.bson2.BsonString;
import org.bson2.BsonValue;
import org.bson2.codecs.configuration.CodecRegistry;
import org.bson2.conversions.Bson;

import java.util.List;
import java.util.Map;

import static com.mongodb2.assertions.Assertions.notNull;
import static com.mongodb2.client.model.BuildersHelper.encodeValue;
import static java.util.Arrays.asList;

/**
 * A factory for document updates. A convenient way to use this class is to statically import all of its methods, which allows usage like:
 * <blockquote><pre>
 *    collection.updateOne(eq("x", 1), set("x", 2));
 * </pre></blockquote>
 *
 * @since 3.1
 * @mongodb.driver.manual reference/operator/update/ Update Operators
 */
public final class Updates {

    /**
     * Combine a list of updates into a single update.
     *
     * @param updates the list of updates
     * @return a combined update
     */
    public static Bson combine(final Bson... updates) {
        return combine(asList(updates));
    }

    /**
     * Combine a list of updates into a single update.
     *
     * @param updates the list of updates
     * @return a combined update
     */
    public static Bson combine(final List<Bson> updates) {
        notNull("updates", updates);
        return new CompositeUpdate(updates);
    }

    /**
     * Creates an update that sets the value of the field with the given name to the given value.
     *
     * @param fieldName the non-null field name
     * @param value     the value
     * @param <TItem>   the value type
     * @return the update
     * @mongodb.driver.manual reference/operator/update/set/ $set
     */
    public static <TItem> Bson set(final String fieldName, final TItem value) {
        return new SimpleUpdate<TItem>(fieldName, value, "$set");
    }

    /**
     * Creates an update that deletes the field with the given name.
     *
     * @param fieldName the non-null field name
     * @return the update
     * @mongodb.driver.manual reference/operator/update/unset/ $unset
     */
    public static Bson unset(final String fieldName) {
        return new SimpleUpdate<String>(fieldName, "", "$unset");
    }

    /**
     * Creates an update that sets the value of the field with the given name to the given value, but only if the update is an upsert that
     * results in an insert of a document.
     *
     * @param fieldName the non-null field name
     * @param value     the value
     * @param <TItem>   the value type
     * @return the update
     * @mongodb.driver.manual reference/operator/update/setOnInsert/ $setOnInsert
     * @see UpdateOptions#upsert(boolean)
     */
    public static <TItem> Bson setOnInsert(final String fieldName, final TItem value) {
        return new SimpleUpdate<TItem>(fieldName, value, "$setOnInsert");
    }

    /**
     * Creates an update that renames a field.
     *
     * @param fieldName    the non-null field name
     * @param newFieldName the non-null new field name
     * @return the update
     * @mongodb.driver.manual reference/operator/update/rename/ $rename
     */
    public static Bson rename(final String fieldName, final String newFieldName) {
        notNull("newFieldName", newFieldName);
        return new SimpleUpdate<String>(fieldName, newFieldName, "$rename");
    }

    /**
     * Creates an update that increments the value of the field with the given name by the given value.
     *
     * @param fieldName the non-null field name
     * @param number    the value
     * @return the update
     * @mongodb.driver.manual reference/operator/update/inc/ $inc
     */
    public static Bson inc(final String fieldName, final Number number) {
        notNull("number", number);
        return new SimpleUpdate<Number>(fieldName, number, "$inc");
    }

    /**
     * Creates an update that multiplies the value of the field with the given name by the given number.
     *
     * @param fieldName the non-null field name
     * @param number    the non-null number
     * @return the update
     * @mongodb.driver.manual reference/operator/update/mul/ $mul
     */
    public static Bson mul(final String fieldName, final Number number) {
        notNull("number", number);
        return new SimpleUpdate<Number>(fieldName, number, "$mul");
    }


    /**
     * Creates an update that sets the value of the field to the given value if the given value is less than the current value of the
     * field.
     *
     * @param fieldName the non-null field name
     * @param value     the value
     * @param <TItem>   the value type
     * @return the update
     * @mongodb.driver.manual reference/operator/update/min/ $min
     */
    public static <TItem> Bson min(final String fieldName, final TItem value) {
        return new SimpleUpdate<TItem>(fieldName, value, "$min");
    }

    /**
     * Creates an update that sets the value of the field to the given value if the given value is greater than the current value of the
     * field.
     *
     * @param fieldName the non-null field name
     * @param value     the value
     * @param <TItem>   the value type
     * @return the update
     * @mongodb.driver.manual reference/operator/update/min/ $min
     */
    public static <TItem> Bson max(final String fieldName, final TItem value) {
        return new SimpleUpdate<TItem>(fieldName, value, "$max");
    }

    /**
     * Creates an update that sets the value of the field to the current date as a BSON date.
     *
     * @param fieldName the non-null field name
     * @return the update
     * @mongodb.driver.manual reference/operator/update/currentDate/ $currentDate
     * @mongodb.driver.manual reference/bson-types/#date Date
     */
    public static Bson currentDate(final String fieldName) {
        return new SimpleUpdate<Boolean>(fieldName, true, "$currentDate");
    }

    /**
     * Creates an update that sets the value of the field to the current date as a BSON timestamp.
     *
     * @param fieldName the non-null field name
     * @return the update
     * @mongodb.driver.manual reference/operator/update/currentDate/ $currentDate
     * @mongodb.driver.manual reference/bson-types/#document-bson-type-timestamp Timestamp
     */
    public static Bson currentTimestamp(final String fieldName) {
        return new SimpleUpdate<BsonDocument>(fieldName, new BsonDocument("$type", new BsonString("timestamp")), "$currentDate");
    }

    /**
     * Creates an update that adds the given value to the array value of the field with the given name, unless the value is
     * already present, in which case it does nothing
     *
     * @param fieldName the non-null field name
     * @param value     the value
     * @param <TItem>   the value type
     * @return the update
     * @mongodb.driver.manual reference/operator/update/addToSet/ $addToSet
     */
    public static <TItem> Bson addToSet(final String fieldName, final TItem value) {
        return new SimpleUpdate<TItem>(fieldName, value, "$addToSet");
    }

    /**
     * Creates an update that adds each of the given values to the array value of the field with the given name, unless the value is
     * already present, in which case it does nothing
     *
     * @param fieldName the non-null field name
     * @param values    the values
     * @param <TItem>   the value type
     * @return the update
     * @mongodb.driver.manual reference/operator/update/addToSet/ $addToSet
     */
    public static <TItem> Bson addEachToSet(final String fieldName, final List<TItem> values) {
        return new WithEachUpdate<TItem>(fieldName, values, "$addToSet");
    }

    /**
     * Creates an update that adds the given value to the array value of the field with the given name.
     *
     * @param fieldName the non-null field name
     * @param value     the value
     * @param <TItem>   the value type
     * @return the update
     * @mongodb.driver.manual reference/operator/update/push/ $push
     */
    public static <TItem> Bson push(final String fieldName, final TItem value) {
        return new SimpleUpdate<TItem>(fieldName, value, "$push");
    }

    /**
     * Creates an update that adds each of the given values to the array value of the field with the given name.
     *
     * @param fieldName the non-null field name
     * @param values    the values
     * @param <TItem>   the value type
     * @return the update
     * @mongodb.driver.manual reference/operator/update/push/ $push
     */
    public static <TItem> Bson pushEach(final String fieldName, final List<TItem> values) {
        return new PushUpdate<TItem>(fieldName, values, new PushOptions());
    }

    /**
     * Creates an update that adds each of the given values to the array value of the field with the given name, applying the given
     * options for positioning the pushed values, and then slicing and/or sorting the array.
     *
     * @param fieldName the non-null field name
     * @param values    the values
     * @param options   the non-null push options
     * @param <TItem>   the value type
     * @return the update
     * @mongodb.driver.manual reference/operator/update/push/ $push
     */
    public static <TItem> Bson pushEach(final String fieldName, final List<TItem> values, final PushOptions options) {
        return new PushUpdate<TItem>(fieldName, values, options);
    }

    /**
     * Creates an update that removes all instances of the given value from the array value of the field with the given name.
     *
     * @param fieldName the non-null field name
     * @param value     the value
     * @param <TItem>   the value type
     * @return the update
     * @mongodb.driver.manual reference/operator/update/pull/ $pull
     */
    public static <TItem> Bson pull(final String fieldName, final TItem value) {
        return new SimpleUpdate<TItem>(fieldName, value, "$pull");
    }

    /**
     * Creates an update that removes from an array all elements that match the given filter.
     *
     * @param filter the query filter
     * @return the update
     * @mongodb.driver.manual reference/operator/update/pull/ $pull
     */
    public static Bson pullByFilter(final Bson filter) {
        return new Bson() {
            @Override
            public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> tDocumentClass, final CodecRegistry codecRegistry) {
                BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());

                writer.writeStartDocument();
                writer.writeName("$pull");

                encodeValue(writer, filter, codecRegistry);

                writer.writeEndDocument();

                return writer.getDocument();
            }
        };
    }

    /**
     * Creates an update that removes all instances of the given values from the array value of the field with the given name.
     *
     * @param fieldName the non-null field name
     * @param values    the values
     * @param <TItem>   the value type
     * @return the update
     * @mongodb.driver.manual reference/operator/update/pull/ $pull
     */
    public static <TItem> Bson pullAll(final String fieldName, final List<TItem> values) {
        return new PullAllUpdate<TItem>(fieldName, values);
    }

    /**
     * Creates an update that pops the first element of an array that is the value of the field with the given name.
     *
     * @param fieldName the non-null field name
     * @return the update
     * @mongodb.driver.manual reference/operator/update/pop/ $pop
     */
    public static Bson popFirst(final String fieldName) {
        return new SimpleUpdate<Integer>(fieldName, -1, "$pop");
    }

    /**
     * Creates an update that pops the last element of an array that is the value of the field with the given name.
     *
     * @param fieldName the non-null field name
     * @return the update
     * @mongodb.driver.manual reference/operator/update/pop/ $pop
     */
    public static Bson popLast(final String fieldName) {
        return new SimpleUpdate<Integer>(fieldName, 1, "$pop");
    }

    /**
     * Creates an update that performs a bitwise and between the given integer value and the integral value of the field with the given
     * name.
     *
     * @param fieldName the field name
     * @param value     the value
     * @return the update
     */
    public static Bson bitwiseAnd(final String fieldName, final int value) {
        return createBitUpdateDocument(fieldName, "and", value);
    }

    /**
     * Creates an update that performs a bitwise and between the given long value and the integral value of the field with the given name.
     *
     * @param fieldName the field name
     * @param value     the value
     * @return the update
     * @mongodb.driver.manual reference/operator/update/bit/ $bit
     */
    public static Bson bitwiseAnd(final String fieldName, final long value) {
        return createBitUpdateDocument(fieldName, "and", value);
    }

    /**
     * Creates an update that performs a bitwise or between the given integer value and the integral value of the field with the given
     * name.
     *
     * @param fieldName the field name
     * @param value     the value
     * @return the update
     * @mongodb.driver.manual reference/operator/update/bit/ $bit
     */
    public static Bson bitwiseOr(final String fieldName, final int value) {
        return createBitUpdateDocument(fieldName, "or", value);
    }

    /**
     * Creates an update that performs a bitwise or between the given long value and the integral value of the field with the given name.
     *
     * @param fieldName the field name
     * @param value     the value
     * @return the update
     * @mongodb.driver.manual reference/operator/update/bit/ $bit
     */
    public static Bson bitwiseOr(final String fieldName, final long value) {
        return createBitUpdateDocument(fieldName, "or", value);
    }

    /**
     * Creates an update that performs a bitwise xor between the given integer value and the integral value of the field with the given
     * name.
     *
     * @param fieldName the field name
     * @param value     the value
     * @return the update
     */
    public static Bson bitwiseXor(final String fieldName, final int value) {
        return createBitUpdateDocument(fieldName, "xor", value);
    }

    /**
     * Creates an update that performs a bitwise xor between the given long value and the integral value of the field with the given name.
     *
     * @param fieldName the field name
     * @param value     the value
     * @return the update
     */
    public static Bson bitwiseXor(final String fieldName, final long value) {
        return createBitUpdateDocument(fieldName, "xor", value);
    }

    private static Bson createBitUpdateDocument(final String fieldName, final String bitwiseOperator, final int value) {
        return createBitUpdateDocument(fieldName, bitwiseOperator, new BsonInt32(value));
    }

    private static Bson createBitUpdateDocument(final String fieldName, final String bitwiseOperator, final long value) {
        return createBitUpdateDocument(fieldName, bitwiseOperator, new BsonInt64(value));
    }

    private static Bson createBitUpdateDocument(final String fieldName, final String bitwiseOperator, final BsonValue value) {
        return new BsonDocument("$bit", new BsonDocument(fieldName, new BsonDocument(bitwiseOperator, value)));
    }

    private static class SimpleUpdate<TItem> implements Bson {
        private final String fieldName;
        private final TItem value;
        private final String operator;

        public SimpleUpdate(final String fieldName, final TItem value, final String operator) {
            this.fieldName = notNull("fieldName", fieldName);
            this.value = value;
            this.operator = operator;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> tDocumentClass, final CodecRegistry codecRegistry) {
            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());

            writer.writeStartDocument();
            writer.writeName(operator);

            writer.writeStartDocument();
            writer.writeName(fieldName);
            encodeValue(writer, value, codecRegistry);
            writer.writeEndDocument();

            writer.writeEndDocument();

            return writer.getDocument();
        }

        @Override
        public String toString() {
            return "Update{"
                           + "fieldName='" + fieldName + '\''
                           + ", operator='" + operator + '\''
                           + ", value=" + value
                           + '}';
        }
    }

    private static class WithEachUpdate<TItem> implements Bson {
        private final String fieldName;
        private final List<TItem> values;
        private final String operator;

        public WithEachUpdate(final String fieldName, final List<TItem> values, final String operator) {
            this.fieldName = notNull("fieldName", fieldName);
            this.values = notNull("values", values);
            this.operator = operator;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> tDocumentClass, final CodecRegistry codecRegistry) {
            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());

            writer.writeStartDocument();
            writer.writeName(operator);

            writer.writeStartDocument();
            writer.writeName(fieldName);
            writer.writeStartDocument();

            writer.writeStartArray("$each");
            for (TItem value : values) {
                encodeValue(writer, value, codecRegistry);
            }
            writer.writeEndArray();

            writeAdditionalFields(writer, tDocumentClass, codecRegistry);

            writer.writeEndDocument();

            writer.writeEndDocument();

            writer.writeEndDocument();

            return writer.getDocument();
        }

        protected <TDocument> void writeAdditionalFields(final BsonDocumentWriter writer, final Class<TDocument> tDocumentClass,
                                                         final CodecRegistry codecRegistry) {
        }


        protected String additionalFieldsToString() {
            return "";
        }

        @Override
        public String toString() {
            return "Each Update{"
                           + "fieldName='" + fieldName + '\''
                           + ", operator='" + operator + '\''
                           + ", values=" + values
                           + additionalFieldsToString()
                           + '}';
        }
    }

    private static class PushUpdate<TItem> extends WithEachUpdate<TItem> {

        private final PushOptions options;

        public PushUpdate(final String fieldName, final List<TItem> values, final PushOptions options) {
            super(fieldName, values, "$push");
            this.options = notNull("options", options);
        }

        @Override
        protected <TDocument> void writeAdditionalFields(final BsonDocumentWriter writer, final Class<TDocument> tDocumentClass,
                                                         final CodecRegistry codecRegistry) {
            if (options.getPosition() != null) {
                writer.writeInt32("$position", options.getPosition());
            }
            if (options.getSlice() != null) {
                writer.writeInt32("$slice", options.getSlice());
            }
            if (options.getSort() != null) {
                writer.writeInt32("$sort", options.getSort());
            } else if (options.getSortDocument() != null) {
                writer.writeName("$sort");
                encodeValue(writer, options.getSortDocument(), codecRegistry);
            }
        }

        @Override
        protected String additionalFieldsToString() {
            return ", options=" + options;
        }
    }

    private static class PullAllUpdate<TItem> implements Bson {
        private final String fieldName;
        private final List<TItem> values;

        public PullAllUpdate(final String fieldName, final List<TItem> values) {
            this.fieldName = notNull("fieldName", fieldName);
            this.values = notNull("values", values);
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> tDocumentClass, final CodecRegistry codecRegistry) {
            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());

            writer.writeStartDocument();
            writer.writeName("$pullAll");

            writer.writeStartDocument();
            writer.writeName(fieldName);

            writer.writeStartArray();
            for (TItem value : values) {
                encodeValue(writer, value, codecRegistry);
            }
            writer.writeEndArray();

            writer.writeEndDocument();

            writer.writeEndDocument();

            return writer.getDocument();
        }

        @Override
        public String toString() {
            return "Update{"
                           + "fieldName='" + fieldName + '\''
                           + ", operator='$pullAll'"
                           + ", value=" + values
                           + '}';
        }
    }

    private static class CompositeUpdate implements Bson {
        private final List<Bson> updates;

        public CompositeUpdate(final List<Bson> updates) {
            this.updates = updates;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> tDocumentClass, final CodecRegistry codecRegistry) {
            BsonDocument document = new BsonDocument();

            for (Bson update : updates) {
                BsonDocument rendered = update.toBsonDocument(tDocumentClass, codecRegistry);
                for (Map.Entry<String, BsonValue> element : rendered.entrySet()) {
                    if (document.containsKey(element.getKey())) {
                        BsonDocument currentOperatorDocument = (BsonDocument) element.getValue();
                        BsonDocument existingOperatorDocument = document.getDocument(element.getKey());
                        for (Map.Entry<String, BsonValue> currentOperationDocumentElements : currentOperatorDocument.entrySet()) {
                            existingOperatorDocument.append(currentOperationDocumentElements.getKey(),
                                    currentOperationDocumentElements.getValue());
                        }
                    } else {
                        document.append(element.getKey(), element.getValue());
                    }
                }
            }

            return document;
        }

        @Override
        public String toString() {
            return "Updates{"
                           + "updates=" + updates
                           + '}';
        }
    }

    private Updates() {
    }
}
