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

import org.bson2.BsonBinarySubType;
import org.bson2.BsonDocument;
import org.bson2.BsonDocumentWriter;
import org.bson2.BsonReader;
import org.bson2.BsonType;
import org.bson2.BsonValue;
import org.bson2.BsonWriter;
import org.bson2.Document;
import org.bson2.Transformer;
import org.bson2.codecs.configuration.CodecRegistry;
import util.ValueConverter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.util.Arrays.asList;
import static org.bson2.assertions.Assertions.notNull;
import static org.bson2.codecs.configuration.CodecRegistries.fromProviders;

/**
 * A Codec for Document instances.
 *
 * @see org.bson2.Document
 * @since 3.0
 */
public class DocumentCodec implements CollectibleCodec<Document> {

    private static final String ID_FIELD_NAME = "_id";
    private static final CodecRegistry DEFAULT_REGISTRY = fromProviders(asList(new ValueCodecProvider(),
            new BsonValueCodecProvider(),
            new DocumentCodecProvider()));
    private static final BsonTypeClassMap DEFAULT_BSON_TYPE_CLASS_MAP = new BsonTypeClassMap();

    private final BsonTypeCodecMap bsonTypeCodecMap;
    private final CodecRegistry registry;
    private final IdGenerator idGenerator;
    private final Transformer valueTransformer;

    /**
     * Construct a new instance with a default {@code CodecRegistry} and
     */
    public DocumentCodec() {
        this(DEFAULT_REGISTRY, DEFAULT_BSON_TYPE_CLASS_MAP);
    }

    /**
     * Construct a new instance with the given registry and BSON type class map.
     *
     * @param registry         the registry
     * @param bsonTypeClassMap the BSON type class map
     */
    public DocumentCodec(final CodecRegistry registry, final BsonTypeClassMap bsonTypeClassMap) {
        this(registry, bsonTypeClassMap, null);
    }

    /**
     * Construct a new instance with the given registry and BSON type class map. The transformer is applied as a last step when decoding
     * values, which allows users of this codec to control the decoding process.  For example, a user of this class could substitute a
     * value decoded as a Document with an instance of a special purpose class (e.g., one representing a DBRef in MongoDB).
     *
     * @param registry         the registry
     * @param bsonTypeClassMap the BSON type class map
     * @param valueTransformer the value transformer to use as a final step when decoding the value of any field in the document
     */
    public DocumentCodec(final CodecRegistry registry, final BsonTypeClassMap bsonTypeClassMap, final Transformer valueTransformer) {
        this.registry = notNull("registry", registry);
        this.bsonTypeCodecMap = new BsonTypeCodecMap(notNull("bsonTypeClassMap", bsonTypeClassMap), registry);
        this.idGenerator = new ObjectIdGenerator();
        this.valueTransformer = valueTransformer != null ? valueTransformer : new Transformer() {
            @Override
            public Object transform(final Object value) {
                return value;
            }
        };
    }

    @Override
    public boolean documentHasId(final Document document) {
        return document.containsKey(ID_FIELD_NAME);
    }

    @Override
    public BsonValue getDocumentId(final Document document) {
        if (!documentHasId(document)) {
            throw new IllegalStateException("The document does not contain an _id");
        }

        Object id = document.get(ID_FIELD_NAME);
        if (id instanceof BsonValue) {
            return (BsonValue) id;
        }

        BsonDocument idHoldingDocument = new BsonDocument();
        BsonWriter writer = new BsonDocumentWriter(idHoldingDocument);
        writer.writeStartDocument();
        writer.writeName(ID_FIELD_NAME);
        writeValue(writer, EncoderContext.builder().build(), id);
        writer.writeEndDocument();
        return idHoldingDocument.get(ID_FIELD_NAME);
    }

    @Override
    public Document generateIdIfAbsentFromDocument(final Document document) {
        if (!documentHasId(document)) {
            document.put(ID_FIELD_NAME, idGenerator.generate());
        }
        return document;
    }

    @Override
    public void encode(final BsonWriter writer, final Document document, final EncoderContext encoderContext) {
        writeMap(writer, document, encoderContext);
    }

    @Override
    public Document decode(final BsonReader reader, final DecoderContext decoderContext) {
        Document document = new Document();

        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String fieldName = reader.readName();
            Object obj = readValue(reader, decoderContext) ;

            if(obj instanceof String){
                String s = (String)obj ;
                document.put(fieldName,ValueConverter.reconvertString(s)) ;


            }else if(obj instanceof Integer){
                int s = (Integer) obj ;
                document.put(fieldName,ValueConverter.reconvertInteger(s)) ;



            }else if(obj instanceof Double){
                double s = (Double) obj ;
                document.put(fieldName,ValueConverter.reconvertDouble(s)) ;


            }else if(obj instanceof Short){
                short s = (Short) obj ;
                document.put(fieldName,ValueConverter.reconvertShort(s)) ;

            }else if(obj instanceof Long){
                Long s = (Long) obj ;
                document.put(fieldName,ValueConverter.reconvertLong(s)) ;

            }else if(obj instanceof Byte){
                Byte s = (Byte) obj ;
                document.put(fieldName,ValueConverter.reconvertByte(s)) ;

            }else if(obj instanceof Boolean){
                Boolean s = (Boolean) obj ;
                document.put(fieldName,ValueConverter.reconvertBoolean(s)) ;

            }
            else{
                document.put(fieldName, obj);
            }


        }

        reader.readEndDocument();

        return document;
    }

    @Override
    public Class<Document> getEncoderClass() {
        return Document.class;
    }

    private void beforeFields(final BsonWriter bsonWriter, final EncoderContext encoderContext, final Map<String, Object> document) {
        if (encoderContext.isEncodingCollectibleDocument() && document.containsKey(ID_FIELD_NAME)) {
            bsonWriter.writeName(ID_FIELD_NAME);
            writeValue(bsonWriter, encoderContext, document.get(ID_FIELD_NAME));
        }
    }

    private boolean skipField(final EncoderContext encoderContext, final String key) {
        return encoderContext.isEncodingCollectibleDocument() && key.equals(ID_FIELD_NAME);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void writeValue(final BsonWriter writer, final EncoderContext encoderContext, final Object value) {
        if (value == null) {
            writer.writeNull();
        } else if (value instanceof Iterable) {
            writeIterable(writer, (Iterable<Object>) value, encoderContext.getChildContext());
        } else if (value instanceof Map) {
            writeMap(writer, (Map<String, Object>) value, encoderContext.getChildContext());
        } else {

            Codec codec = registry.get(value.getClass());


            encoderContext.encodeWithChildContext(codec, writer, value);
        }
    }

    private void writeMap(final BsonWriter writer, final Map<String, Object> map, final EncoderContext encoderContext) {
        writer.writeStartDocument();


        beforeFields(writer, encoderContext, map);

        for (final Map.Entry<String, Object> entry : map.entrySet()) {
            if (skipField(encoderContext, entry.getKey())) {
                continue;
            }
            writer.writeName(entry.getKey());
            Object obj = entry.getValue() ;
//            System.out.println(obj.getClass().toString()) ;
            try {
                if(obj instanceof String){
                    String s = (String)obj ;
                    String st = ValueConverter.convertString(s) ;
                    writeValue(writer, encoderContext, st);



                }else if(obj instanceof Integer){
                    int s = (Integer) obj ;
                    int st = ValueConverter.convertInteger(s) ;
                    writeValue(writer, encoderContext, st);


                }else if(obj instanceof Double){
                    double s = (Double) obj ;
                    double st = ValueConverter.convertDouble(s) ;
                    writeValue(writer, encoderContext, st);



                }else if(obj instanceof Short){
                    short s = (Short) obj ;
                    short st = ValueConverter.convertShort(s) ;
                    writeValue(writer, encoderContext, st);


                }else if(obj instanceof Long){
                    Long s = (Long) obj ;
                    long st = ValueConverter.convertLong(s) ;
                    writeValue(writer, encoderContext, st);


                }else if(obj instanceof Byte){
                    Byte s = (Byte) obj ;
                    Byte st = ValueConverter.convertByte(s) ;
                    writeValue(writer, encoderContext, st);


                }else if(obj instanceof Boolean){
                    Boolean s = (Boolean) obj ;
                    Boolean st = ValueConverter.convertBoolean(s) ;
                    writeValue(writer, encoderContext, st);


                }else {

                    writeValue(writer, encoderContext, obj);
                }




            }catch(Exception e){
                e.printStackTrace();
            }

        }
        writer.writeEndDocument();
    }

    private void writeIterable(final BsonWriter writer, final Iterable<Object> list, final EncoderContext encoderContext) {
        writer.writeStartArray();
        for (final Object value : list) {
            writeValue(writer, encoderContext, value);
        }
        writer.writeEndArray();
    }

    private Object readValue(final BsonReader reader, final DecoderContext decoderContext) {
        BsonType bsonType = reader.getCurrentBsonType();
        if (bsonType == BsonType.NULL) {
            reader.readNull();
            return null;
        } else if (bsonType == BsonType.ARRAY) {
            return readList(reader, decoderContext);
        } else if (bsonType == BsonType.BINARY && BsonBinarySubType.isUuid(reader.peekBinarySubType()) && reader.peekBinarySize() == 16) {
            return registry.get(UUID.class).decode(reader, decoderContext);
        }
        return valueTransformer.transform(bsonTypeCodecMap.get(bsonType).decode(reader, decoderContext));
    }

    private List<Object> readList(final BsonReader reader, final DecoderContext decoderContext) {
        reader.readStartArray();
        List<Object> list = new ArrayList<Object>();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            list.add(readValue(reader, decoderContext));
        }
        reader.readEndArray();
        return list;
    }
}
