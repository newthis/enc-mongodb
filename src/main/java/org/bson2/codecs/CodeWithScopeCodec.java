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

import org.bson2.BsonReader;
import org.bson2.BsonWriter;
import org.bson2.Document;
import org.bson2.types.CodeWithScope;

/**
 * Encodes and decodes {@code CodeWithScope} instances.
 *
 * @since 3.0
 */
public class CodeWithScopeCodec implements Codec<CodeWithScope> {
    private final Codec<Document> documentCodec;

    /**
     * Creates a new CodeWithScopeCodec.
     *
     * @param documentCodec a Codec for encoding and decoding the {@link org.bson2.types.CodeWithScope#getScope()}.
     */
    public CodeWithScopeCodec(final Codec<Document> documentCodec) {
        this.documentCodec = documentCodec;
    }

    @Override
    public CodeWithScope decode(final BsonReader bsonReader, final DecoderContext decoderContext) {
        String code = bsonReader.readJavaScriptWithScope();
        Document scope = documentCodec.decode(bsonReader, decoderContext);
        return new CodeWithScope(code, scope);
    }

    @Override
    public void encode(final BsonWriter writer, final CodeWithScope codeWithScope, final EncoderContext encoderContext) {
        writer.writeJavaScriptWithScope(codeWithScope.getCode());
        documentCodec.encode(writer, codeWithScope.getScope(), encoderContext);
    }

    @Override
    public Class<CodeWithScope> getEncoderClass() {
        return CodeWithScope.class;
    }
}
