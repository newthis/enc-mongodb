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

package com.mongodb2.connection;

import com.mongodb2.ServerAddress;
import com.mongodb2.async.SingleResultCallback;
import com.mongodb2.diagnostics.logging.Logger;
import com.mongodb2.diagnostics.logging.Loggers;
import org.bson2.BsonDocument;
import org.bson2.BsonDocumentReader;
import org.bson2.codecs.BsonDocumentCodec;
import org.bson2.codecs.Decoder;
import org.bson2.codecs.DecoderContext;

class CommandResultCallback<T> extends CommandResultBaseCallback<BsonDocument> {
    public static final Logger LOGGER = Loggers.getLogger("protocol.command");

    private final SingleResultCallback<T> callback;
    private final Decoder<T> decoder;

    CommandResultCallback(final SingleResultCallback<T> callback, final Decoder<T> decoder,
                          final long requestId, final ServerAddress serverAddress) {
        super(new BsonDocumentCodec(), requestId, serverAddress);
        this.callback = callback;
        this.decoder = decoder;
    }

    @Override
    protected void callCallback(final BsonDocument response, final Throwable t) {
        if (t != null) {
            callback.onResult(null, t);
        } else {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Command execution completed with status " + ProtocolHelper.isCommandOk(response));
            }
            if (!ProtocolHelper.isCommandOk(response)) {
                callback.onResult(null, ProtocolHelper.getCommandFailureException(response, getServerAddress()));
            } else {
                try {
                    callback.onResult(decoder.decode(new BsonDocumentReader(response), DecoderContext.builder().build()), null);
                } catch (Throwable t1) {
                    callback.onResult(null, t1);
                }
            }
        }
    }
}
