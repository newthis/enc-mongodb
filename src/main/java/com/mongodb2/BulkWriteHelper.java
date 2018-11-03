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

package com.mongodb2;

import org.bson2.BsonDocument;
import org.bson2.BsonDocumentReader;
import org.bson2.codecs.Decoder;
import org.bson2.codecs.DecoderContext;

import java.util.ArrayList;
import java.util.List;

final class BulkWriteHelper {

    static BulkWriteResult translateBulkWriteResult(final com.mongodb2.bulk.BulkWriteResult bulkWriteResult,
                                                    final Decoder<DBObject> decoder) {
        if (bulkWriteResult.wasAcknowledged()) {
            Integer modifiedCount = (bulkWriteResult.isModifiedCountAvailable()) ? bulkWriteResult.getModifiedCount() : null;
            return new AcknowledgedBulkWriteResult(bulkWriteResult.getInsertedCount(), bulkWriteResult.getMatchedCount(),
                                                   bulkWriteResult.getDeletedCount(), modifiedCount,
                                                   translateBulkWriteUpserts(bulkWriteResult.getUpserts(), decoder));
        } else {
            return new UnacknowledgedBulkWriteResult();
        }
    }

    static List<BulkWriteUpsert> translateBulkWriteUpserts(final List<com.mongodb2.bulk.BulkWriteUpsert> upserts,
                                                           final Decoder<DBObject> decoder) {
        List<BulkWriteUpsert> retVal = new ArrayList<BulkWriteUpsert>(upserts.size());
        for (com.mongodb2.bulk.BulkWriteUpsert cur : upserts) {
            retVal.add(new com.mongodb2.BulkWriteUpsert(cur.getIndex(), getUpsertedId(cur, decoder)));
        }
        return retVal;
    }

    private static Object getUpsertedId(final com.mongodb2.bulk.BulkWriteUpsert cur, final Decoder<DBObject> decoder) {
        return decoder.decode(new BsonDocumentReader(new BsonDocument("_id", cur.getId())), DecoderContext.builder().build()).get("_id");
    }

    static BulkWriteException translateBulkWriteException(final MongoBulkWriteException e, final Decoder<DBObject> decoder) {
        return new BulkWriteException(translateBulkWriteResult(e.getWriteResult(), decoder), translateWriteErrors(e.getWriteErrors()),
                                           translateWriteConcernError(e.getWriteConcernError()), e.getServerAddress());
    }

    static WriteConcernError translateWriteConcernError(final com.mongodb2.bulk.WriteConcernError writeConcernError) {
        return writeConcernError == null ? null : new WriteConcernError(writeConcernError.getCode(), writeConcernError.getMessage(),
                                                                        DBObjects.toDBObject(writeConcernError.getDetails()));
    }

    static List<BulkWriteError> translateWriteErrors(final List<com.mongodb2.bulk.BulkWriteError> errors) {
        List<BulkWriteError> retVal = new ArrayList<BulkWriteError>(errors.size());
        for (com.mongodb2.bulk.BulkWriteError cur : errors) {
            retVal.add(new BulkWriteError(cur.getCode(), cur.getMessage(), DBObjects.toDBObject(cur.getDetails()), cur.getIndex()));
        }
        return retVal;
    }

    static List<com.mongodb2.bulk.WriteRequest> translateWriteRequestsToNew(final List<WriteRequest> writeRequests) {
        List<com.mongodb2.bulk.WriteRequest> retVal = new ArrayList<com.mongodb2.bulk.WriteRequest>(writeRequests.size());
        for (WriteRequest cur : writeRequests) {
            retVal.add(cur.toNew());
        }
        return retVal;
    }

    private BulkWriteHelper() {
    }
}
