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

import com.mongodb2.MongoInternalException;
import com.mongodb2.ServerAddress;
import com.mongodb2.bulk.BulkWriteError;
import com.mongodb2.MongoBulkWriteException;
import com.mongodb2.bulk.BulkWriteResult;
import com.mongodb2.bulk.BulkWriteUpsert;
import com.mongodb2.bulk.WriteConcernError;
import com.mongodb2.bulk.WriteRequest;
import org.bson2.BsonArray;
import org.bson2.BsonDocument;
import org.bson2.BsonInt32;
import org.bson2.BsonNumber;
import org.bson2.BsonValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.mongodb2.bulk.WriteRequest.Type.REPLACE;
import static com.mongodb2.bulk.WriteRequest.Type.UPDATE;

final class WriteCommandResultHelper {

    static boolean hasError(final BsonDocument result) {
        return result.get("writeErrors") != null || result.get("writeConcernError") != null;
    }

    static BulkWriteResult getBulkWriteResult(final WriteRequest.Type type, final BsonDocument result) {
        int count = getCount(result);
        List<BulkWriteUpsert> upsertedItems = getUpsertedItems(result);
        return BulkWriteResult.acknowledged(type, count - upsertedItems.size(), getModifiedCount(type, result), upsertedItems);
    }

    static MongoBulkWriteException getBulkWriteException(final WriteRequest.Type type, final BsonDocument result,
                                                    final ServerAddress serverAddress) {
        if (!hasError(result)) {
            throw new MongoInternalException("This method should not have been called");
        }
        return new MongoBulkWriteException(getBulkWriteResult(type, result), getWriteErrors(result),
                                           getWriteConcernError(result), serverAddress);
    }

    @SuppressWarnings("unchecked")
    private static List<BulkWriteError> getWriteErrors(final BsonDocument result) {
        List<BulkWriteError> writeErrors = new ArrayList<BulkWriteError>();
        BsonArray writeErrorsDocuments = (BsonArray) result.get("writeErrors");
        if (writeErrorsDocuments != null) {
            for (BsonValue cur : writeErrorsDocuments) {
                BsonDocument curDocument = (BsonDocument) cur;
                writeErrors.add(new BulkWriteError(curDocument.getNumber("code").intValue(),
                                                   curDocument.getString("errmsg").getValue(),
                                                   curDocument.getDocument("errInfo", new BsonDocument()),
                                                   curDocument.getNumber("index").intValue()));
            }
        }
        return writeErrors;
    }

    private static WriteConcernError getWriteConcernError(final BsonDocument result) {
        BsonDocument writeConcernErrorDocument = (BsonDocument) result.get("writeConcernError");
        if (writeConcernErrorDocument == null) {
            return null;
        } else {
            return new WriteConcernError(writeConcernErrorDocument.getNumber("code").intValue(),
                                         writeConcernErrorDocument.getString("errmsg").getValue(),
                                         writeConcernErrorDocument.getDocument("errInfo", new BsonDocument()));
        }
    }

    @SuppressWarnings("unchecked")
    private static List<BulkWriteUpsert> getUpsertedItems(final BsonDocument result) {
        BsonValue upsertedValue = result.get("upserted");
        if (upsertedValue == null) {
            return Collections.emptyList();
        } else {
            List<BulkWriteUpsert> bulkWriteUpsertList = new ArrayList<BulkWriteUpsert>();
            for (BsonValue upsertedItem : (BsonArray) upsertedValue) {
                BsonDocument upsertedItemDocument = (BsonDocument) upsertedItem;
                bulkWriteUpsertList.add(new BulkWriteUpsert(upsertedItemDocument.getNumber("index").intValue(),
                                                            upsertedItemDocument.get("_id")));
            }
            return bulkWriteUpsertList;
        }
    }

    private static int getCount(final BsonDocument result) {
        return result.getNumber("n").intValue();
    }

    private static Integer getModifiedCount(final WriteRequest.Type type, final BsonDocument result) {
        BsonNumber modifiedCount = result.getNumber("nModified", (type == UPDATE || type == REPLACE) ? null : new BsonInt32(0));
        return modifiedCount == null ? null : modifiedCount.intValue();

    }

    private WriteCommandResultHelper() {
    }
}
