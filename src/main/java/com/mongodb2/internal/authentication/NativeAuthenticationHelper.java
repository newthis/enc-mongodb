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

package com.mongodb2.internal.authentication;

import org.bson2.BsonDocument;
import org.bson2.BsonInt32;
import org.bson2.BsonString;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;

import static com.mongodb2.internal.HexUtils.hexMD5;

/**
 * Utility class for working with MongoDB native authentication.
 *
 * <p>This class should not be considered a part of the public API.</p>
 */
public final class NativeAuthenticationHelper {

    private static final Charset UTF_8_CHARSET = Charset.forName("UTF-8");

    /**
     * Creates a hash of the given user name and password, which is the hex encoding of
     * {@code MD5( <userName> + ":mongo:" + <password> )}.
     *
     * @param userName the user name
     * @param password the password
     * @return the hash as a string
     * @mongodb.driver.manual ../meta-driver/latest/legacy/implement-authentication-in-driver/ Authentication
     */
    public static String createAuthenticationHash(final String userName, final char[] password) {
        ByteArrayOutputStream bout = new ByteArrayOutputStream(userName.length() + 20 + password.length);
        try {
            bout.write(userName.getBytes(UTF_8_CHARSET));
            bout.write(":mongo:".getBytes(UTF_8_CHARSET));
            bout.write(new String(password).getBytes(UTF_8_CHARSET));
        } catch (IOException ioe) {
            throw new RuntimeException("impossible", ioe);
        }
        return hexMD5(bout.toByteArray());
    }

    public static BsonDocument getAuthCommand(final String userName, final char[] password, final String nonce) {
        return getAuthCommand(userName, createAuthenticationHash(userName, password), nonce);
    }

    public static BsonDocument getAuthCommand(final String userName, final String authHash, final String nonce) {
        String key = nonce + userName + authHash;

        BsonDocument cmd = new BsonDocument();

        cmd.put("authenticate", new BsonInt32(1));
        cmd.put("user", new BsonString(userName));
        cmd.put("nonce", new BsonString(nonce));
        cmd.put("key", new BsonString(hexMD5(key.getBytes(UTF_8_CHARSET))));

        return cmd;
    }

    public static BsonDocument getNonceCommand() {
        return new BsonDocument("getnonce", new BsonInt32(1));
    }

    private NativeAuthenticationHelper() {
    }
}
