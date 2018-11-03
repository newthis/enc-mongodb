/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.mongodb2.connection;

// to enable unit testing of classes that rely on System.nanoTime
final class Time {
    static final long CONSTANT_TIME = 42;

    private static boolean isConstant;

    static void makeTimeConstant() {
        isConstant = true;
    }

    static void makeTimeMove() {
        isConstant = false;
    }

    static long nanoTime() {
        return isConstant ? CONSTANT_TIME : System.nanoTime();
    }

    private Time() {
    }
}
