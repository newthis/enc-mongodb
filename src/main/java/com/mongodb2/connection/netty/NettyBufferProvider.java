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

package com.mongodb2.connection.netty;

import com.mongodb2.connection.BufferProvider;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import org.bson2.ByteBuf;

final class NettyBufferProvider implements BufferProvider {

    private final ByteBufAllocator allocator;

    public NettyBufferProvider() {
        allocator = PooledByteBufAllocator.DEFAULT;
    }

    public NettyBufferProvider(final ByteBufAllocator allocator) {
        this.allocator = allocator;
    }

    @Override
    public ByteBuf getBuffer(final int size) {
        return new NettyByteBuf(allocator.directBuffer(size, size));
    }
}
