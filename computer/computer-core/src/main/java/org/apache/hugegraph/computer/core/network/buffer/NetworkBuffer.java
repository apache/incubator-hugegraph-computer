/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.computer.core.network.buffer;

import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;

/**
 * This interface provides an immutable view for data in the form of bytes
 * with reference counting.
 *
 * The implementation should specify how the data is provided:
 *
 * - {@link NioBuffer}: data backed by a NIO ByteBuffer
 * - {@link NettyBuffer}: data backed by a Netty ByteBuf
 * - {@link FileRegionBuffer}: data backed by a File Region
 */
public interface NetworkBuffer {

    /**
     * Number of bytes of the data.
     */
    int length();

    /**
     * Increase the reference count by one if applicable.
     */
    NetworkBuffer retain();

    /**
     * If applicable, decrease the reference count by one and deallocates
     * the buffer if the reference count reaches zero.
     */
    NetworkBuffer release();

    /**
     * Returns the reference count.
     */
    int referenceCount();

    /**
     * Exposes this buffer's data as an NIO ByteBuffer.
     * Changing the position and limit of the returned ByteBuffer should not
     * affect the content of this buffer.
     */
    ByteBuffer nioByteBuffer();

    /**
     * Convert the buffer into an ByteBuf object, used to write the data out.
     */
    ByteBuf nettyByteBuf();

    /**
     * Copy to a new byte[]
     */
    byte[] copyToByteArray();
}
