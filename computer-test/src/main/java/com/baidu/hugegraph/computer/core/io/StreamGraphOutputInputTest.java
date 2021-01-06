/*
 * Copyright 2017 HugeGraph Authors
 *
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

package com.baidu.hugegraph.computer.core.io;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Test;

import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.id.IdType;
import com.baidu.hugegraph.computer.core.graph.id.LongId;
import com.baidu.hugegraph.testutil.Assert;

public class StreamGraphOutputInputTest {

    @Test
    public void testWriteReadId() throws IOException {
        LongId longId1 = new LongId(100L);
        byte[] bytes;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             StreamGraphOutput output = new StreamGraphOutput(baos)) {
            output.writeId(longId1);
            bytes = baos.toByteArray();
        }

        byte[] expect = new byte[]{IdType.LONG.code(),
                                   0, 0, 0, 0, 0, 0, 0, 100};
        Assert.assertArrayEquals(expect, bytes);

        Id longId2 = new LongId();
        Assert.assertEquals(0L, longId2.asLong());
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
             StreamGraphInput input = new StreamGraphInput(bais)) {
            longId2 = input.readId();
        }
        Assert.assertEquals(100L, longId2.asLong());
    }

    @Test
    public void testWriteReadVInt() throws IOException {
        testBytesStreamWriteReadVInt(new byte[]{0}, 0);
        testBytesStreamWriteReadVInt(new byte[]{1}, 1);
        testBytesStreamWriteReadVInt(new byte[]{(byte) 0x7f}, 127);
        testBytesStreamWriteReadVInt(new byte[]{(byte) 0x81, 0}, 128);
        testBytesStreamWriteReadVInt(new byte[]{(byte) 0xff, (byte) 0x7f},
                                     16383);
        testBytesStreamWriteReadVInt(new byte[]{(byte) 0x81, (byte) 0x80, 0},
                                     16384);
        testBytesStreamWriteReadVInt(new byte[]{(byte) 0x81, (byte) 0x80, 1},
                                     16385);
        testBytesStreamWriteReadVInt(new byte[]{-113, -1, -1, -1, 127}, -1);
        testBytesStreamWriteReadVInt(new byte[]{-121, -1, -1, -1, 127},
                                     Integer.MAX_VALUE);
        testBytesStreamWriteReadVInt(new byte[]{-120, -128, -128, -128, 0},
                                     Integer.MIN_VALUE);
    }

    @Test
    public void testWriteReadVLong() throws IOException {
        testBytesStreamWriteReadVLong(new byte[]{0}, 0);
        testBytesStreamWriteReadVLong(new byte[]{1}, 1);
        testBytesStreamWriteReadVLong(new byte[]{(byte) 0x7f}, 127);
        testBytesStreamWriteReadVLong(new byte[]{(byte) 0x81, 0}, 128);
        testBytesStreamWriteReadVLong(new byte[]{(byte) 0xff, (byte) 0x7f},
                                      16383);
        testBytesStreamWriteReadVLong(new byte[]{(byte) 0x81, (byte) 0x80, 0},
                                      16384);
        testBytesStreamWriteReadVLong(new byte[]{(byte) 0x81, (byte) 0x80, 1},
                                      16385);
        testBytesStreamWriteReadVLong(new byte[]{-127, -1, -1, -1, -1,
                                                 -1, -1, -1, -1, 127}, -1);
        testBytesStreamWriteReadVLong(new byte[]{-121, -1, -1, -1, 127},
                                      Integer.MAX_VALUE);
        testBytesStreamWriteReadVLong(new byte[]{-127, -1, -1, -1, -1,
                                                 -8, -128, -128, -128, 0},
                                      Integer.MIN_VALUE);
        testBytesStreamWriteReadVLong(new byte[]{-1, -1, -1, -1, -1,
                                                 -1, -1, -1, 127},
                                      Long.MAX_VALUE);
        testBytesStreamWriteReadVLong(new byte[]{-127, -128, -128, -128, -128,
                                                 -128, -128, -128, -128, 0},
                                      Long.MIN_VALUE);
    }

    public static void testBytesStreamWriteReadVInt(byte[] bytes, int value)
                                                    throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(5);
             StreamGraphOutput output = new StreamGraphOutput(baos)) {
            output.writeVInt(value);
            Assert.assertArrayEquals(bytes, baos.toByteArray());
        }

        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
             StreamGraphInput input = new StreamGraphInput(bais)) {
            int readValue = input.readVInt();
            Assert.assertEquals(value, readValue);
        }
    }

    public static void testBytesStreamWriteReadVLong(byte[] bytes, long value)
                                                     throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(5);
             StreamGraphOutput output = new StreamGraphOutput(baos)) {
            output.writeVLong(value);
            Assert.assertArrayEquals(bytes, baos.toByteArray());
        }

        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
             StreamGraphInput input = new StreamGraphInput(bais)) {
            long readValue = input.readVLong();
            Assert.assertEquals(value, readValue);
        }
    }
}
