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

import java.io.IOException;

import org.junit.Test;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.util.Log;

public class UnsafeByteArrayTest {

    private static final Logger LOG = Log.logger(UnsafeByteArrayTest.class);

    @Test
    public void testConstructor() {
        UnsafeByteArrayOutput output = new UnsafeByteArrayOutput();
        Assert.assertEquals(0, output.position());

        UnsafeByteArrayOutput output2 = new UnsafeByteArrayOutput(16);
        Assert.assertEquals(0, output2.position());

        UnsafeByteArrayInput input = new UnsafeByteArrayInput(
                                         output.buffer());
        Assert.assertEquals(0, input.position());
    }

    @Test
    public void testBoolean() {
        UnsafeByteArrayOutput output = new UnsafeByteArrayOutput();
        output.writeBoolean(true);
        output.writeBoolean(false);
        UnsafeByteArrayInput input = new UnsafeByteArrayInput(
                                         output.toByteArray());
        Assert.assertTrue(input.readBoolean());
        Assert.assertFalse(input.readBoolean());
    }

    @Test
    public void testByte() {
        UnsafeByteArrayOutput output = new UnsafeByteArrayOutput();
        for (int i = -128; i <= 127; i++) {
            output.write(i);
        }
        UnsafeByteArrayInput input = new UnsafeByteArrayInput(
                                         output.toByteArray());
        for (int i = -128; i <= 127; i++) {
            int value = input.readByte();
            Assert.assertEquals(i, value);
        }
    }

    @Test
    public void testUnsignedByte() {
        UnsafeByteArrayOutput output = new UnsafeByteArrayOutput();
        for (int i = 0; i <= 255; i++) {
            output.write(i);
        }
        UnsafeByteArrayInput input = new UnsafeByteArrayInput(
                                         output.toByteArray());
        for (int i = 0; i <= 255; i++) {
            int value = input.readUnsignedByte();
            Assert.assertEquals(i, value);
        }
    }

    @Test
    public void testShort() {
        UnsafeByteArrayOutput output = new UnsafeByteArrayOutput();
        for (short i = -128; i <= 127; i++) {
            output.writeShort(i);
        }
        output.writeShort(Short.MAX_VALUE);
        output.writeShort(Short.MIN_VALUE);
        UnsafeByteArrayInput input = new UnsafeByteArrayInput(
                                         output.toByteArray());
        for (int i = -128; i <= 127; i++) {
            Assert.assertEquals(i, input.readShort());
        }
        Assert.assertEquals(Short.MAX_VALUE, input.readShort());
        Assert.assertEquals(Short.MIN_VALUE, input.readShort());
    }

    @Test
    public void testUnsignedShort() {
        UnsafeByteArrayOutput output = new UnsafeByteArrayOutput();
        for (short i = 0; i <= 255; i++) {
            output.writeShort(i);
        }
        output.writeShort(Short.MAX_VALUE);
        output.writeShort(Short.MIN_VALUE);
        UnsafeByteArrayInput input = new UnsafeByteArrayInput(
                                         output.toByteArray());
        for (int i = 0; i <= 255; i++) {
            Assert.assertEquals(i, input.readUnsignedShort());
        }
        Assert.assertEquals(Short.MAX_VALUE, input.readUnsignedShort());
        Assert.assertEquals(Short.MIN_VALUE & 0xFFFF,
                            input.readUnsignedShort());
    }

    @Test
    public void testShortWithPosition() {
        UnsafeByteArrayOutput output = new UnsafeByteArrayOutput();
        int position = output.skipBytes(Constants.SHORT_LEN);
        output.writeShort(2);
        output.writeShort(position, 1);

        UnsafeByteArrayInput input = new UnsafeByteArrayInput(
                                         output.toByteArray());
        Assert.assertEquals(1, input.readShort());
        Assert.assertEquals(2, input.readShort());
    }

    @Test
    public void testChar() {
        UnsafeByteArrayOutput output = new UnsafeByteArrayOutput();
        for (char i = 'a'; i <= 'z'; i++) {
            output.writeChar(i);
        }
        UnsafeByteArrayInput input = new UnsafeByteArrayInput(
                                         output.toByteArray());
        for (char i = 'a'; i <= 'z'; i++) {
            char value = input.readChar();
            Assert.assertEquals(i, value);
        }
    }

    @Test
    public void testInt() {
        UnsafeByteArrayOutput output = new UnsafeByteArrayOutput();
        for (int i = -128; i <= 127; i++) {
            output.writeInt(i);
        }
        output.writeInt(Integer.MAX_VALUE);
        output.writeInt(Integer.MIN_VALUE);
        UnsafeByteArrayInput input = new UnsafeByteArrayInput(
                                         output.toByteArray());
        for (int i = -128; i <= 127; i++) {
            Assert.assertEquals(i, input.readInt());
        }
        Assert.assertEquals(Integer.MAX_VALUE, input.readInt());
        Assert.assertEquals(Integer.MIN_VALUE, input.readInt());
    }

    @Test
    public void testWriteIntWithPosition() {
        UnsafeByteArrayOutput output = new UnsafeByteArrayOutput();
        int position = output.skipBytes(Constants.INT_LEN);
        output.writeInt(2);
        output.writeInt(position, 1);

        UnsafeByteArrayInput input = new UnsafeByteArrayInput(
                                         output.toByteArray());
        Assert.assertEquals(1, input.readInt());
        Assert.assertEquals(2, input.readInt());
    }

    @Test
    public void testLong() {
        UnsafeByteArrayOutput output = new UnsafeByteArrayOutput();
        for (long i = -128; i <= 127; i++) {
            output.writeLong(i);
        }
        output.writeLong(Long.MAX_VALUE);
        output.writeLong(Long.MIN_VALUE);
        UnsafeByteArrayInput input = new UnsafeByteArrayInput(
                                         output.toByteArray());
        for (long i = -128; i <= 127; i++) {
            Assert.assertEquals(i, input.readLong());
        }
        Assert.assertEquals(Long.MAX_VALUE, input.readLong());
        Assert.assertEquals(Long.MIN_VALUE, input.readLong());
    }

    @Test
    public void testFloat() {
        UnsafeByteArrayOutput output = new UnsafeByteArrayOutput();
        for (int i = -128; i <= 127; i++) {
            output.writeFloat((float) i);
        }
        output.writeFloat(Float.MAX_VALUE);
        output.writeFloat(Float.MIN_VALUE);
        UnsafeByteArrayInput input = new UnsafeByteArrayInput(
                                         output.toByteArray());
        for (int i = -128; i <= 127; i++) {
            Assert.assertEquals((float) i, input.readFloat(), 0.0D);
        }
        Assert.assertEquals(Float.MAX_VALUE, input.readFloat(), 0.0D);
        Assert.assertEquals(Float.MIN_VALUE, input.readFloat(), 0.0D);
    }

    @Test
    public void testDouble() {
        UnsafeByteArrayOutput output = new UnsafeByteArrayOutput();
        for (int i = -128; i <= 127; i++) {
            output.writeDouble(i);
        }
        output.writeDouble(Double.MAX_VALUE);
        output.writeDouble(Double.MIN_VALUE);
        UnsafeByteArrayInput input = new UnsafeByteArrayInput(
                                         output.toByteArray());
        for (int i = -128; i <= 127; i++) {
            Assert.assertEquals(i, input.readDouble(), 0.0D);
        }
        Assert.assertEquals(Double.MAX_VALUE, input.readDouble(), 0.0D);
        Assert.assertEquals(Double.MIN_VALUE, input.readDouble(), 0.0D);
    }

    @Test
    public void testByteArray() {
        byte[] bytes = "testByteArray".getBytes();
        UnsafeByteArrayOutput output = new UnsafeByteArrayOutput();
        output.write(bytes);
        byte[] bytesRead = new byte[bytes.length];
        UnsafeByteArrayInput input = new UnsafeByteArrayInput(
                                         output.toByteArray());
        input.readFully(bytesRead);
        Assert.assertArrayEquals(bytes, bytesRead);
    }

    @Test
    public void testWritePartByteArray() {
        byte[] bytes = "testByteArray".getBytes();
        UnsafeByteArrayOutput output = new UnsafeByteArrayOutput();
        output.write(bytes, 1, bytes.length - 1);
        byte[] bytesRead = new byte[bytes.length];
        UnsafeByteArrayInput input = new UnsafeByteArrayInput(
                                         output.toByteArray());
        input.readFully(bytesRead, 1, bytes.length - 1);
        bytesRead[0] = bytes[0];
        Assert.assertArrayEquals(bytes, bytesRead);
    }

    @Test
    public void testWriteChars() {
        String chars = "testByteArray";
        UnsafeByteArrayOutput output = new UnsafeByteArrayOutput();
        output.writeChars(chars);
        UnsafeByteArrayInput input = new UnsafeByteArrayInput(
                                         output.toByteArray());
        for (int i = 0; i < chars.length(); i++) {
            char c = input.readChar();
            LOG.info(c + "");
            Assert.assertEquals(chars.charAt(i), c);
        }
    }

    @Test
    public void testReadLine() {
        UnsafeByteArrayInput input = new UnsafeByteArrayInput(
                                         Constants.EMPTY_BYTES);
        Assert.assertThrows(ComputerException.class, () -> {
            input.readLine();
        });
    }

    @Test
    public void testUTFWithChineseCharacters() throws IOException {
        String s = "带汉字的字符串";
        UnsafeByteArrayOutput output = new UnsafeByteArrayOutput();
        output.writeUTF(s);
        UnsafeByteArrayInput input = new UnsafeByteArrayInput(
                                         output.toByteArray());
        String value = input.readUTF();
        Assert.assertEquals(s, value);
    }

    @Test
    public void testUTF() throws IOException {
        String prefix = "random string";
        UnsafeByteArrayOutput output = new UnsafeByteArrayOutput();
        for (int i = -128; i <= 127; i++) {
            output.writeUTF(prefix + i);
        }
        UnsafeByteArrayInput input = new UnsafeByteArrayInput(output.buffer());
        for (int i = -128; i <= 127; i++) {
            String value = input.readUTF();
            Assert.assertEquals(prefix + i, value);
        }
    }

    @Test
    public void testSkipBytes() {
        UnsafeByteArrayOutput output = new UnsafeByteArrayOutput();
        output.skipBytes(4);
        output.writeInt(Integer.MAX_VALUE);
        UnsafeByteArrayInput input = new UnsafeByteArrayInput(
                                         output.toByteArray());
        input.skipBytes(4);
        Assert.assertEquals(4, input.remaining());
        Assert.assertEquals(Integer.MAX_VALUE, input.readInt());
        Assert.assertEquals(0, input.remaining());
        Assert.assertEquals(0, input.skipBytes(1));
    }

    @Test
    public void testBuffer() {
        UnsafeByteArrayOutput output = new UnsafeByteArrayOutput();
        output.writeInt(Integer.MAX_VALUE);
        UnsafeByteArrayInput input = new UnsafeByteArrayInput(
                                         output.buffer(), output.position());
        Assert.assertEquals(Integer.MAX_VALUE, input.readInt());
    }

    @Test
    public void testOverRead() {
        UnsafeByteArrayOutput output = new UnsafeByteArrayOutput();
        output.writeInt(Integer.MAX_VALUE);
        UnsafeByteArrayInput input = new UnsafeByteArrayInput(
                                         output.buffer(), output.position());
        Assert.assertEquals(Integer.MAX_VALUE, input.readInt());
        Assert.assertThrows(ComputerException.class, () -> {
            input.readInt();
        });
    }
}