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
import java.io.UTFDataFormatException;

import org.junit.Test;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.UnitTestBase;
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
    public void testBoolean() throws IOException {
        UnsafeByteArrayOutput output = new UnsafeByteArrayOutput();
        output.writeBoolean(true);
        output.writeBoolean(false);
        UnsafeByteArrayInput input = new UnsafeByteArrayInput(
                                         output.toByteArray());
        Assert.assertTrue(input.readBoolean());
        Assert.assertFalse(input.readBoolean());
    }

    @Test
    public void testByte() throws IOException {
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
    public void testUnsignedByte() throws IOException {
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
    public void testShort() throws IOException {
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
    public void testUnsignedShort() throws IOException {
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
    public void testChar() throws IOException {
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
    public void testInt() throws IOException {
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
    public void testWriteIntWithPosition() throws IOException {
        UnsafeByteArrayOutput output = new UnsafeByteArrayOutput();
        long position = output.skip(Constants.INT_LEN);
        output.writeInt(2);
        output.writeInt(position, 1);

        UnsafeByteArrayInput input = new UnsafeByteArrayInput(
                                         output.toByteArray());
        Assert.assertEquals(1, input.readInt());
        Assert.assertEquals(2, input.readInt());
    }

    @Test
    public void testOverWriteWithPosition() {
        Assert.assertThrows(ComputerException.class, () -> {
            UnsafeByteArrayOutput output = new UnsafeByteArrayOutput(4);
            output.writeInt(1, 100);
        });

        Assert.assertThrows(ComputerException.class, () -> {
            UnsafeByteArrayOutput output = new UnsafeByteArrayOutput(4);
            output.writeInt(2, 100);
        });

        Assert.assertThrows(ComputerException.class, () -> {
            UnsafeByteArrayOutput output = new UnsafeByteArrayOutput(4);
            output.writeInt(3, 100);
        });

        Assert.assertThrows(ComputerException.class, () -> {
            UnsafeByteArrayOutput output = new UnsafeByteArrayOutput(4);
            output.writeInt(4, 100);
        });
    }

    @Test
    public void testLong() throws IOException {
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
    public void testFloat() throws IOException {
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
    public void testDouble() throws IOException {
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
    public void testByteArray() throws IOException {
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
    public void testWritePartByteArray() throws IOException {
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
    public void testWriteChars() throws IOException {
        String chars = "testByteArray";
        UnsafeByteArrayOutput output = new UnsafeByteArrayOutput();
        output.writeChars(chars);
        UnsafeByteArrayInput input = new UnsafeByteArrayInput(
                                         output.toByteArray());
        for (int i = 0; i < chars.length(); i++) {
            char c = input.readChar();
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
    public void testUTFBoundary() throws IOException {
        String prefix = "random string";
        UnsafeByteArrayOutput output = new UnsafeByteArrayOutput();
        String s1 = UnitTestBase.randomString(65535);
        output.writeUTF(s1);
        String s2 = UnitTestBase.randomString(65536);
        Assert.assertThrows(UTFDataFormatException.class, () -> {
            output.writeUTF(s2);
        });
        UnsafeByteArrayInput input = new UnsafeByteArrayInput(output.buffer());
        String value = input.readUTF();
        Assert.assertEquals(s1, value);
    }

    @Test
    public void testSkipBytes() throws IOException {
        UnsafeByteArrayOutput output = new UnsafeByteArrayOutput();
        long position = output.skip(4);
        Assert.assertEquals(0, position);
        output.writeInt(Integer.MAX_VALUE);
        UnsafeByteArrayInput input = new UnsafeByteArrayInput(
                                         output.toByteArray());
        int bytesSkipped = input.skipBytes(4);
        Assert.assertEquals(4, bytesSkipped);
        Assert.assertEquals(4, input.remaining());
        Assert.assertEquals(Integer.MAX_VALUE, input.readInt());
        Assert.assertEquals(0, input.remaining());
        Assert.assertEquals(0, input.skipBytes(1));
    }

    @Test
    public void testSkip() throws IOException {
        UnsafeByteArrayOutput output = new UnsafeByteArrayOutput();
        output.skip(4);
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
    public void testBuffer() throws IOException {
        UnsafeByteArrayOutput output = new UnsafeByteArrayOutput();
        output.writeInt(Integer.MAX_VALUE);
        UnsafeByteArrayInput input = new UnsafeByteArrayInput(
                                         output.buffer(), output.position());
        Assert.assertEquals(Integer.MAX_VALUE, input.readInt());
    }

    @Test
    public void testOverRead() throws IOException {
        UnsafeByteArrayOutput output = new UnsafeByteArrayOutput();
        output.writeInt(Integer.MAX_VALUE);
        UnsafeByteArrayInput input = new UnsafeByteArrayInput(
                                         output.buffer(), output.position());
        Assert.assertEquals(Integer.MAX_VALUE, input.readInt());
        Assert.assertThrows(ComputerException.class, () -> {
            input.readInt();
        });
    }

    @Test
    public void testSeek() throws IOException {
        UnsafeByteArrayOutput output = new UnsafeByteArrayOutput();
        for (int i = -128; i <= 127; i++) {
            output.writeInt(i);
        }
        // Overwrite last 2 elements
        output.seek(256 * 4 - 8);
        output.writeInt(Integer.MAX_VALUE);
        output.writeInt(Integer.MIN_VALUE);
        UnsafeByteArrayInput input = new UnsafeByteArrayInput(
                                         output.toByteArray());
        for (int i = -128; i <= 125; i++) {
            Assert.assertEquals(i, input.readInt());
        }
        Assert.assertEquals(Integer.MAX_VALUE, input.readInt());
        Assert.assertEquals(Integer.MIN_VALUE, input.readInt());
        input.seek(256 * 4 - 8);
        Assert.assertEquals(Integer.MAX_VALUE, input.readInt());
        Assert.assertEquals(Integer.MIN_VALUE, input.readInt());
    }
}
