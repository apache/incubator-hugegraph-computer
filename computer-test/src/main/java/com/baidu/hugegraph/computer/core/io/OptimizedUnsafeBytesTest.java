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

import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.testutil.Assert;

@SuppressWarnings("resource")
public class OptimizedUnsafeBytesTest {

    @Test
    public void testConstructor() {
        OptimizedBytesOutput output = new OptimizedBytesOutput(
                                      Constants.SMALL_BUF_SIZE);
        Assert.assertEquals(0, output.position());

        OptimizedBytesOutput output2 = new OptimizedBytesOutput(16);
        Assert.assertEquals(0, output2.position());

        OptimizedBytesInput input = new OptimizedBytesInput(output.buffer());
        Assert.assertEquals(0, input.position());

        OptimizedBytesInput input2 = new OptimizedBytesInput(output.buffer(),
                                                             4);
        Assert.assertEquals(0, input2.position());
    }

    @Test
    public void testUnsignedByte() throws IOException {
        OptimizedBytesOutput output = new OptimizedBytesOutput(
                                      Constants.BIG_BUF_SIZE);
        for (int i = 0; i <= 255; i++) {
            output.write(i);
        }
        OptimizedBytesInput input = new OptimizedBytesInput(
                                    output.toByteArray());
        for (int i = 0; i <= 255; i++) {
            int value = input.readUnsignedByte();
            Assert.assertEquals(i, value);
        }
    }

    @Test
    public void testChar() throws IOException {
        OptimizedBytesOutput output = new OptimizedBytesOutput(
                                      Constants.BIG_BUF_SIZE);
        for (char i = 'a'; i <= 'z'; i++) {
            output.writeChar(i);
        }
        OptimizedBytesInput input = new OptimizedBytesInput(
                                    output.toByteArray());
        for (char i = 'a'; i <= 'z'; i++) {
            char value = input.readChar();
            Assert.assertEquals(i, value);
        }
    }

    @Test
    public void testWriteChars() throws IOException {
        String chars = "testByteArray";
        OptimizedBytesOutput output = new OptimizedBytesOutput(
                                      Constants.BIG_BUF_SIZE);
        output.writeChars(chars);
        OptimizedBytesInput input = new OptimizedBytesInput(
                                    output.toByteArray());
        for (int i = 0; i < chars.length(); i++) {
            char c = input.readChar();
            Assert.assertEquals(chars.charAt(i), c);
        }
    }

    @Test
    public void testReadLine() {
        OptimizedBytesInput input = new OptimizedBytesInput(
                                    Constants.EMPTY_BYTES);
        Assert.assertThrows(ComputerException.class, () -> {
            input.readLine();
        });
    }

    @Test
    public void testDuplicate() throws IOException {
        OptimizedBytesInput raw = inputByString("apple");
        OptimizedBytesInput dup = raw.duplicate();
        raw.readByte();
        Assert.assertEquals(1, raw.position());
        Assert.assertEquals(0, dup.position());
    }

    @Test
    public void testCompare() throws IOException {
        OptimizedBytesInput apple = inputByString("apple");
        OptimizedBytesInput egg = inputByString("egg");
        Assert.assertTrue(apple.compare(0, 2, egg, 0, 2) < 0);
        Assert.assertTrue(apple.compare(1, 3, egg, 0, 2) > 0);
        Assert.assertEquals(0, apple.compare(4, 1, egg, 0, 1));
    }

    @Test
    public void testSkipBytes() throws IOException {
        OptimizedBytesOutput output = new OptimizedBytesOutput(
                                      Constants.SMALL_BUF_SIZE);
        long position = output.skip(4);
        Assert.assertEquals(0, position);
        output.writeFixedInt(Integer.MAX_VALUE);
        OptimizedBytesInput input = new OptimizedBytesInput(
                                    output.toByteArray());
        int bytesSkipped = input.skipBytes(4);
        Assert.assertEquals(4, bytesSkipped);
        Assert.assertEquals(Integer.MAX_VALUE, input.readFixedInt());
        Assert.assertEquals(0, input.skipBytes(1));
    }

    private static OptimizedBytesInput inputByString(String s)
                                                     throws IOException {
        OptimizedBytesOutput output = new OptimizedBytesOutput(
                                      Constants.SMALL_BUF_SIZE);
        output.writeBytes(s);
        return new OptimizedBytesInput(output.toByteArray());
    }
}
