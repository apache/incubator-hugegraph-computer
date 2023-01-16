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

package org.apache.hugegraph.computer.core.io;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

@SuppressWarnings("resource")
public class OptimizedUnsafeBytesTest {

    private static final int SIZE = Constants.SMALL_BUF_SIZE;

    @Test
    public void testConstructor() {
        OptimizedBytesOutput output = new OptimizedBytesOutput(SIZE);
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
        OptimizedBytesOutput output = new OptimizedBytesOutput(SIZE);
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
    public void testUnsignedShort() throws IOException {
        OptimizedBytesOutput output = new OptimizedBytesOutput(SIZE);
        for (short i = 0; i <= 255; i++) {
            output.writeShort(i);
        }
        output.writeShort(Short.MAX_VALUE);
        output.writeShort(Short.MIN_VALUE);
        OptimizedBytesInput input = new OptimizedBytesInput(
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
        OptimizedBytesOutput output = new OptimizedBytesOutput(SIZE);
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
        OptimizedBytesOutput output = new OptimizedBytesOutput(SIZE);
        output.writeChars(chars);
        OptimizedBytesInput input = new OptimizedBytesInput(
                                    output.toByteArray());
        for (int i = 0; i < chars.length(); i++) {
            char c = input.readChar();
            Assert.assertEquals(chars.charAt(i), c);
        }
    }

    @Test
    public void testWriteByInput() throws IOException {
        // Input class is OptimizedBytesInput
        String uuid = UUID.randomUUID().toString();
        OptimizedBytesInput input = inputByString(uuid);
        OptimizedBytesOutput output = new OptimizedBytesOutput(SIZE);
        output.write(input, 0, input.available());
        Assert.assertEquals(uuid, new String(output.toByteArray()));

        // Input class isn't OptimizedBytesInput
        File tempFile = File.createTempFile(UUID.randomUUID().toString(), "");
        BufferedFileOutput fileOutput = null;
        BufferedFileInput fileInput = null;
        try {
            fileOutput = new BufferedFileOutput(tempFile);
            fileOutput.writeBytes(uuid);
            fileOutput.close();
            fileInput = new BufferedFileInput(tempFile);
            output = new OptimizedBytesOutput(SIZE);
            output.write(fileInput, 0, fileInput.available());
            Assert.assertEquals(uuid, new String(output.toByteArray()));
        } finally {
            if (fileInput != null) {
                fileInput.close();
            }
            if (fileOutput != null) {
                fileOutput.close();
            }
            FileUtils.deleteQuietly(tempFile);
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
        OptimizedBytesOutput output = new OptimizedBytesOutput(SIZE);
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
        OptimizedBytesOutput output = new OptimizedBytesOutput(SIZE);
        output.writeBytes(s);
        return new OptimizedBytesInput(output.toByteArray());
    }
}
