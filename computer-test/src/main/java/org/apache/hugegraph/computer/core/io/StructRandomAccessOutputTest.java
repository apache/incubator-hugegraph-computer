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
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class StructRandomAccessOutputTest {

    @Test
    public void testPosition() throws IOException {
        try (BytesOutput output = IOFactory.createBytesOutput(
                Constants.SMALL_BUF_SIZE);
             RandomAccessOutput srao = new StructRandomAccessOutput(output)) {
            Assert.assertEquals(0L, srao.position());
            srao.writeLong(12345678);
            Assert.assertEquals(8L, srao.position());
        }
    }

    @Test
    public void testSeek() throws IOException {
        try (BytesOutput output = IOFactory.createBytesOutput(
                                  Constants.SMALL_BUF_SIZE);
             RandomAccessOutput srao = new StructRandomAccessOutput(output)) {
            for (int i = -128; i <= 127; i++) {
                srao.writeInt(i);
            }
            Assert.assertEquals(678L, srao.position());
            // Overwrite last 8 bytes
            srao.seek(srao.position() - 8);
            srao.writeInt(1000);
            srao.writeInt(2000);
            Assert.assertEquals(678L, srao.position());
        }
    }

    @Test
    public void testSkip() throws IOException {
        try (BytesOutput output = new UnsafeBytesOutput(
                                  Constants.SMALL_BUF_SIZE);
             RandomAccessOutput srao = new StructRandomAccessOutput(output)) {
            Assert.assertEquals(0L, srao.position());
            output.writeInt(1000);
            output.writeInt(2000);
            Assert.assertEquals(8L, srao.position());
            srao.skip(4);
            Assert.assertEquals(12L, srao.position());
        }
    }

    @Test
    public void testWriteFixedInt() throws IOException {
        try (BytesOutput output = IOFactory.createBytesOutput(
                                  Constants.SMALL_BUF_SIZE);
             RandomAccessOutput srao = new StructRandomAccessOutput(output)) {
            Assert.assertEquals(0L, srao.position());
            srao.writeFixedInt(1000);
            Assert.assertEquals(4L, srao.position());
            srao.writeFixedInt(2000);
            Assert.assertEquals(8L, srao.position());
        }
    }

    @Test
    public void testWriteFixedIntWithPosition() throws IOException {
        try (BytesOutput output = IOFactory.createBytesOutput(
                                  Constants.SMALL_BUF_SIZE);
             RandomAccessOutput srao = new StructRandomAccessOutput(output)) {
            Assert.assertEquals(0L, srao.position());
            srao.writeFixedInt(4, 1000);
            Assert.assertEquals(0L, srao.position());
            srao.seek(8);
            srao.writeFixedInt(4, 2000);
            Assert.assertEquals(8L, srao.position());
        }
    }

    @Test
    public void testWriteByInput() throws IOException {
        // Input class is UnsafeBytesInput
        String uuid = UUID.randomUUID().toString();
        UnsafeBytesInput input = inputByString(uuid);
        BytesOutput output = IOFactory.createBytesOutput(
                             Constants.SMALL_BUF_SIZE);
        RandomAccessOutput srao = new StructRandomAccessOutput(output);
        srao.write(input, 0, input.available());
        Assert.assertEquals(50L, srao.position());
        srao.close();

        // Input class isn't UnsafeBytesInput
        File tempFile = File.createTempFile(UUID.randomUUID().toString(), "");
        BufferedFileOutput fileOutput = null;
        BufferedFileInput fileInput = null;
        try {
            fileOutput = new BufferedFileOutput(tempFile);
            fileOutput.writeBytes(uuid);
            fileOutput.close();
            fileInput = new BufferedFileInput(tempFile);
            srao.write(fileInput, 0, fileInput.available());
            Assert.assertEquals(100L, srao.position());
            srao.close();
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
    public void testByte() throws IOException {
        try (BytesOutput output = IOFactory.createBytesOutput(
                                  Constants.SMALL_BUF_SIZE);
             RandomAccessOutput srao = new StructRandomAccessOutput(output)) {
            for (int i = -128; i <= 127; i++) {
                srao.write(i);
            }
            Assert.assertEquals(678L, srao.position());
        }

        try (BytesOutput output = IOFactory.createBytesOutput(
                                  Constants.SMALL_BUF_SIZE);
             RandomAccessOutput srao = new StructRandomAccessOutput(output)) {
            for (int i = -128; i <= 127; i++) {
                srao.writeByte(i);
            }
            Assert.assertEquals(678L, srao.position());
        }
    }

    @Test
    public void testByteArray() throws IOException {
        byte[] bytes = "testByteArray".getBytes();
        try (BytesOutput output = IOFactory.createBytesOutput(
                                  Constants.SMALL_BUF_SIZE);
             RandomAccessOutput srao = new StructRandomAccessOutput(output)) {
            srao.write(bytes);
            Assert.assertEquals(22L, srao.position());
        }
    }

    @Test
    public void testWritePartByteArray() throws IOException {
        byte[] bytes = "testByteArray".getBytes();
        try (BytesOutput output = IOFactory.createBytesOutput(
                                  Constants.SMALL_BUF_SIZE);
             RandomAccessOutput srao = new StructRandomAccessOutput(output)) {
            srao.write(bytes, 1, bytes.length - 1);
            Assert.assertEquals(18L, srao.position());
        }
    }

    @Test
    public void testShort() throws IOException {
        try (BytesOutput output = IOFactory.createBytesOutput(
                                  Constants.SMALL_BUF_SIZE);
             RandomAccessOutput srao = new StructRandomAccessOutput(output)) {
            for (short i = -128; i <= 127; i++) {
                srao.writeShort(i);
            }
            Assert.assertEquals(678L, srao.position());
            srao.writeShort(1000);
            Assert.assertEquals(682L, srao.position());
        }
    }

    @Test
    public void testChar() throws IOException {
        try (BytesOutput output = IOFactory.createBytesOutput(
                                  Constants.SMALL_BUF_SIZE);
             RandomAccessOutput srao = new StructRandomAccessOutput(output)) {
            for (char i = 'a'; i <= 'z'; i++) {
                srao.writeChar(i);
            }
            Assert.assertEquals(75L, srao.position());
        }
    }

    @Test
    public void testBytes() throws IOException {
        String bytes = "testByteArray";
        try (BytesOutput output = IOFactory.createBytesOutput(
                                  Constants.SMALL_BUF_SIZE);
             RandomAccessOutput srao = new StructRandomAccessOutput(output)) {
            srao.writeBytes(bytes);
            Assert.assertEquals(15L, srao.position());
        }
    }

    @Test
    public void testChars() throws IOException {
        String chars = "testByteArray";
        try (BytesOutput output = IOFactory.createBytesOutput(
                                  Constants.SMALL_BUF_SIZE);
             RandomAccessOutput srao = new StructRandomAccessOutput(output)) {
            srao.writeChars(chars);
            Assert.assertEquals(15L, srao.position());
        }
    }

    @Test
    public void testUTF() throws IOException {
        String prefix = "random string";
        try (BytesOutput output = IOFactory.createBytesOutput(
                                  Constants.SMALL_BUF_SIZE);
             RandomAccessOutput srao = new StructRandomAccessOutput(output)) {
            for (int i = 0; i <= 9; i++) {
                srao.writeUTF(prefix + i);
            }
            Assert.assertEquals(160L, srao.position());
        }
    }

    private static UnsafeBytesInput inputByString(String s) throws IOException {
        BytesOutput output = IOFactory.createBytesOutput(
                             Constants.SMALL_BUF_SIZE);
        output.writeBytes(s);
        return new UnsafeBytesInput(output.toByteArray());
    }
}
