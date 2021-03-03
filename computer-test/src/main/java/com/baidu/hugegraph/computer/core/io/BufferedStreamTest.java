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

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.UnitTestBase;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.util.Log;

public class BufferedStreamTest {

    private static final Logger LOG = Log.logger(BufferedStreamTest.class);

    private static final int BUFFER_SIZE = 128;

    @Test
    public void testConstructor() throws IOException {
        File file  = this.createTempFile();
        try {
            OutputStream os = new FileOutputStream(file);
            try (BufferedOutputStream output = new BufferedOutputStream(os)) {
                Assert.assertEquals(0, output.position());
            }
            InputStream in = new FileInputStream(file);
            try (BufferedInputStream input = new BufferedInputStream(in)) {
                Assert.assertEquals(0, input.position());
            }
            Assert.assertThrows(IllegalArgumentException.class, () -> {
                new BufferedOutputStream(os, 1);
            });
            Assert.assertThrows(IllegalArgumentException.class, () -> {
                new BufferedInputStream(in, 1);
            });
        } finally {
            FileUtils.deleteQuietly(file);
        }
    }

    @Test
    public void testInt() throws IOException {
        File file = this.createTempFile();
        try {
            try (BufferedOutputStream output = this.createOutput(file)) {
                for (int i = -128; i <= 127; i++) {
                    output.writeInt(i);
                }
                output.writeInt(Integer.MAX_VALUE);
                output.writeInt(Integer.MIN_VALUE);
            }
            try (BufferedInputStream input = this.createInput(file)) {
                for (int i = -128; i <= 127; i++) {
                    Assert.assertEquals(i, input.readInt());
                }
                Assert.assertEquals(Integer.MAX_VALUE, input.readInt());
                Assert.assertEquals(Integer.MIN_VALUE, input.readInt());
            }
        } finally {
            FileUtils.deleteQuietly(file);
        }
    }

    @Test
    public void testWriteIntWithPosition() throws IOException {
        File file = this.createTempFile();
        try {
            try (BufferedOutputStream output = this.createOutput(file)) {
                // 256 bytes
                for (int i = 0; i < 64; i++) {
                    output.writeInt(i);
                }
                output.writeInt(200, 1);
                // Previous buffer
                Assert.assertThrows(IOException.class, () -> {
                    output.writeInt(100, 4);
                });
                output.writeInt(Integer.MAX_VALUE);
                output.writeInt(Integer.MIN_VALUE);
            }
            try (BufferedInputStream input = this.createInput(file)) {
                for (int i = 0; i < 64; i++) {
                    int position = i * 4;
                    if (position == 200) {
                        Assert.assertEquals(1, input.readInt());
                    } else {
                        Assert.assertEquals(i, input.readInt());
                    }
                }
                Assert.assertEquals(Integer.MAX_VALUE, input.readInt());
                Assert.assertEquals(Integer.MIN_VALUE, input.readInt());
            }
        } finally {
            FileUtils.deleteQuietly(file);
        }
    }

    @Test
    public void testByteArray() throws IOException {
        int loopTimes = 129;
        byte[] array = UnitTestBase.randomBytes(10);
        File file = this.createTempFile();
        try {
            try (BufferedOutputStream output = this.createOutput(file)) {
                for (int i = 0; i < loopTimes; i++) {
                    output.write(array);
                }
            }

            byte[] arrayRead = new byte[10];
            try (DataInputStream dis = new DataInputStream(
                    new FileInputStream(file))) {
                for (int i = 0; i < loopTimes; i++) {
                    dis.readFully(arrayRead);
                    Assert.assertArrayEquals(array, arrayRead);
                }
            }
            try (BufferedInputStream input = this.createInput(file)) {
                for (int i = 0; i < loopTimes - 1; i++) {
                    input.readFully(arrayRead);
                    Assert.assertArrayEquals(array, arrayRead);
                }
                Assert.assertThrows(IOException.class, () -> {
                    input.readFully(new byte[20]);
                });
            }
        } finally {
            FileUtils.deleteQuietly(file);
        }
    }

    @Test
    public void testLargeByteArray() throws IOException {
        int size = 10;
        int arraySize = 1280; // large than buffer size
        byte[] array = UnitTestBase.randomBytes(arraySize);
        File file = this.createTempFile();
        try {
            try (BufferedOutputStream output = this.createOutput(file)) {
                for (int i = 0; i < size; i++) {
                    output.write(array);
                }
            }

            byte[] arrayRead = new byte[arraySize];
            try (DataInputStream dis = new DataInputStream(
                    new FileInputStream(file))) {
                for (int i = 0; i < size; i++) {
                    dis.readFully(arrayRead);
                    Assert.assertArrayEquals(array, arrayRead);
                }
            }

            try (BufferedInputStream input = this.createInput(file)) {
                for (int i = 0; i < size - 1; i++) {
                    input.readFully(arrayRead);
                    Assert.assertArrayEquals(array, arrayRead);
                }
                Assert.assertThrows(IOException.class, () -> {
                    input.readFully(new byte[arraySize * 2]);
                });
            }
        } finally {
            FileUtils.deleteQuietly(file);
        }
    }

    @Test
    public void testSeek() throws IOException {
        int size = 1024;
        File file = this.createTempFile();
        try {
            try (BufferedOutputStream output = this.createOutput(file)) {
                for (int i = 0; i < size; i++) {
                    output.seek(i * 4);
                    output.writeInt(i);
                }
            }
            try (BufferedInputStream input = this.createInput(file)) {
                for (int i = 0; i < size; i++) {
                    input.seek(i * 4);
                    Assert.assertEquals(i, input.readInt());
                }
                Assert.assertThrows(IOException.class, () -> {
                    input.seek(size * 4 + 1);
                });
            }
        } finally {
            FileUtils.deleteQuietly(file);
        }
    }

    @Test
    public void testSeekOutRange() throws IOException {
        int size = 1024;
        File file = this.createTempFile();
        try {
            try (BufferedOutputStream output = this.createOutput(file)) {
                for (int i = 0; i < size; i++) {
                    output.seek(i * 4);
                    output.writeInt(i);

                }
                // Overwrite last two elements
                output.seek((size - 2) * 4);
                output.writeInt(Integer.MAX_VALUE);
                output.writeInt(Integer.MIN_VALUE);
                Assert.assertThrows(IOException.class, () -> {
                    output.seek(output.position() - BUFFER_SIZE - 2);
                });
            }
            try (BufferedInputStream input = this.createInput(file)) {
                for (int i = 0; i < size - 2; i++) {
                    input.seek(i * 4);
                    Assert.assertEquals(i, input.readInt());
                }
                Assert.assertEquals(Integer.MAX_VALUE, input.readInt());
                Assert.assertEquals(Integer.MIN_VALUE, input.readInt());
                Assert.assertThrows(IOException.class, () -> {
                    input.seek(input.position() - BUFFER_SIZE - 2);
                });
            }
        } finally {
            FileUtils.deleteQuietly(file);
        }
    }

    @Test
    public void testSeekMoreThanBuffer() throws IOException {
        long seekPosition = BUFFER_SIZE * 2 + 1;
        File file = this.createTempFile();
        try {
            try (BufferedOutputStream output = this.createOutput(file)) {
                output.writeInt(1);
                output.seek(seekPosition);
                output.writeInt(Integer.MAX_VALUE);
                output.writeInt(Integer.MIN_VALUE);
            }
            LOG.info("file.length:{}", file.length());
            try (BufferedInputStream input = this.createInput(file)) {
                Assert.assertEquals(1, input.readInt());
                input.seek(seekPosition);
                Assert.assertEquals(Integer.MAX_VALUE, input.readInt());
                Assert.assertEquals(Integer.MIN_VALUE, input.readInt());
            }
        } finally {
            FileUtils.deleteQuietly(file);
        }
    }

    @Test
    public void testSkip() throws IOException {
        File file = this.createTempFile();
        try {
            try (BufferedOutputStream output = this.createOutput(file)) {
                for (int i = -128; i <= 127; i++) {
                    output.writeByte(i);
                }
                output.skip(4L);
                output.writeByte(127);
                output.skip(1280L);
                for (int i = 1; i <= 1280; i++) {
                    output.writeByte(i);
                }
            }

            try (DataInputStream dis = new DataInputStream(
                                       new FileInputStream(file))) {
                Assert.assertEquals(-128, dis.readByte());
                dis.skip(1);
                for (int i = -126; i <= 127; i++) {
                    Assert.assertEquals(i, dis.readByte());
                }
                long count1 = dis.skip(4);
                Assert.assertEquals(4, count1);
                Assert.assertEquals(127, dis.readByte());
                long count2 = dis.skip(1280);
                Assert.assertEquals(1280, count2);
                for (int i = 1; i <= 1280; i++) {
                    Assert.assertEquals((byte) i, dis.readByte());
                }
            }

            try (BufferedInputStream input = this.createInput(file)) {
                Assert.assertEquals(-128, input.readByte());
                input.skip(1);
                for (int i = -126; i <= 127; i++) {
                    Assert.assertEquals(i, input.readByte());
                }
                input.skip(4);
                Assert.assertEquals(127, input.readByte());
                input.skip(1280);
                for (int i = 1; i <= 1280; i++) {
                    Assert.assertEquals((byte) i, input.readByte());
                }
            }
        } finally {
            FileUtils.deleteQuietly(file);
        }
    }

    @Test
    public void testPosition() throws IOException {
        int size = 1024;
        File file = this.createTempFile();
        try {
            try (BufferedOutputStream output = this.createOutput(file)) {
                for (int i = 0; i <= size; i++) {
                    Assert.assertEquals(i * 4, output.position());
                    output.writeInt(i);
                }
            }
            try (BufferedInputStream input = this.createInput(file)) {
                for (int i = 0; i <= size; i++) {
                    Assert.assertEquals(i * 4, input.position());
                    Assert.assertEquals(i, input.readInt());
                }
            }
        } finally {
            FileUtils.deleteQuietly(file);
        }
    }

    private File createTempFile() throws IOException {
        return File.createTempFile(UUID.randomUUID().toString(), null);
    }

    private BufferedOutputStream createOutput(File file)
                                              throws FileNotFoundException {
        return new BufferedOutputStream(new FileOutputStream(file),
                                        BUFFER_SIZE);
    }

    private BufferedInputStream createInput(File file)
                                            throws IOException {
        return new BufferedInputStream(new FileInputStream(file),
                                       BUFFER_SIZE);
    }
}
