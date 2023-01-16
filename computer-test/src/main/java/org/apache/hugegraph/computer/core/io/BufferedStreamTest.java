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
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class BufferedStreamTest {

    private static final int BUFFER_SIZE = 128;

    @Test
    public void testConstructor() throws IOException {
        File file = createTempFile();
        try {
            OutputStream os = new FileOutputStream(file);
            try (BufferedStreamOutput output = new BufferedStreamOutput(os)) {
                Assert.assertEquals(0, output.position());
            }
            InputStream in = new FileInputStream(file);
            try (BufferedStreamInput input = new BufferedStreamInput(in)) {
                Assert.assertEquals(0, input.position());
            }
            Assert.assertThrows(IllegalArgumentException.class, () -> {
                new BufferedStreamOutput(os, 1);
            }, e -> {
                Assert.assertContains("The parameter bufferSize must be >= 8",
                                      e.getMessage());
            });
            Assert.assertThrows(IllegalArgumentException.class, () -> {
                new BufferedStreamInput(in, 1);
            }, e -> {
                Assert.assertContains("The parameter bufferSize must be >= 8",
                                      e.getMessage());
            });
        } finally {
            FileUtils.deleteQuietly(file);
        }
    }

    @Test
    public void testWriteInt() throws IOException {
        File file = createTempFile();
        try {
            try (BufferedStreamOutput output = createOutput(file)) {
                for (int i = -128; i <= 127; i++) {
                    output.writeInt(i);
                }
                output.writeInt(Integer.MAX_VALUE);
                output.writeInt(Integer.MIN_VALUE);
            }
            try (BufferedStreamInput input = createInput(file)) {
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
        File file = createTempFile();
        try {
            try (BufferedStreamOutput output = createOutput(file)) {
                // 256 bytes
                for (int i = 0; i < 64; i++) {
                    output.writeInt(i);
                }
                // Write at start point of current buffer
                output.writeFixedInt(128, 1);
                // Write at middle point of current buffer
                output.writeFixedInt(200, 2);
                // Write at end point of current buffer
                output.writeFixedInt(252, 3);
                // Write at previous buffer
                Assert.assertThrows(IOException.class, () -> {
                    output.writeFixedInt(100, 4);
                }, e -> {
                    Assert.assertContains("underflows the start position",
                                          e.getMessage());
                });
                // Write at next buffer
                Assert.assertThrows(IOException.class, () -> {
                    output.writeFixedInt(256, 5);
                }, e -> {
                    Assert.assertContains("overflows the write position",
                                          e.getMessage());
                });

                output.writeInt(Integer.MAX_VALUE);
                output.writeInt(Integer.MIN_VALUE);
            }
            try (BufferedStreamInput input = createInput(file)) {
                for (int i = 0; i < 64; i++) {
                    int position = i * 4;
                    switch (position) {
                        case 128:
                            Assert.assertEquals(1, input.readInt());
                            break;
                        case 200:
                            Assert.assertEquals(2, input.readInt());
                            break;
                        case 252:
                            Assert.assertEquals(3, input.readInt());
                            break;
                        default:
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
    public void testWriteByteArray() throws IOException {
        int loopTimes = 129;
        byte[] array = UnitTestBase.randomBytes(10);
        File file = createTempFile();
        try {
            try (BufferedStreamOutput output = createOutput(file)) {
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
            try (BufferedStreamInput input = createInput(file)) {
                for (int i = 0; i < loopTimes - 1; i++) {
                    input.readFully(arrayRead);
                    Assert.assertArrayEquals(array, arrayRead);
                }
                Assert.assertThrows(IOException.class, () -> {
                    input.readFully(new byte[20]);
                }, e -> {
                    Assert.assertContains("overflows buffer",
                                          e.getMessage());
                });
            }
        } finally {
            FileUtils.deleteQuietly(file);
        }
    }

    @Test
    public void testWriteLargeByteArray() throws IOException {
        int size = 10;
        int arraySize = 1280; // large than buffer size
        byte[] array = UnitTestBase.randomBytes(arraySize);
        File file = createTempFile();
        try {
            try (BufferedStreamOutput output = createOutput(file)) {
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

            try (BufferedStreamInput input = createInput(file)) {
                for (int i = 0; i < size - 1; i++) {
                    input.readFully(arrayRead);
                    Assert.assertArrayEquals(array, arrayRead);
                }
                Assert.assertThrows(IOException.class, () -> {
                    input.readFully(new byte[arraySize * 2]);
                }, e -> {
                    Assert.assertContains("There is no enough data in " +
                                          "input stream",
                                          e.getMessage());
                });
            }
        } finally {
            FileUtils.deleteQuietly(file);
        }
    }

    @Test
    public void testSeek() throws IOException {
        int size = 1024;
        File file = createTempFile();
        try {
            try (BufferedStreamOutput output = createOutput(file)) {
                for (int i = 0; i < size; i++) {
                    output.seek(i * 4);
                    output.writeInt(i);
                }
            }
            try (BufferedStreamInput input = createInput(file)) {
                for (int i = 0; i < size; i++) {
                    input.seek(i * 4);
                    Assert.assertEquals(i, input.readInt());
                }
                Assert.assertThrows(IOException.class, () -> {
                    input.seek(size * 4 + 1);
                }, e -> {
                    Assert.assertContains("reach the end of input stream",
                                          e.getMessage());
                });
            }
        } finally {
            FileUtils.deleteQuietly(file);
        }
    }

    @Test
    public void testSeekOutRange() throws IOException {
        int size = 1024;
        File file = createTempFile();
        try {
            try (BufferedStreamOutput output = createOutput(file)) {
                for (int i = 0; i < size; i++) {
                    output.seek(i * 4);
                    output.writeInt(i);
                }
                // Overwrite last two elements
                output.seek((size - 2) * 4);
                output.writeInt(Integer.MAX_VALUE);
                output.writeInt(Integer.MIN_VALUE);
                // The position is underflow the buffer
                Assert.assertThrows(IOException.class, () -> {
                    output.seek(output.position() - BUFFER_SIZE - 2);
                }, e -> {
                    Assert.assertContains("out of range", e.getMessage());
                });

                // The position after the current position
                Assert.assertThrows(IOException.class, () -> {
                    output.seek(output.position() + 2);
                }, e -> {
                    Assert.assertContains("out of range", e.getMessage());
                });
            }
            try (BufferedStreamInput input = createInput(file)) {
                for (int i = 0; i < size - 2; i++) {
                    input.seek(i * 4);
                    Assert.assertEquals(i, input.readInt());
                }
                Assert.assertEquals(Integer.MAX_VALUE, input.readInt());
                Assert.assertEquals(Integer.MIN_VALUE, input.readInt());
                Assert.assertThrows(IOException.class, () -> {
                    input.seek(input.position() - BUFFER_SIZE - 2);
                }, e -> {
                    Assert.assertContains("underflows the start position",
                                          e.getMessage());
                });
            }
        } finally {
            FileUtils.deleteQuietly(file);
        }
    }

    @Test
    public void testSkip() throws IOException {
        File file = createTempFile();
        try {
            try (BufferedStreamOutput output = createOutput(file)) {
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

            try (BufferedStreamInput input = createInput(file)) {
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
        File file = createTempFile();
        try {
            try (BufferedStreamOutput output = createOutput(file)) {
                for (int i = 0; i <= size; i++) {
                    Assert.assertEquals(i * 4, output.position());
                    output.writeInt(i);
                }
            }
            try (BufferedStreamInput input = createInput(file)) {
                for (int i = 0; i <= size; i++) {
                    Assert.assertEquals(i * 4, input.position());
                    Assert.assertEquals(i, input.readInt());
                }
            }
        } finally {
            FileUtils.deleteQuietly(file);
        }
    }

    @Test
    public void testAvailable() throws IOException {
        int size = 1;
        File file = createTempFile();
        try {
            try (BufferedStreamOutput output = createOutput(file)) {
                for (int i = 0; i < size; i++) {
                    output.writeInt(i);
                }
            }
            try (BufferedStreamInput input = createInput(file)) {
                Assert.assertEquals(Long.MAX_VALUE, input.available());
            }
        } finally {
            FileUtils.deleteQuietly(file);
        }
    }

    private static File createTempFile() throws IOException {
        return File.createTempFile(UUID.randomUUID().toString(), null);
    }

    private static BufferedStreamOutput createOutput(File file)
                                        throws FileNotFoundException {
        return new BufferedStreamOutput(new FileOutputStream(file),
                                        BUFFER_SIZE);
    }

    private static BufferedStreamInput createInput(File file)
                                                   throws IOException {
        return new BufferedStreamInput(new FileInputStream(file),
                                       BUFFER_SIZE);
    }
}
