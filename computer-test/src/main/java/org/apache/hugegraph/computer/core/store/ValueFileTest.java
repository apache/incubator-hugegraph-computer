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

package org.apache.hugegraph.computer.core.store;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.io.UnsafeBytesInput;
import org.apache.hugegraph.computer.core.io.UnsafeBytesOutput;
import org.apache.hugegraph.computer.core.store.file.seqfile.ValueFileInput;
import org.apache.hugegraph.computer.core.store.file.seqfile.ValueFileOutput;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.util.E;
import org.junit.BeforeClass;
import org.junit.Test;

public class ValueFileTest {

    private static Config CONFIG;

    @BeforeClass
    public static void init() {
        CONFIG = UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.VALUE_FILE_MAX_SEGMENT_SIZE, "32"
        );
    }

    @Test
    public void testConstructor() throws IOException {
        int bufferCapacity = 13;
        File dir = createTempDir();
        try {
            try (ValueFileOutput output = new ValueFileOutput(CONFIG, dir,
                                                              bufferCapacity)) {
                Assert.assertEquals(0, output.position());
            }
            Assert.assertThrows(IllegalArgumentException.class, () -> {
                new ValueFileOutput(CONFIG, dir, 1);
            }, e -> {
                Assert.assertContains("bufferCapacity must be >= 8",
                                      e.getMessage());
            });

            try (ValueFileInput input = new ValueFileInput(CONFIG, dir,
                                                           bufferCapacity)) {
                Assert.assertEquals(0, input.position());
            }
            Assert.assertThrows(IllegalArgumentException.class, () -> {
                new ValueFileInput(CONFIG, dir, 1);
            }, e -> {
                Assert.assertContains("The parameter bufferSize must be >= 8",
                                      e.getMessage());
            });

            Config config = UnitTestBase.updateWithRequiredOptions(
                    ComputerOptions.VALUE_FILE_MAX_SEGMENT_SIZE,
                    String.valueOf(Integer.MAX_VALUE)
            );
            try (ValueFileOutput output = new ValueFileOutput(config, dir)) {
                Assert.assertEquals(0, output.position());
            }
            try (ValueFileInput input = new ValueFileInput(config, dir)) {
                Assert.assertEquals(0, input.position());
            }
        } finally {
            FileUtils.deleteQuietly(dir);
        }
    }

    @Test
    public void testByteArray() throws IOException {
        File dir = createTempDir();
        final int bufferSize = 13;

        byte[] bytes;
        try {
            try (ValueFileOutput output = new ValueFileOutput(CONFIG, dir,
                                                              bufferSize)) {
                // The remaining capacity of the buffer can hold
                bytes = orderBytesBySize(5);
                output.write(bytes);

                /*
                 * The remaining capacity of the buffer can't hold and write
                 * size is smaller than buffer capacity
                 */
                bytes = orderBytesBySize(11);
                output.write(bytes);

                /*
                 * Write size is greater than buffer capacity and remaining of
                 * segment can hold write size
                 */
                bytes = orderBytesBySize(14);
                output.write(bytes);

                /*
                 * Write size is greater than buffer capacity and remaining of
                 * segment can't hold write size
                 */
                bytes = orderBytesBySize(80);
                output.write(bytes);
            }

            try (ValueFileInput input = new ValueFileInput(CONFIG, dir,
                                                           bufferSize)) {
                assertBytes(5, input.readBytes(5));
                assertBytes(11, input.readBytes(11));
                assertBytes(14, input.readBytes(14));
                assertBytes(80, input.readBytes(80));
            }
        } finally {
            FileUtils.deleteQuietly(dir);
        }
    }

    @Test
    public void testWriteInt() throws IOException {
        File dir = createTempDir();
        final int bufferSize = 10;

        try {
            try (ValueFileOutput output = new ValueFileOutput(CONFIG, dir,
                                                              bufferSize)) {
                for (int i = 0; i < 50; i++) {
                    output.writeInt(i);
                }
            }

            try (ValueFileInput input = new ValueFileInput(CONFIG, dir,
                                                           bufferSize)) {
                for (int i = 0; i < 50; i++) {
                    Assert.assertEquals(i, input.readInt());
                }
                Assert.assertThrows(IOException.class, input::readInt, e -> {
                    Assert.assertContains("overflows buffer",
                                          e.getMessage());
                });
            }
        } finally {
            FileUtils.deleteQuietly(dir);
        }
    }

    @Test
    public void testWriteIntWithPosition() throws IOException {
        File dir = createTempDir();
        final int bufferSize = 20;

        try {
            try (ValueFileOutput output = new ValueFileOutput(CONFIG, dir,
                                                              bufferSize)) {
                for (int i = 0; i < 50; i++) {
                    // Write position in buffer
                    if (i == 2) {
                        output.writeFixedInt(4, 100);
                    }
                    output.writeInt(i);
                }
                output.writeFixedInt(Integer.BYTES * 10, 100);
            }

            try (ValueFileInput input = new ValueFileInput(CONFIG, dir,
                                                           bufferSize)) {
                for (int i = 0; i < 50; i++) {
                    if (i == 1 || i == 10) {
                        Assert.assertEquals(100, input.readInt());
                        continue;
                    }
                    Assert.assertEquals(i, input.readInt());
                }
            }
        } finally {
            FileUtils.deleteQuietly(dir);
        }
    }

    @Test
    public void testSeek() throws IOException {
        File dir = createTempDir();
        final int bufferSize = 15;

        try {
            // Test ValueFileInput
            try (ValueFileOutput output = new ValueFileOutput(CONFIG, dir,
                                                              bufferSize)) {
                for (int i = 0; i < 50; i++) {
                    output.writeInt(i);
                }
            }

            try (ValueFileInput input = new ValueFileInput(CONFIG, dir,
                                                           bufferSize)) {
                Long fileLength = Whitebox.getInternalState(input,
                                                            "fileLength");
                Assert.assertEquals(200L, fileLength);

                long position;
                // Position in buffer
                position = 13L;
                assertSeek(fileLength, input, position);

                // Position not in buffer but in current segment
                position = 28L;
                assertSeek(fileLength, input, position);

                // Position not in buffer and not in current segment
                position = 200L;
                assertSeek(fileLength, input, position);
                position = 3L;
                assertSeek(fileLength, input, position);
                position = 3L;
                assertSeek(fileLength, input, position);
                position = 68L;
                assertSeek(fileLength, input, position);
                position = 0L;
                assertSeek(fileLength, input, position);

                // Position out of bound
                position = 300L;
                long finalPosition = position;
                Assert.assertThrows(EOFException.class, () -> {
                    input.seek(finalPosition);
                }, e -> {
                    Assert.assertContains("reach the end of file",
                                          e.getMessage());
                });

                input.seek(16 * Integer.BYTES);
                Assert.assertEquals(16, input.readInt());

                // Random seek and assert data
                Random random = new Random();
                for (int i = 0; i < 10; i++) {
                    int num = random.nextInt(49);
                    input.seek(num * Integer.BYTES);
                    Assert.assertEquals(num, input.readInt());
                }
            }

            // Test ValueFileOutput
            try (ValueFileOutput output = new ValueFileOutput(CONFIG, dir,
                                                              bufferSize)) {
                // 200, 100, 2, 3, 4.....
                for (int i = 0; i < 50; i++) {
                    // Position in buffer
                    if (i == 2) {
                        output.seek(4);
                        output.writeInt(100);
                    }
                    // Position not int buffer
                    if (i == 10) {
                        long oldPosition = output.position();
                        output.seek(0);
                        output.writeInt(200);
                        output.seek(oldPosition);
                    }
                    output.writeInt(i);
                }

                Assert.assertThrows(EOFException.class, () -> {
                    output.seek(500L);
                }, e -> {
                    Assert.assertContains("reach the end of file",
                                          e.getMessage());
                });
            }

            try (ValueFileInput input = new ValueFileInput(CONFIG, dir,
                                                           bufferSize)) {
                for (int i = 0; i < 50; i++) {
                    if (i == 0) {
                        Assert.assertEquals(200, input.readInt());
                        continue;
                    }
                    if (i == 1) {
                        Assert.assertEquals(100, input.readInt());
                        continue;
                    }
                    Assert.assertEquals(i, input.readInt());
                }
            }
        } finally {
            FileUtils.deleteQuietly(dir);
        }
    }

    @Test
    public void testSkip() throws IOException {
        File dir = createTempDir();
        final int bufferSize = 15;

        try {
            try (ValueFileOutput output = new ValueFileOutput(CONFIG, dir,
                                                              bufferSize)) {
                // 100, 1, 2, 200, 4, 5 ......
                for (int i = 0; i < 50; i++) {
                    output.writeInt(i);
                }
                output.seek(0);
                output.writeInt(100);

                output.skip(8);
                output.writeInt(200);
            }

            try (ValueFileInput input = new ValueFileInput(CONFIG, dir,
                                                           bufferSize)) {
                Assert.assertEquals(100, input.readInt());

                input.skip(8);
                Assert.assertEquals(200, input.readInt());

                input.seek(20 * Integer.BYTES);
                Assert.assertEquals(20, input.readInt());

                input.skip(10 * Integer.BYTES);
                Assert.assertEquals(31, input.readInt());

                Assert.assertThrows(IllegalArgumentException.class, () -> {
                    input.skip(500L);
                }, e -> {
                    Assert.assertContains("because don't have enough data",
                                          e.getMessage());
                });
            }
        } finally {
            FileUtils.deleteQuietly(dir);
        }
    }

    @Test
    public void testDuplicate() throws IOException {
        File dir = createTempDir();
        final int bufferSize = 15;

        try {
            try (ValueFileOutput output = new ValueFileOutput(CONFIG, dir,
                                                              bufferSize)) {
                for (int i = 0; i < 50; i++) {
                    output.writeInt(i);
                }
            }

            try (ValueFileInput input = new ValueFileInput(CONFIG, dir,
                                                           bufferSize)) {
                input.seek(20 * Integer.BYTES);
                Assert.assertEquals(20, input.readInt());

                UnsafeBytesInput other = input.duplicate();
                Assert.assertEquals(21, other.readInt());
                Assert.assertEquals(21, input.readInt());
            }
        } finally {
            FileUtils.deleteQuietly(dir);
        }
    }

    @Test
    public void testCompare() throws IOException {
        File dir = createTempDir();
        final int bufferSize = 15;

        try {
            try (ValueFileOutput output = new ValueFileOutput(CONFIG, dir,
                                                              bufferSize)) {
                String data = "HugeGraph is a fast-speed and highly-scalable " +
                              "graph database";
                output.write(data.getBytes());
            }

            try (ValueFileInput input1 = new ValueFileInput(CONFIG, dir,
                                                            bufferSize);
                 ValueFileInput input2 = new ValueFileInput(CONFIG, dir,
                                                            bufferSize)) {
                int result;

                // All in buffer
                result = input1.compare(0, 3, input2, 1, 3);
                Assert.assertLt(0, result);

                result = input1.compare(0, 3, input2, 0, 5);
                Assert.assertLt(0, result);

                result = input1.compare(1, 3, input2, 1, 3);
                Assert.assertEquals(0, result);

                // The input1 in buffer, input2 not in buffer
                result = input1.compare(0, 3, input2, 21, 3);
                Assert.assertLt(0, result);

                result = input1.compare(1, 3, input2, 23, 3);
                Assert.assertGt(0, result);

                result = input1.compare(0, 3, input2, 23, 5);
                Assert.assertLt(0, result);

                result = input1.compare(3, 1, input2, 23, 1);
                Assert.assertEquals(0, result);

                // The input1 not in buffer, input2 in buffer
                input2.seek(20);
                result = input1.compare(23, 5, input2, 23, 5);
                Assert.assertEquals(0, result);

                result = input1.compare(23, 12, input2, 23, 5);
                Assert.assertGt(0, result);

                // All not in buffer
                input2.seek(0);
                result = input1.compare(23, 5, input2, 23, 5);
                Assert.assertEquals(0, result);

                result = input1.compare(23, 12, input2, 23, 5);
                Assert.assertGt(0, result);

                // Compare with different class
                UnsafeBytesOutput output = new UnsafeBytesOutput(20);
                output.writeBytes("banana");
                UnsafeBytesInput input = new UnsafeBytesInput(output.buffer());
                output.close();

                result = input1.compare(0, 2, input, 0, 4);
                Assert.assertLt(0, result);

                result = input1.compare(1, 5, input, 0, 4);
                Assert.assertGt(0, result);
            }
        } finally {
            FileUtils.deleteQuietly(dir);
        }
    }

    private static byte[] orderBytesBySize(int size) {
        E.checkArgument(size <= Byte.MAX_VALUE,
                        "Size must be <= %s but get '%s'",
                        Byte.MAX_VALUE, size);
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++) {
            bytes[i] = (byte) i;
        }
        return bytes;
    }

    private static void assertBytes(int expectSize, byte[] bytes) {
        E.checkArgument(expectSize <= Byte.MAX_VALUE,
                        "Size must be <= %s but get '%s'",
                        Byte.MAX_VALUE, expectSize);
        for (int i = 0; i < expectSize; i++) {
            Assert.assertEquals(i, bytes[i]);
        }
    }

    private static File createTempDir() {
        File dir = new File(UUID.randomUUID().toString());
        dir.mkdirs();
        return dir;
    }

    private static void assertSeek(long fileLength, ValueFileInput input,
                                   long position) throws IOException {
        input.seek(position);
        Assert.assertEquals(position, input.position());
        Assert.assertEquals(fileLength - position, input.available());
    }
}
