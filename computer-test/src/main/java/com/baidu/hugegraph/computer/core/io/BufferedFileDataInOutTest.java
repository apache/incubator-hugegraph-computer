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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.UnitTestBase;
import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.util.Log;

public class BufferedFileDataInOutTest {

    private static final Logger LOG = Log.logger(
                                          BufferedFileDataInOutTest.class);

    private static final int BUFFER_SIZE = 128;

    @Test
    public void testConstructor() throws IOException {
        File file  = this.createTempFile();
        try (BufferedFileDataOutput output = new BufferedFileDataOutput(file)) {
            Assert.assertEquals(0, output.position());
        }
        try (BufferedFileDataInput input = new BufferedFileDataInput(file)) {
            Assert.assertEquals(0, input.position());
        }
        FileUtils.deleteQuietly(file);
    }

    @Test
    public void testInt() throws IOException {
        File file = this.createTempFile();
        try (BufferedFileDataOutput output = this.createOutput(file)) {
            for (int i = -128; i <= 127; i++) {
                output.writeInt(i);
            }
            output.writeInt(Integer.MAX_VALUE);
            output.writeInt(Integer.MIN_VALUE);
        }
        try (BufferedFileDataInput input = this.createInput(file)) {
            for (int i = -128; i <= 127; i++) {
                Assert.assertEquals(i, input.readInt());
            }
            Assert.assertEquals(Integer.MAX_VALUE, input.readInt());
            Assert.assertEquals(Integer.MIN_VALUE, input.readInt());
        }
        FileUtils.deleteQuietly(file);
    }

    @Test
    public void testByteArray() throws IOException {
        int loopTimes = 129;
        byte[] array = UnitTestBase.randomBytes(10);
        File file = this.createTempFile();
        try (BufferedFileDataOutput output = this.createOutput(file)) {
            for (int i = 0; i <= loopTimes; i++) {
                output.write(array);
            }
        }

        byte[] arrayRead = new byte[10];
        try (DataInputStream dis = new DataInputStream(
                                   new FileInputStream(file))) {
            for (int i = 0; i <= loopTimes; i++) {
                dis.readFully(arrayRead);
                Assert.assertArrayEquals(array, arrayRead);
            }
        }
        try (DataInputStream dis = new DataInputStream(
                                   new FileInputStream(file))) {
            for (int i = 0; i <= loopTimes; i++) {
                dis.readFully(arrayRead);
                Assert.assertArrayEquals(array, arrayRead);
            }
        }
        try (BufferedFileDataInput input = this.createInput(file)) {
            for (int i = 0; i <= loopTimes; i++) {
                input.readFully(arrayRead);
                Assert.assertArrayEquals(array, arrayRead);
            }
        }
        FileUtils.deleteQuietly(file);
    }

    @Test
    public void testLargeByteArray() throws IOException {
        int loopTimes = 10;
        int arraySize = 1280; // large than buffer size
        byte[] array = UnitTestBase.randomBytes(arraySize);
        File file = this.createTempFile();
        try (BufferedFileDataOutput output = this.createOutput(file)) {
            for (int i = 0; i <= loopTimes; i++) {
                output.write(array);
            }
        }

        byte[] arrayRead = new byte[arraySize];
        try (DataInputStream dis = new DataInputStream(
                                   new FileInputStream(file))) {
            for (int i = 0; i <= loopTimes; i++) {
                dis.readFully(arrayRead);
                Assert.assertArrayEquals(array, arrayRead);
            }
        }

        try (BufferedFileDataInput input = this.createInput(file)) {
            for (int i = 0; i <= loopTimes; i++) {
                input.readFully(arrayRead);
                Assert.assertArrayEquals(array, arrayRead);
            }
        }
        FileUtils.deleteQuietly(file);
    }

    @Test
    public void testSeekAtStart() throws IOException {
        File file = this.createTempFile();
        try (BufferedFileDataOutput output = this.createOutput(file)) {
            for (int i = -128; i <= 127; i++) {
                output.writeInt(i);
            }
            // Overwrite last 2 elements
            output.seek(0);
            output.writeInt(Integer.MAX_VALUE);
            output.writeInt(Integer.MIN_VALUE);
        }
        try (BufferedFileDataInput input = this.createInput(file)) {
            Assert.assertEquals(Integer.MAX_VALUE, input.readInt());
            Assert.assertEquals(Integer.MIN_VALUE, input.readInt());
            for (int i = -126; i <= 125; i++) {
                Assert.assertEquals(i, input.readInt());
            }
            input.seek(0);
            Assert.assertEquals(Integer.MAX_VALUE, input.readInt());
            Assert.assertEquals(Integer.MIN_VALUE, input.readInt());
        }
        FileUtils.deleteQuietly(file);
    }

    @Test
    public void testSeekAtMiddle() throws IOException {
        long seekPosition = 192;
        File file = this.createTempFile();
        try (BufferedFileDataOutput output = this.createOutput(file)) {
            for (int i = -128; i <= 127; i++) {
                output.writeInt(i);
            }
            // Overwrite last 2 elements
            output.seek(seekPosition);
            output.writeInt(Integer.MAX_VALUE);
            output.writeInt(Integer.MIN_VALUE);
        }

        try (BufferedFileDataInput input = this.createInput(file)) {
            int end = -128 + (int) (seekPosition / Constants.INT_LEN);
            for (int i = -128; i < end; i++) {
                Assert.assertEquals(i, input.readInt());
            }
            Assert.assertEquals(Integer.MAX_VALUE, input.readInt());
            Assert.assertEquals(Integer.MIN_VALUE, input.readInt());
            for (int i = end + 2; i <= 127; i++) {
                Assert.assertEquals(i, input.readInt());
            }
        }
        FileUtils.deleteQuietly(file);
    }

    @Test
    public void testInputSeekAtRandom() throws IOException {
        int size = 1024;
        File file = this.createTempFile();
        try (BufferedFileDataOutput output = this.createOutput(file)) {
            for (int i = 0; i <= size; i++) {
                output.writeInt(i);
            }
        }
        Random random = new Random();
        try (BufferedFileDataInput input = this.createInput(file)) {
            for (int i = 0; i <= 10; i++) {
                long position = 4 * random.nextInt(size);
                input.seek(position);
                Assert.assertEquals(position / 4, input.readInt());
            }
        }
        FileUtils.deleteQuietly(file);
    }

    @Test
    public void testSeekAtEnd() throws IOException {
        File file = this.createTempFile();
        try (BufferedFileDataOutput output = this.createOutput(file)) {
            for (int i = -128; i <= 127; i++) {
                output.writeInt(i);
            }
            // Overwrite last 2 elements
            output.seek(256 * 4 - 8);
            output.writeInt(Integer.MAX_VALUE);
            output.writeInt(Integer.MIN_VALUE);
        }

        try (BufferedFileDataInput input = this.createInput(file)) {
            for (int i = -128; i <= 125; i++) {
                Assert.assertEquals(i, input.readInt());
            }
            Assert.assertEquals(Integer.MAX_VALUE, input.readInt());
            Assert.assertEquals(Integer.MIN_VALUE, input.readInt());
            input.seek(input.position() - 8);
            Assert.assertEquals(Integer.MAX_VALUE, input.readInt());
            Assert.assertEquals(Integer.MIN_VALUE, input.readInt());
        }
        FileUtils.deleteQuietly(file);
    }

    @Test
    public void testSkip() throws IOException {
        File file = this.createTempFile();
        try (BufferedFileDataOutput output = this.createOutput(file)) {
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

        try (BufferedFileDataInput input = this.createInput(file)) {
            for (int i = -128; i <= 127; i++) {
                Assert.assertEquals(i, input.readByte());
            }
            input.skip(4);
            Assert.assertEquals(127, input.readByte());
            input.skip(1280);
            for (int i = 1; i <= 1280; i++) {
                Assert.assertEquals((byte) i, input.readByte());
            }
        }
        FileUtils.deleteQuietly(file);
    }

    @Test
    public void testPosition() throws IOException {
        int size = 1024;
        File file = this.createTempFile();
        try (BufferedFileDataOutput output = this.createOutput(file)) {
            for (int i = 0; i <= size; i++) {
                Assert.assertEquals(i * 4, output.position());
                output.writeInt(i);
            }
        }
        try (BufferedFileDataInput input = this.createInput(file)) {
            for (int i = 0; i <= size; i++) {
                Assert.assertEquals(i * 4, input.position());
                Assert.assertEquals(i, input.readInt());
            }

            Random random = new Random();
            for (int i = 0; i <= 10; i++) {
                long position = 4 * random.nextInt(size);
                input.seek(position);
                Assert.assertEquals(position / 4, input.readInt());
                Assert.assertEquals(position + 4, input.position());
            }
        }
        FileUtils.deleteQuietly(file);
    }

    // @Test
    public void testLongPerformanceUnsafe() throws IOException {
        long size = 1024 * 1024 * 1024;
        long startTime;
        long endTime;
        long time;
        File fileUnsafe = new File("long-unsafe.bin");
        startTime = System.currentTimeMillis();
        try (BufferedFileDataOutput output = new BufferedFileDataOutput(
                                                 fileUnsafe)) {
            for (long i = 0; i <= size / 8; i++) {
                output.writeLong(i);
            }
        }
        endTime = System.currentTimeMillis();
        time = endTime - startTime;
        LOG.info(String.format("Write %s bytes use BufferedFileDataOutput" +
                               ".writeLong takes %s ms", size, time));

        startTime = System.currentTimeMillis();
        try (BufferedFileDataInput input = new BufferedFileDataInput(
                                               fileUnsafe)) {
            for (long i = 0; i <= size / 8; i++) {
                input.readLong();
            }
        }
        endTime = System.currentTimeMillis();
        time = endTime - startTime;
        LOG.info(String.format("Read %s bytes use BufferedFileDataInput" +
                               ".readLong takes %s ms", size, time));
        FileUtils.deleteQuietly(fileUnsafe);
    }

    // @Test
    public void testLongPerformanceNormal() throws IOException {
        long size = 1024 * 1024 * 1024;
        long startTime;
        long endTime;
        long time;

        File dataFile = new File("long-data.bin");
        startTime = System.currentTimeMillis();
        try (DataOutputStream output = new DataOutputStream(
                                       new BufferedOutputStream(
                                       new FileOutputStream(dataFile)))) {
            for (long i = 0; i <= size / 8; i++) {
                output.writeLong(i);
            }
        }
        endTime = System.currentTimeMillis();
        time = endTime - startTime;
        LOG.info(String.format("Write %s bytes use DataOutputStream.writeLong" +
                               " takes %s ms", size, time));

        startTime = System.currentTimeMillis();
        try (DataInputStream input = new DataInputStream(
                                     new BufferedInputStream(
                                     new FileInputStream(dataFile)))) {
            for (long i = 0; i <= size / 8; i++) {
                input.readLong();
            }
        }
        endTime = System.currentTimeMillis();
        time = endTime - startTime;
        LOG.info(String.format("Read %s bytes use DataInputStream.readLong " +
                               "takes %s ms", size, time));
        FileUtils.deleteQuietly(dataFile);
    }

    // @Test
    public void testIntPerformanceIntUnsafe() throws IOException {
        int size = 1024 * 1024 * 1024;

        long startTime;
        long endTime;
        File unsafeFile = new File("int-unsafe.bin");
        startTime = System.currentTimeMillis();
        try (BufferedFileDataOutput output = new BufferedFileDataOutput(
                                                 unsafeFile)) {
            for (int i = 0; i <= size / 4; i++) {
                output.writeInt(i);
            }
        }
        endTime = System.currentTimeMillis();
        long time = endTime - startTime;
        LOG.info(String.format("Write %s bytes use BufferedFileDataOutput" +
                               ".writeInt takes %s ms", size, time));

        startTime = System.currentTimeMillis();
        try (BufferedFileDataInput input = new BufferedFileDataInput(
                                               unsafeFile)) {
            for (int i = 0; i <= size / 4; i++) {
                input.readInt();
            }
        }
        endTime = System.currentTimeMillis();
        time = endTime - startTime;
        LOG.info(String.format("Read %s bytes use BufferedFileDataInput" +
                               ".readInt takes %s ms", size, time));
        FileUtils.deleteQuietly(unsafeFile);
    }

    // @Test
    public void testIntPerformanceNormal() throws IOException {
        int size = 1024 * 1024 * 1024;
        long startTime;
        long endTime;
        long time;
        File dataFile = new File("int-data-out.bin");
        startTime = System.currentTimeMillis();
        try (DataOutputStream output = new DataOutputStream(
                                       new BufferedOutputStream(
                                       new FileOutputStream(dataFile)))) {
            for (int i = 0; i <= size / 4; i++) {
                output.writeInt(i);
            }
        }
        endTime = System.currentTimeMillis();
        time = endTime - startTime;
        LOG.info(String.format("Write %s bytes use DataOutputStream.writeInt" +
                               " takes %s ms", size, time));

        startTime = System.currentTimeMillis();
        try (DataInputStream input = new DataInputStream(
                                     new BufferedInputStream(
                                     new FileInputStream(dataFile)))) {
            for (int i = 0; i <= size / 4; i++) {
                input.readInt();
            }
        }
        endTime = System.currentTimeMillis();
        time = endTime - startTime;
        LOG.info(String.format("Read %s bytes use DataInputStream.readInt " +
                               "takes %s ms", size, time));
        FileUtils.deleteQuietly(dataFile);
    }

    // @Test
    public void testByteArrayPerformanceUnsafe() throws IOException {
        int size = 1024 * 1024 * 1024;
        long startTime;
        long endTime;
        long time;
        byte[] writeArray = UnitTestBase.randomBytes(16);
        byte[] readArray = new byte[16];

        File unsafeFile = new File("int-unsafe-out.bin");
        startTime = System.currentTimeMillis();
        try (BufferedFileDataOutput output = new BufferedFileDataOutput(
                                                 unsafeFile)) {
            for (int i = 0; i <= size / writeArray.length; i++) {
                output.write(writeArray);
            }
        }
        endTime = System.currentTimeMillis();
        time = endTime - startTime;
        LOG.info(String.format("Write %s bytes use BufferedFileDataOutput" +
                               ".write takes %s ms", size, time));

        startTime = System.currentTimeMillis();
        try (BufferedFileDataInput input = new BufferedFileDataInput(
                                               unsafeFile)) {
            for (int i = 0; i <= size / readArray.length; i++) {
                input.readFully(readArray);
            }
        }
        endTime = System.currentTimeMillis();
        time = endTime - startTime;
        LOG.info(String.format("Read %s bytes use BufferedFileDataInput" +
                               ".readFully takes %s ms", size, time));

        FileUtils.deleteQuietly(unsafeFile);
    }

    // @Test
    public void testByteArrayPerformanceNormal() throws IOException {
        int size = 1024 * 1024 * 1024;
        long startTime;
        long endTime;
        long time;
        byte[] writeArray = UnitTestBase.randomBytes(16);
        byte[] readArray = new byte[16];
        File dataFile = new File("int-data-out.bin");
        startTime = System.currentTimeMillis();
        try (DataOutputStream output = new DataOutputStream(
                                       new BufferedOutputStream(
                                       new FileOutputStream(dataFile)))) {
            for (int i = 0; i <= size / writeArray.length; i++) {
                output.write(writeArray);
            }
        }
        endTime = System.currentTimeMillis();
        time = endTime - startTime;
        LOG.info(String.format("Write %s bytes use DataOutputStream.write " +
                               "takes %s ms", size, time));

        startTime = System.currentTimeMillis();
        try (DataInputStream input = new DataInputStream(
                new BufferedInputStream(
                        new FileInputStream(dataFile)))) {
            for (int i = 0; i <= size / writeArray.length; i++) {
                input.readFully(readArray);
            }
        }
        endTime = System.currentTimeMillis();
        time = endTime - startTime;
        LOG.info(String.format("Read %s bytes use DataInputStream.readFully " +
                               "takes %s ms", size, time));
        FileUtils.deleteQuietly(dataFile);
    }

    private File createTempFile() throws IOException {
        return File.createTempFile(UUID.randomUUID().toString(), null);
    }

    private BufferedFileDataOutput createOutput(File file)
                                                throws FileNotFoundException {
        return new BufferedFileDataOutput(new RandomAccessFile(file, "rw"),
                                          BUFFER_SIZE);
    }

    private BufferedFileDataInput createInput(File file)
                                              throws IOException {
        return new BufferedFileDataInput(new RandomAccessFile(file, "rw"),
                                         BUFFER_SIZE);
    }
}
