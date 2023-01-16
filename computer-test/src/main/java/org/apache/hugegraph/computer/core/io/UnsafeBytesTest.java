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
import java.io.UTFDataFormatException;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

@SuppressWarnings("resource")
public class UnsafeBytesTest {

    private static final int SIZE = Constants.SMALL_BUF_SIZE;

    @Test
    public void testConstructor() {
        UnsafeBytesOutput output = new UnsafeBytesOutput(SIZE);
        Assert.assertEquals(0, output.position());

        UnsafeBytesOutput output2 = new UnsafeBytesOutput(SIZE);
        Assert.assertEquals(0, output2.position());

        UnsafeBytesInput input = new UnsafeBytesInput(output.buffer());
        Assert.assertEquals(0, input.position());
    }

    @Test
    public void testBoolean() throws IOException {
        UnsafeBytesOutput output = new UnsafeBytesOutput(SIZE);
        output.writeBoolean(true);
        output.writeBoolean(false);
        UnsafeBytesInput input = new UnsafeBytesInput(output.toByteArray());
        Assert.assertTrue(input.readBoolean());
        Assert.assertFalse(input.readBoolean());
    }

    @Test
    public void testByte() throws IOException {
        UnsafeBytesOutput output = new UnsafeBytesOutput(SIZE);
        for (int i = -128; i <= 127; i++) {
            output.write(i);
        }
        UnsafeBytesInput input = new UnsafeBytesInput(output.toByteArray());
        for (int i = -128; i <= 127; i++) {
            int value = input.readByte();
            Assert.assertEquals(i, value);
        }
    }

    @Test
    public void testUnsignedByte() throws IOException {
        UnsafeBytesOutput output = new UnsafeBytesOutput(SIZE);
        for (int i = 0; i <= 255; i++) {
            output.write(i);
        }
        UnsafeBytesInput input = new UnsafeBytesInput(output.toByteArray());
        for (int i = 0; i <= 255; i++) {
            int value = input.readUnsignedByte();
            Assert.assertEquals(i, value);
        }
    }

    @Test
    public void testShort() throws IOException {
        UnsafeBytesOutput output = new UnsafeBytesOutput(SIZE);
        for (short i = -128; i <= 127; i++) {
            output.writeShort(i);
        }
        output.writeShort(Short.MAX_VALUE);
        output.writeShort(Short.MIN_VALUE);
        UnsafeBytesInput input = new UnsafeBytesInput(output.toByteArray());
        for (int i = -128; i <= 127; i++) {
            Assert.assertEquals(i, input.readShort());
        }
        Assert.assertEquals(Short.MAX_VALUE, input.readShort());
        Assert.assertEquals(Short.MIN_VALUE, input.readShort());
    }

    @Test
    public void testUnsignedShort() throws IOException {
        UnsafeBytesOutput output = new UnsafeBytesOutput(SIZE);
        for (short i = 0; i <= 255; i++) {
            output.writeShort(i);
        }
        output.writeShort(Short.MAX_VALUE);
        output.writeShort(Short.MIN_VALUE);
        UnsafeBytesInput input = new UnsafeBytesInput(output.toByteArray());
        for (int i = 0; i <= 255; i++) {
            Assert.assertEquals(i, input.readUnsignedShort());
        }
        Assert.assertEquals(Short.MAX_VALUE, input.readUnsignedShort());
        Assert.assertEquals(Short.MIN_VALUE & 0xFFFF,
                            input.readUnsignedShort());
    }

    @Test
    public void testChar() throws IOException {
        UnsafeBytesOutput output = new UnsafeBytesOutput(SIZE);
        for (char i = 'a'; i <= 'z'; i++) {
            output.writeChar(i);
        }
        UnsafeBytesInput input = new UnsafeBytesInput(output.toByteArray());
        for (char i = 'a'; i <= 'z'; i++) {
            char value = input.readChar();
            Assert.assertEquals(i, value);
        }
    }

    @Test
    public void testInt() throws IOException {
        UnsafeBytesOutput output = new UnsafeBytesOutput(SIZE);
        for (int i = -128; i <= 127; i++) {
            output.writeInt(i);
        }
        output.writeInt(Integer.MAX_VALUE);
        output.writeInt(Integer.MIN_VALUE);
        UnsafeBytesInput input = new UnsafeBytesInput(output.toByteArray());
        for (int i = -128; i <= 127; i++) {
            Assert.assertEquals(i, input.readInt());
        }
        Assert.assertEquals(Integer.MAX_VALUE, input.readInt());
        Assert.assertEquals(Integer.MIN_VALUE, input.readInt());
    }

    @Test
    public void testWriteIntWithPosition() throws IOException {
        UnsafeBytesOutput output = new UnsafeBytesOutput(SIZE);
        long position = output.skip(Constants.INT_LEN);
        output.writeInt(2);
        output.writeFixedInt(position, 1);

        UnsafeBytesInput input = new UnsafeBytesInput(output.toByteArray());
        Assert.assertEquals(1, input.readInt());
        Assert.assertEquals(2, input.readInt());
    }

    @Test
    public void testOverWriteWithPosition() {
        Assert.assertThrows(ComputerException.class, () -> {
            UnsafeBytesOutput output = new UnsafeBytesOutput(4);
            output.writeFixedInt(1, 100);
        });

        Assert.assertThrows(ComputerException.class, () -> {
            UnsafeBytesOutput output = new UnsafeBytesOutput(4);
            output.writeFixedInt(2, 100);
        });

        Assert.assertThrows(ComputerException.class, () -> {
            UnsafeBytesOutput output = new UnsafeBytesOutput(4);
            output.writeFixedInt(3, 100);
        });

        Assert.assertThrows(ComputerException.class, () -> {
            UnsafeBytesOutput output = new UnsafeBytesOutput(4);
            output.writeFixedInt(4, 100);
        });
    }

    @Test
    public void testLong() throws IOException {
        UnsafeBytesOutput output = new UnsafeBytesOutput(SIZE);
        for (long i = -128; i <= 127; i++) {
            output.writeLong(i);
        }
        output.writeLong(Long.MAX_VALUE);
        output.writeLong(Long.MIN_VALUE);
        UnsafeBytesInput input = new UnsafeBytesInput(output.toByteArray());
        for (long i = -128; i <= 127; i++) {
            Assert.assertEquals(i, input.readLong());
        }
        Assert.assertEquals(Long.MAX_VALUE, input.readLong());
        Assert.assertEquals(Long.MIN_VALUE, input.readLong());
    }

    @Test
    public void testFloat() throws IOException {
        UnsafeBytesOutput output = new UnsafeBytesOutput(SIZE);
        for (int i = -128; i <= 127; i++) {
            output.writeFloat(i);
        }
        output.writeFloat(Float.MAX_VALUE);
        output.writeFloat(Float.MIN_VALUE);
        UnsafeBytesInput input = new UnsafeBytesInput(output.toByteArray());
        for (int i = -128; i <= 127; i++) {
            Assert.assertEquals(i, input.readFloat(), 0.0D);
        }
        Assert.assertEquals(Float.MAX_VALUE, input.readFloat(), 0.0D);
        Assert.assertEquals(Float.MIN_VALUE, input.readFloat(), 0.0D);
    }

    @Test
    public void testDouble() throws IOException {
        UnsafeBytesOutput output = new UnsafeBytesOutput(SIZE);
        for (int i = -128; i <= 127; i++) {
            output.writeDouble(i);
        }
        output.writeDouble(Double.MAX_VALUE);
        output.writeDouble(Double.MIN_VALUE);
        UnsafeBytesInput input = new UnsafeBytesInput(output.toByteArray());
        for (int i = -128; i <= 127; i++) {
            Assert.assertEquals(i, input.readDouble(), 0.0D);
        }
        Assert.assertEquals(Double.MAX_VALUE, input.readDouble(), 0.0D);
        Assert.assertEquals(Double.MIN_VALUE, input.readDouble(), 0.0D);
    }

    @Test
    public void testByteArray() throws IOException {
        byte[] bytes = "testByteArray".getBytes();
        UnsafeBytesOutput output = new UnsafeBytesOutput(SIZE);
        output.write(bytes);
        byte[] bytesRead = new byte[bytes.length];
        UnsafeBytesInput input = new UnsafeBytesInput(output.toByteArray());
        input.readFully(bytesRead);
        Assert.assertArrayEquals(bytes, bytesRead);
    }

    @Test
    public void testWritePartByteArray() throws IOException {
        byte[] bytes = "testByteArray".getBytes();
        UnsafeBytesOutput output = new UnsafeBytesOutput(SIZE);
        output.write(bytes, 1, bytes.length - 1);
        byte[] bytesRead = new byte[bytes.length];
        UnsafeBytesInput input = new UnsafeBytesInput(output.toByteArray());
        input.readFully(bytesRead, 1, bytes.length - 1);
        bytesRead[0] = bytes[0];
        Assert.assertArrayEquals(bytes, bytesRead);
    }

    @Test
    public void testWriteChars() throws IOException {
        String chars = "testByteArray";
        UnsafeBytesOutput output = new UnsafeBytesOutput(SIZE);
        output.writeChars(chars);
        UnsafeBytesInput input = new UnsafeBytesInput(output.toByteArray());
        for (int i = 0; i < chars.length(); i++) {
            char c = input.readChar();
            Assert.assertEquals(chars.charAt(i), c);
        }
    }

    @Test
    public void testReadLine() {
        UnsafeBytesInput input = new UnsafeBytesInput(Constants.EMPTY_BYTES);
        Assert.assertThrows(ComputerException.class, () -> {
            input.readLine();
        });
    }

    @Test
    public void testUTFWithChineseCharacters() throws IOException {
        String s = "带汉字的字符串";
        UnsafeBytesOutput output = new UnsafeBytesOutput(SIZE);
        output.writeUTF(s);
        UnsafeBytesInput input = new UnsafeBytesInput(output.toByteArray());
        String value = input.readUTF();
        Assert.assertEquals(s, value);
    }

    @Test
    public void testUTF() throws IOException {
        String prefix = "random string";
        UnsafeBytesOutput output = new UnsafeBytesOutput(SIZE);
        for (int i = -128; i <= 127; i++) {
            output.writeUTF(prefix + i);
        }
        UnsafeBytesInput input = new UnsafeBytesInput(output.buffer());
        for (int i = -128; i <= 127; i++) {
            String value = input.readUTF();
            Assert.assertEquals(prefix + i, value);
        }
    }

    @Test
    public void testUTFBoundary() throws IOException {
        UnsafeBytesOutput output = new UnsafeBytesOutput(SIZE);
        String s1 = UnitTestBase.randomString(65535);
        output.writeUTF(s1);
        String s2 = UnitTestBase.randomString(65536);
        Assert.assertThrows(UTFDataFormatException.class, () -> {
            output.writeUTF(s2);
        });
        UnsafeBytesInput input = new UnsafeBytesInput(output.buffer());
        String value = input.readUTF();
        Assert.assertEquals(s1, value);
    }

    @Test
    public void testSkipBytes() throws IOException {
        UnsafeBytesOutput output = new UnsafeBytesOutput(SIZE);
        long position = output.skip(4);
        Assert.assertEquals(0, position);
        output.writeInt(Integer.MAX_VALUE);
        UnsafeBytesInput input = new UnsafeBytesInput(output.toByteArray());
        int bytesSkipped = input.skipBytes(4);
        Assert.assertEquals(4, bytesSkipped);
        Assert.assertEquals(4, input.remaining());
        Assert.assertEquals(Integer.MAX_VALUE, input.readInt());
        Assert.assertEquals(0, input.remaining());
        Assert.assertEquals(0, input.skipBytes(1));
    }

    @Test
    public void testSkip() throws IOException {
        UnsafeBytesOutput output = new UnsafeBytesOutput(SIZE);
        output.skip(4);
        output.writeInt(Integer.MAX_VALUE);
        UnsafeBytesInput input = new UnsafeBytesInput(output.toByteArray());
        input.skipBytes(4);
        Assert.assertEquals(4, input.remaining());
        Assert.assertEquals(Integer.MAX_VALUE, input.readInt());
        Assert.assertEquals(0, input.remaining());
        Assert.assertEquals(0, input.skipBytes(1));
    }

    @Test
    public void testBuffer() throws IOException {
        UnsafeBytesOutput output = new UnsafeBytesOutput(SIZE);
        output.writeInt(Integer.MAX_VALUE);
        UnsafeBytesInput input = new UnsafeBytesInput(output.buffer(),
                                                      (int) output.position());
        Assert.assertEquals(Integer.MAX_VALUE, input.readInt());
    }

    @Test
    public void testOverRead() throws IOException {
        UnsafeBytesOutput output = new UnsafeBytesOutput(SIZE);
        output.writeInt(Integer.MAX_VALUE);
        UnsafeBytesInput input = new UnsafeBytesInput(output.buffer(),
                                                      (int) output.position());
        Assert.assertEquals(Integer.MAX_VALUE, input.readInt());
        Assert.assertThrows(ComputerException.class, () -> {
            input.readInt();
        });
    }

    @Test
    public void testSeek() throws IOException {
        UnsafeBytesOutput output = new UnsafeBytesOutput(SIZE);
        for (int i = -128; i <= 127; i++) {
            output.writeInt(i);
        }
        // Overwrite last 2 elements
        output.seek(256 * 4 - 8);
        output.writeInt(Integer.MAX_VALUE);
        output.writeInt(Integer.MIN_VALUE);
        UnsafeBytesInput input = new UnsafeBytesInput(output.toByteArray());
        for (int i = -128; i <= 125; i++) {
            Assert.assertEquals(i, input.readInt());
        }
        Assert.assertEquals(Integer.MAX_VALUE, input.readInt());
        Assert.assertEquals(Integer.MIN_VALUE, input.readInt());
        input.seek(256 * 4 - 8);
        Assert.assertEquals(Integer.MAX_VALUE, input.readInt());
        Assert.assertEquals(Integer.MIN_VALUE, input.readInt());
    }

    @Test
    public void testAvailable() throws IOException {
        UnsafeBytesOutput output = new UnsafeBytesOutput(SIZE);
        for (int i = -128; i <= 127; i++) {
            output.write(i);
        }
        UnsafeBytesInput input = new UnsafeBytesInput(output.toByteArray());
        for (int i = 0; i < 256; i++) {
            Assert.assertEquals(256 - i, input.available());
            input.readByte();
        }
    }

    @Test
    public void testPosition() throws IOException {
        byte[] bytes;
        try (UnsafeBytesOutput bao = new UnsafeBytesOutput(SIZE)) {
            Assert.assertEquals(0L, bao.position());
            bao.writeLong(Long.MAX_VALUE);
            Assert.assertEquals(8L, bao.position());
            bytes = bao.toByteArray();
        }

        try (UnsafeBytesInput bai = new UnsafeBytesInput(bytes)) {
            Assert.assertEquals(0L, bai.position());
            Assert.assertEquals(Long.MAX_VALUE, bai.readLong());
            Assert.assertEquals(8L, bai.position());
        }
    }

    @Test
    public void testWriteByInput() throws IOException {
        // Input class is UnsafeBytesInput
        String uuid = UUID.randomUUID().toString();
        UnsafeBytesInput input = inputByString(uuid);
        UnsafeBytesOutput output = new UnsafeBytesOutput(SIZE);
        output.write(input, 0, input.available());
        Assert.assertEquals(uuid, new String(output.toByteArray()));

        // Input class isn't UnsafeBytesInput
        File tempFile = File.createTempFile(UUID.randomUUID().toString(), "");
        BufferedFileOutput fileOutput = null;
        BufferedFileInput fileInput = null;
        try {
            fileOutput = new BufferedFileOutput(tempFile);
            fileOutput.writeBytes(uuid);
            fileOutput.close();
            fileInput = new BufferedFileInput(tempFile);
            output = new UnsafeBytesOutput(SIZE);
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
    public void testReadBytes() throws IOException {
        String uuid = UUID.randomUUID().toString();
        UnsafeBytesOutput output = new UnsafeBytesOutput(SIZE);
        output.writeBytes(uuid);
        UnsafeBytesInput input = new UnsafeBytesInput(output.toByteArray());
        byte[] bytes = input.readBytes(uuid.length());
        Assert.assertEquals(uuid, new String(bytes));
    }

    @Test
    public void testCompare() throws IOException {
        UnsafeBytesInput apple = inputByString("apple");
        UnsafeBytesInput egg = inputByString("egg");
        Assert.assertTrue(apple.compare(0, 2, egg, 0, 2) < 0);
        Assert.assertTrue(apple.compare(1, 3, egg, 0, 2) > 0);
        Assert.assertEquals(0, apple.compare(4, 1, egg, 0, 1));
    }

    @Test
    public void testDuplicate() throws IOException {
        UnsafeBytesInput raw = inputByString("apple");
        UnsafeBytesInput dup = raw.duplicate();
        raw.readByte();
        Assert.assertEquals(1, raw.position());
        Assert.assertEquals(0, dup.position());

        String uuid = UUID.randomUUID().toString();
        File tempFile = File.createTempFile(UUID.randomUUID().toString(), "");
        BufferedFileOutput fileOutput = null;
        BufferedFileInput fileInput = null;
        try {
            fileOutput = new BufferedFileOutput(tempFile);
            fileOutput.writeBytes(uuid);
            fileOutput.close();
            fileInput = new BufferedFileInput(tempFile);
            dup = fileInput.duplicate();

            fileInput.readChar();
            Assert.assertEquals(2, fileInput.position());
            Assert.assertEquals(0, dup.position());
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
    public void testWriteReadVInt() throws IOException {
        assertEqualAfterWriteAndReadVInt(0, new byte[]{0});
        assertEqualAfterWriteAndReadVInt(1, new byte[]{1});
        assertEqualAfterWriteAndReadVInt(127, new byte[]{(byte) 0x7f});
        assertEqualAfterWriteAndReadVInt(128, new byte[]{(byte) 0x81, 0});
        assertEqualAfterWriteAndReadVInt(16383,
                                         new byte[]{(byte) 0xff, (byte) 0x7f});
        assertEqualAfterWriteAndReadVInt(16384,
                                         new byte[]{(byte) 0x81, (byte) 0x80,
                                                    0}
        );
        assertEqualAfterWriteAndReadVInt(16385,
                                         new byte[]{(byte) 0x81, (byte) 0x80,
                                                    1});
        assertEqualAfterWriteAndReadVInt(-1, new byte[]{-113, -1, -1, -1, 127});
        assertEqualAfterWriteAndReadVInt(Integer.MAX_VALUE,
                                         new byte[]{-121, -1, -1, -1, 127}
        );
        assertEqualAfterWriteAndReadVInt(Integer.MIN_VALUE,
                                         new byte[]{-120, -128, -128, -128, 0}
        );
    }

    @Test
    public void testWriteReadVLong() throws IOException {
        assertEqualAfterWriteAndReadVLong(0L, new byte[]{0});
        assertEqualAfterWriteAndReadVLong(1L, new byte[]{1});
        assertEqualAfterWriteAndReadVLong(127L, new byte[]{(byte) 0x7f});
        assertEqualAfterWriteAndReadVLong(128L, new byte[]{(byte) 0x81, 0});
        assertEqualAfterWriteAndReadVLong(16383L,
                                          new byte[]{(byte) 0xff, (byte) 0x7f});
        assertEqualAfterWriteAndReadVLong(16384L,
                                          new byte[]{(byte) 0x81, (byte) 0x80,
                                                     0}
        );
        assertEqualAfterWriteAndReadVLong(16385L,
                                          new byte[]{(byte) 0x81, (byte) 0x80,
                                                     1}
        );
        assertEqualAfterWriteAndReadVLong(-1L, new byte[]{-127, -1, -1, -1, -1,
                                                          -1, -1, -1, -1, 127});
        assertEqualAfterWriteAndReadVLong(Integer.MAX_VALUE,
                                          new byte[]{-121, -1, -1, -1, 127}
        );
        assertEqualAfterWriteAndReadVLong(Integer.MIN_VALUE,
                                          new byte[]{-127, -1, -1, -1, -1,
                                                     -8, -128, -128, -128, 0}
        );
        assertEqualAfterWriteAndReadVLong(Long.MAX_VALUE,
                                          new byte[]{-1, -1, -1, -1, -1,
                                                     -1, -1, -1, 127}
        );
        assertEqualAfterWriteAndReadVLong(Long.MIN_VALUE,
                                          new byte[]{-127, -128, -128, -128,
                                                     -128, -128, -128, -128,
                                                     -128, 0}
        );
    }

    @Test
    public void testWriteReadString() throws IOException {
        assertEqualAfterWriteAndReadString("", new byte[]{0});
        assertEqualAfterWriteAndReadString("1", new byte[]{1, 49});
        assertEqualAfterWriteAndReadString("789", new byte[]{3, 55, 56, 57});
        assertEqualAfterWriteAndReadString("ABCDE",
                                           new byte[]{5, 65, 66, 67, 68, 69});
    }

    public static void assertEqualAfterWriteAndReadVInt(int value, byte[] bytes)
                                                        throws IOException {
        try (OptimizedBytesOutput bao = new OptimizedBytesOutput(5)) {
            bao.writeInt(value);
            Assert.assertArrayEquals(bytes, bao.toByteArray());
        }

        try (OptimizedBytesInput bai = new OptimizedBytesInput(bytes)) {
            int readValue = bai.readInt();
            Assert.assertEquals(value, readValue);
        }
    }

    public static void assertEqualAfterWriteAndReadVLong(long value,
                                                         byte[] bytes)
                                                         throws IOException {
        try (OptimizedBytesOutput bao = new OptimizedBytesOutput(9)) {
            bao.writeLong(value);
            Assert.assertArrayEquals(bytes, bao.toByteArray());
        }

        try (OptimizedBytesInput bai = new OptimizedBytesInput(bytes)) {
            long readValue = bai.readLong();
            Assert.assertEquals(value, readValue);
        }
    }

    public static void assertEqualAfterWriteAndReadString(String value,
                                                          byte[] bytes)
                                                          throws IOException {
        try (OptimizedBytesOutput bao = new OptimizedBytesOutput(SIZE)) {
            bao.writeUTF(value);
            Assert.assertArrayEquals(bytes, bao.toByteArray());
        }

        try (OptimizedBytesInput bai = new OptimizedBytesInput(bytes)) {
            String readValue = bai.readUTF();
            Assert.assertEquals(value, readValue);
        }
    }

    private static UnsafeBytesInput inputByString(String s) throws IOException {
        UnsafeBytesOutput output = new UnsafeBytesOutput(SIZE);
        output.writeBytes(s);
        return new UnsafeBytesInput(output.toByteArray());
    }
}
