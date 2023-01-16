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

import java.io.IOException;
import java.io.UTFDataFormatException;
import java.lang.reflect.Field;
import java.util.Arrays;

import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.util.CoderUtil;
import org.apache.hugegraph.util.E;

import sun.misc.Unsafe;

/**
 * Use unsafe method to write the value to the buffer to improve the write
 * performance. The buffer is auto extendable.
 */
public class UnsafeBytesOutput implements BytesOutput {

    private static final sun.misc.Unsafe UNSAFE;

    private byte[] buffer;
    private int position;

    static {
        try {
            Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            UNSAFE = (sun.misc.Unsafe) field.get(null);
        } catch (Exception e) {
            throw new ComputerException("Failed to get unsafe", e);
        }
    }

    public UnsafeBytesOutput(int size) {
        this.buffer = new byte[size];
        this.position = 0;
    }

    @Override
    public void write(int b) throws IOException {
        this.require(Constants.BYTE_LEN);
        this.buffer[this.position] = (byte) b;
        this.position += Constants.BYTE_LEN;
    }

    @Override
    public void write(byte[] b) throws IOException {
        this.require(b.length);
        System.arraycopy(b, 0, this.buffer, this.position, b.length);
        this.position += b.length;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        this.require(len);
        System.arraycopy(b, off, this.buffer, this.position, len);
        this.position += len;
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        this.require(Constants.BOOLEAN_LEN);
        UNSAFE.putBoolean(this.buffer, this.offset(), v);
        this.position += Constants.BOOLEAN_LEN;
    }

    @Override
    public void writeByte(int v) throws IOException {
        this.require(Constants.BYTE_LEN);
        this.buffer[this.position] = (byte) v;
        this.position += Constants.BYTE_LEN;
    }

    @Override
    public void writeShort(int v) throws IOException {
        this.require(Constants.SHORT_LEN);
        UNSAFE.putShort(this.buffer, this.offset(), (short) v);
        this.position += Constants.SHORT_LEN;
    }

    @Override
    public void writeChar(int v) throws IOException {
        this.require(Constants.CHAR_LEN);
        UNSAFE.putChar(this.buffer, this.offset(), (char) v);
        this.position += Constants.CHAR_LEN;
    }

    @Override
    public void writeInt(int v) throws IOException {
        this.require(Constants.INT_LEN);
        UNSAFE.putInt(this.buffer, this.offset(), v);
        this.position += Constants.INT_LEN;
    }

    @Override
    public void writeLong(long v) throws IOException {
        this.require(Constants.LONG_LEN);
        UNSAFE.putLong(this.buffer, this.offset(), v);
        this.position += Constants.LONG_LEN;
    }

    @Override
    public void writeFloat(float v) throws IOException {
        this.require(Constants.FLOAT_LEN);
        UNSAFE.putFloat(this.buffer, this.offset(), v);
        this.position += Constants.FLOAT_LEN;
    }

    @Override
    public void writeDouble(double v) throws IOException {
        this.require(Constants.DOUBLE_LEN);
        UNSAFE.putDouble(this.buffer, this.offset(), v);
        this.position += Constants.DOUBLE_LEN;
    }

    @Override
    public void writeBytes(String s) throws IOException {
        int len = s.length();
        this.require(len);
        for (int i = 0; i < len; i++) {
            this.buffer[this.position] = (byte) s.charAt(i);
            this.position++;
        }
    }

    @Override
    public void writeChars(String s) throws IOException {
        int len = s.length();
        this.require(len * Constants.CHAR_LEN);
        for (int i = 0; i < len; i++) {
            char v = s.charAt(i);
            UNSAFE.putChar(this.buffer, this.offset(), v);
            this.position += Constants.CHAR_LEN;
        }
    }

    @Override
    public void writeUTF(String s) throws IOException {
        byte[] bytes = CoderUtil.encode(s);
        if (bytes.length > 65535) {
            throw new UTFDataFormatException(
                      "Encoded string too long: " + bytes.length + " bytes");
        }
        this.writeShort(bytes.length);
        this.write(bytes);
    }

    @Override
    public long position() {
        return this.position;
    }

    @Override
    public void seek(long position) throws IOException {
        this.position = (int) position;
    }

    /**
     * If you write some thing, and you need to write the serialized byte size
     * first. If the byte size is unknown when write, you can skip 4 bytes
     * （size of int）first, after write out the content, get the serialized
     * byte size and calls writeInt(int position, int v) to write the int at
     * skipped position. The serialized byte size can be get by
     * the difference of {@link #position()} before and after write the content.
     * @return the position before skip.
     */
    @Override
    public long skip(long bytesToSkip) throws IOException {
        E.checkArgument(bytesToSkip >= 0L,
                        "The parameter bytesToSkip must be >= 0, but got %s",
                        bytesToSkip);
        this.require((int) bytesToSkip);
        long positionBeforeSkip = this.position;
        this.position += bytesToSkip;
        return positionBeforeSkip;
    }

    @Override
    public void write(RandomAccessInput input, long offset, long length)
                      throws IOException {
        if (UnsafeBytesInput.class == input.getClass()) {
            byte[] buffer = ((UnsafeBytesInput) input).buffer();
            this.write(buffer, (int) offset, (int) length);
        } else {
            input.seek(offset);
            byte[] bytes = input.readBytes((int) length);
            this.write(bytes);
        }
    }

    @Override
    public void writeFixedInt(int v) throws IOException {
        this.require(Constants.INT_LEN);
        UNSAFE.putInt(this.buffer, this.offset(), v);
        this.position += Constants.INT_LEN;
    }

    @Override
    public void writeFixedInt(long position, int v) throws IOException {
        // The position is not changed after write
        this.require(position, Constants.INT_LEN);
        UNSAFE.putInt(this.buffer, this.offset(position), v);
    }

    /**
     * @return the internal byte array, can't modify the returned byte array
     */
    @Override
    public byte[] buffer() {
        return this.buffer;
    }

    /**
     * @return Copied byte array.
     */
    public byte[] toByteArray() {
        return Arrays.copyOf(this.buffer, this.position);
    }

    protected void require(int size) throws IOException {
        if (this.position + size > this.buffer.length) {
            byte[] newBuf = new byte[(this.buffer.length + size) << 1];
            System.arraycopy(this.buffer, 0, newBuf, 0, this.position);
            this.buffer = newBuf;
        }
    }

    protected void require(long position, int size) throws IOException {
        if (position + size > this.buffer.length) {
            throw new ComputerException(
                      "Unable to write %s bytes at position %s",
                      size, position);
        }
    }

    private long offset() {
        return Unsafe.ARRAY_BYTE_BASE_OFFSET + this.position;
    }

    private long offset(long position) {
        return Unsafe.ARRAY_BYTE_BASE_OFFSET + position;
    }

    @Override
    public void close() throws IOException {
        // pass
    }
}
