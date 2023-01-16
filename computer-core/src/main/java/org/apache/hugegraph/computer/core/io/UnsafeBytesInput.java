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
import java.lang.reflect.Field;

import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.util.BytesUtil;
import org.apache.hugegraph.computer.core.util.CoderUtil;
import org.apache.hugegraph.util.E;

import sun.misc.Unsafe;

@SuppressWarnings("deprecation") // Unsafe.getXx
public class UnsafeBytesInput implements BytesInput {

    private static final sun.misc.Unsafe UNSAFE;

    private final byte[] buffer;
    private int limit;
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

    public UnsafeBytesInput(byte[] buffer) {
        this(buffer, buffer.length);
    }

    public UnsafeBytesInput(byte[] buffer, int limit) {
        this(buffer, 0, limit);
    }

    public UnsafeBytesInput(byte[] buffer, int position, int limit) {
        E.checkArgumentNotNull(buffer, "The buffer can't be null");
        this.buffer = buffer;
        this.limit = limit;
        this.position = position;
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        this.readFully(b, 0, b.length);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        this.require(len);
        System.arraycopy(this.buffer, this.position, b, off, len);
        this.position += len;
    }

    @Override
    public int skipBytes(int n) {
        int remaining = this.remaining();
        if (remaining >= n) {
            this.position += n;
            return n;
        } else {
            this.position += remaining;
            return remaining;
        }
    }

    @Override
    public boolean readBoolean() throws IOException {
        this.require(Constants.BOOLEAN_LEN);
        boolean value = UNSAFE.getBoolean(this.buffer, this.offset());
        this.position += Constants.BOOLEAN_LEN;
        return value;
    }

    @Override
    public byte readByte() throws IOException {
        this.require(Constants.BYTE_LEN);
        byte value = this.buffer[this.position];
        this.position += Constants.BYTE_LEN;
        return value;
    }

    @Override
    public int readUnsignedByte() throws IOException {
        this.require(Constants.BYTE_LEN);
        int value = this.buffer[this.position] & 0xFF;
        this.position += Constants.BYTE_LEN;
        return value;
    }

    @Override
    public short readShort() throws IOException {
        this.require(Constants.SHORT_LEN);
        short value = UNSAFE.getShort(this.buffer, this.offset());
        this.position += Constants.SHORT_LEN;
        return value;
    }

    @Override
    public int readUnsignedShort() throws IOException {
        this.require(Constants.SHORT_LEN);
        int value = UNSAFE.getShort(this.buffer, this.offset()) & 0xFFFF;
        this.position += Constants.SHORT_LEN;
        return value;
    }

    @Override
    public char readChar() throws IOException {
        this.require(Constants.CHAR_LEN);
        char value = UNSAFE.getChar(this.buffer, this.offset());
        this.position += Constants.CHAR_LEN;
        return value;
    }

    @Override
    public int readInt() throws IOException {
        this.require(Constants.INT_LEN);
        int value = UNSAFE.getInt(this.buffer, this.offset());
        this.position += Constants.INT_LEN;
        return value;
    }

    @Override
    public long readLong() throws IOException {
        this.require(Constants.LONG_LEN);
        long value = UNSAFE.getLong(this.buffer, this.offset());
        this.position += Constants.LONG_LEN;
        return value;
    }

    @Override
    public float readFloat() throws IOException {
        this.require(Constants.FLOAT_LEN);
        float value = UNSAFE.getFloat(this.buffer, this.offset());
        this.position += Constants.FLOAT_LEN;
        return value;
    }

    @Override
    public double readDouble() throws IOException {
        this.require(Constants.DOUBLE_LEN);
        double value = UNSAFE.getDouble(this.buffer, this.offset());
        this.position += Constants.DOUBLE_LEN;
        return value;
    }

    @Override
    public String readLine() {
        throw new ComputerException("Not implemented yet");
    }

    @Override
    public String readUTF() throws IOException {
        int len = this.readUnsignedShort();
        byte[] bytes = new byte[len];
        this.readFully(bytes, 0, len);
        return CoderUtil.decode(bytes);
    }

    @Override
    public long position() {
        return this.position;
    }

    @Override
    public void seek(long position) throws IOException {
        this.position = (int) position;
    }

    @Override
    public long skip(long bytesToSkip) throws IOException {
        int positionBeforeSkip = this.position;
        this.require((int) bytesToSkip);
        this.position += bytesToSkip;
        return positionBeforeSkip;
    }

    @Override
    public long available() throws IOException {
        return this.limit - this.position;
    }

    protected int remaining() {
        return this.limit - this.position;
    }

    @Override
    public void close() throws IOException {
        // pass
    }

    @Override
    public UnsafeBytesInput duplicate() throws IOException {
        return new UnsafeBytesInput(this.buffer, this.position, this.limit);
    }

    @Override
    public int compare(long offset, long length, RandomAccessInput other,
                       long otherOffset, long otherLength) throws IOException {
        E.checkArgument(offset < this.buffer.length,
                        "Invalid offset parameter %s, expect < %s",
                        offset, this.buffer.length);
        E.checkArgument(length <= (this.buffer.length - offset),
                        "Invalid length parameter %s, expect <= %s",
                        length, this.buffer.length - offset);

        if (other.getClass() == UnsafeBytesInput.class) {
            return BytesUtil.compare(this.buffer, (int) offset, (int) length,
                                     ((UnsafeBytesInput) other).buffer,
                                     (int) otherOffset, (int) otherLength);
        } else {
            long otherPosition = other.position();
            other.seek(otherOffset);
            byte[] bytes = other.readBytes((int) otherLength);
            other.seek(otherPosition);

            return BytesUtil.compare(this.buffer, (int) offset, (int) length,
                                     bytes, 0, bytes.length);
        }
    }

    protected void require(int size) throws IOException {
        if (this.position + size > this.limit) {
            throw new ComputerException(
                      "Only %s bytes available, trying to read %s bytes",
                      this.limit - this.position, size);
        }
    }

    protected byte[] buffer() {
        return this.buffer;
    }

    /**
     * Cut the content from 0 to position and copy the content from position
     * to the end to 0.
     */
    protected void shiftBuffer() {
        int remaining = this.remaining();
        if (remaining > 0) {
            System.arraycopy(this.buffer, this.position,
                             this.buffer, 0, remaining);
        }
        this.position = 0;
        this.limit = remaining;
    }

    protected void limit(int limit) {
        E.checkArgument(limit <= this.buffer.length,
                        "The limit must be >= buffer length %s",
                        this.buffer.length);
        this.limit = limit;
    }

    public int limit() {
        return this.limit;
    }

    private int offset() {
        return Unsafe.ARRAY_BYTE_BASE_OFFSET + this.position;
    }
}
