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

import java.io.Closeable;
import java.io.DataInput;
import java.io.IOException;
import java.lang.reflect.Field;

import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.util.CoderUtil;
import com.baidu.hugegraph.util.E;

import sun.misc.Unsafe;

public final class UnsafeByteArrayInput implements DataInput, Closeable {

    private static final sun.misc.Unsafe UNSAFE;

    private final byte[] buffer;
    private final int limit;
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

    public UnsafeByteArrayInput(byte[] buffer) {
        this(buffer, buffer.length);
    }

    public UnsafeByteArrayInput(byte[] buffer, int limit) {
        this(buffer, 0, limit);
    }

    public UnsafeByteArrayInput(byte[] buffer, int position, int limit) {
        E.checkArgumentNotNull(buffer, "The buffer can't be null");
        this.buffer = buffer;
        this.limit = limit;
        this.position = position;
    }

    @Override
    public void readFully(byte[] b) {
        this.readFully(b, 0, b.length);
    }

    @Override
    public void readFully(byte[] b, int off, int len) {
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
    public boolean readBoolean() {
        this.require(Constants.BOOLEAN_LEN);
        boolean value = UNSAFE.getBoolean(this.buffer, this.offset());
        this.position += Constants.BOOLEAN_LEN;
        return value;
    }

    @Override
    public byte readByte() {
        this.require(Constants.BYTE_LEN);
        byte value = this.buffer[position];
        this.position += Constants.BYTE_LEN;
        return value;
    }

    @Override
    public int readUnsignedByte() {
        this.require(Constants.BYTE_LEN);
        int value = this.buffer[position] & 0xFF;
        this.position += Constants.BYTE_LEN;
        return value;
    }

    @Override
    public short readShort() {
        this.require(Constants.SHORT_LEN);
        short value = UNSAFE.getShort(this.buffer, this.offset());
        this.position += Constants.SHORT_LEN;
        return value;
    }

    @Override
    public int readUnsignedShort() {
        this.require(Constants.SHORT_LEN);
        int value = UNSAFE.getShort(this.buffer, this.offset()) & 0xFFFF;
        this.position += Constants.SHORT_LEN;
        return value;
    }

    @Override
    public char readChar() {
        this.require(Constants.CHAR_LEN);
        char value = UNSAFE.getChar(this.buffer, this.offset());
        this.position += Constants.CHAR_LEN;
        return value;
    }

    @Override
    public int readInt() {
        this.require(Constants.INT_LEN);
        int value = UNSAFE.getInt(this.buffer, this.offset());
        this.position += Constants.INT_LEN;
        return value;
    }

    @Override
    public long readLong() {
        this.require(Constants.LONG_LEN);
        long value = UNSAFE.getLong(this.buffer, this.offset());
        this.position += Constants.LONG_LEN;
        return value;
    }

    @Override
    public float readFloat() {
        this.require(Constants.FLOAT_LEN);
        float value = UNSAFE.getFloat(this.buffer, this.offset());
        this.position += Constants.FLOAT_LEN;
        return value;
    }

    @Override
    public double readDouble() {
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
    public String readUTF() {
        int len = this.readUnsignedShort();
        byte[] bytes = new byte[len];
        this.readFully(bytes, 0, len);
        return CoderUtil.decode(bytes);
    }

    public int position() {
        return this.position;
    }

    public int remaining() {
        return this.limit - this.position;
    }

    private void require(int size) {
        if (this.position + size > this.limit) {
            throw new ComputerException(
                      "Only %s bytes available, trying to read %s bytes",
                      this.limit - this.position, size);
        }
    }

    private int offset() {
        return Unsafe.ARRAY_BYTE_BASE_OFFSET + this.position;
    }

    @Override
    public void close() throws IOException {
        // Do nothing
    }
}
