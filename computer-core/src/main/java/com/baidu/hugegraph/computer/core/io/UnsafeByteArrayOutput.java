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

import java.io.DataOutput;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.lang.reflect.Field;
import java.util.Arrays;

import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.util.CoderUtil;

import sun.misc.Unsafe;

public final class UnsafeByteArrayOutput implements DataOutput {

    private static final sun.misc.Unsafe UNSAFE;
    private static final int DEFAULT_SIZE = 32;

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

    public UnsafeByteArrayOutput() {
        this(DEFAULT_SIZE);
    }

    public UnsafeByteArrayOutput(int size) {
        this.buffer = new byte[size];
        this.position = 0;
    }

    @Override
    public void write(int b) {
        this.require(Constants.BYTE_LEN);
        this.buffer[this.position] = (byte) b;
        this.position += Constants.BYTE_LEN;
    }

    @Override
    public void write(byte[] b) {
        this.require(b.length);
        System.arraycopy(b, 0, this.buffer, this.position, b.length);
        this.position += b.length;
    }

    @Override
    public void write(byte[] b, int off, int len) {
        this.require(len);
        System.arraycopy(b, off, this.buffer, this.position, len);
        this.position += len;
    }

    @Override
    public void writeBoolean(boolean v) {
        this.require(Constants.BOOLEAN_LEN);
        UNSAFE.putBoolean(this.buffer, this.offset(), v);
        this.position += Constants.BOOLEAN_LEN;
    }

    @Override
    public void writeByte(int v) {
        this.require(Constants.BYTE_LEN);
        this.buffer[this.position] = (byte) v;
        this.position += Constants.BYTE_LEN;
    }

    @Override
    public void writeShort(int v) {
        this.require(Constants.SHORT_LEN);
        UNSAFE.putShort(this.buffer, this.offset(), (short) v);
        this.position += Constants.SHORT_LEN;
    }

    public void writeShort(int position, int v) {
        UNSAFE.putShort(this.buffer, this.offset(position), (short) v);
    }

    @Override
    public void writeChar(int v) {
        this.require(Constants.CHAR_LEN);
        UNSAFE.putChar(this.buffer, this.offset(), (char) v);
        this.position += Constants.CHAR_LEN;
    }

    @Override
    public void writeInt(int v) {
        this.require(Constants.INT_LEN);
        UNSAFE.putInt(this.buffer, this.offset(), v);
        this.position += Constants.INT_LEN;
    }

    public void writeInt(int position, int v) {
        this.require(Constants.INT_LEN);
        UNSAFE.putInt(this.buffer, this.offset(position), v);
    }

    @Override
    public void writeLong(long v) {
        this.require(Constants.LONG_LEN);
        UNSAFE.putLong(this.buffer, this.offset(), v);
        this.position += Constants.LONG_LEN;
    }

    @Override
    public void writeFloat(float v) {
        this.require(Constants.FLOAT_LEN);
        UNSAFE.putFloat(this.buffer, this.offset(), v);
        this.position += Constants.FLOAT_LEN;
    }

    @Override
    public void writeDouble(double v) {
        this.require(Constants.DOUBLE_LEN);
        UNSAFE.putDouble(this.buffer, this.offset(), v);
        this.position += Constants.DOUBLE_LEN;
    }

    @Override
    public void writeBytes(String s) {
        int len = s.length();
        this.require(len);
        for (int i = 0; i < len; i++) {
            this.buffer[this.position] = (byte) s.charAt(i);
            this.position++;
        }
    }

    @Override
    public void writeChars(String s) {
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
        this.require(bytes.length);
        System.arraycopy(bytes, 0, this.buffer, this.position, bytes.length);
        this.position += bytes.length;
    }

    public int position() {
        return this.position;
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
    public int skipBytes(int bytesToSkip) {
        this.require(bytesToSkip);
        int positionBeforeSkip = this.position;
        this.position += bytesToSkip;
        return positionBeforeSkip;
    }

    /**
     * @return the internal byte array, can't modify the returned byte array
     */
    public byte[] buffer() {
        return this.buffer;
    }

    /**
     * @return Copied byte array.
     */
    public byte[] toByteArray() {
        return Arrays.copyOf(this.buffer, this.position);
    }

    private void require(int size) {
        if (this.position + size > this.buffer.length) {
            byte[] newBuf = new byte[(this.buffer.length + size) << 1];
            System.arraycopy(this.buffer, 0, newBuf, 0, this.position);
            this.buffer = newBuf;
        }
    }

    private int offset() {
        return Unsafe.ARRAY_BYTE_BASE_OFFSET + this.position;
    }

    private int offset(int position) {
        return Unsafe.ARRAY_BYTE_BASE_OFFSET + position;
    }
}
