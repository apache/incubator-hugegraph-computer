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

import java.io.IOException;
import java.io.UTFDataFormatException;
import java.lang.reflect.Field;
import java.util.Arrays;

import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.value.Value;

import sun.misc.Unsafe;

public class UnsafeByteArrayGraphOutput implements GraphOutput {

    private static final sun.misc.Unsafe UNSAFE;
    private static final int DEFAULT_SIZE = 32;

    private byte[] buffer;
    private int capacity;
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

    public UnsafeByteArrayGraphOutput() {
        this(DEFAULT_SIZE);
    }

    public UnsafeByteArrayGraphOutput(int size) {
        this.buffer = new byte[size];
        this.capacity = size;
        this.position = 0;
    }

    @Override
    public void writeId(Id id) throws IOException {
        // For code only
        this.require(1);
        this.writeByte(id.type().code());
        id.write(this);
    }

    @Override
    public void writeValue(Value value) throws IOException {
        // TODO: doesn't need write type, fetch value type from config
        // For code only
        this.require(1);
        this.writeByte(value.type().code());
        value.write(this);
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
    @SuppressWarnings("deprecated")
    public void writeBoolean(boolean v) {
        this.require(Constants.BOOLEAN_LEN);
        UNSAFE.putBoolean(buffer,
                          Unsafe.ARRAY_BYTE_BASE_OFFSET + this.position, v);
        this.position += Constants.BOOLEAN_LEN;
    }

    @Override
    public void writeByte(int v) {
        this.require(Constants.BYTE_LEN);
        this.buffer[position] = (byte) v;
        this.position += Constants.BYTE_LEN;
    }

    @Override
    @SuppressWarnings("deprecated")
    public void writeShort(int v) {
        this.require(Constants.SHORT_LEN);
        UNSAFE.putShort(buffer,
                          Unsafe.ARRAY_BYTE_BASE_OFFSET + this.position,
                          (short) v);
        this.position += Constants.SHORT_LEN;
    }

    @SuppressWarnings("deprecated")
    public void writeShort(int position, int v) {
        UNSAFE.putShort(buffer,
                        Unsafe.ARRAY_BYTE_BASE_OFFSET + position, (short) v);
    }

    @Override
    @SuppressWarnings("deprecated")
    public void writeChar(int v) {
        this.require(Constants.CHAR_LEN);
        UNSAFE.putChar(buffer,
                        Unsafe.ARRAY_BYTE_BASE_OFFSET + this.position,
                        (char) v);
        this.position += Constants.CHAR_LEN;
    }

    @Override
    @SuppressWarnings("deprecated")
    public void writeInt(int v) {
        this.require(Constants.INT_LEN);
        UNSAFE.putInt(buffer,
                      Unsafe.ARRAY_BYTE_BASE_OFFSET + this.position, v);
        this.position += Constants.INT_LEN;
    }

    @SuppressWarnings("deprecated")
    public void writeInt(int position, int v) {
        this.require(Constants.INT_LEN);
        UNSAFE.putInt(buffer,
                      Unsafe.ARRAY_BYTE_BASE_OFFSET + position, v);
    }

    @Override
    @SuppressWarnings("deprecated")
    public void writeLong(long v) {
        this.require(Constants.LONG_LEN);
        UNSAFE.putLong(buffer,
                      Unsafe.ARRAY_BYTE_BASE_OFFSET + this.position, v);
        this.position += Constants.LONG_LEN;
    }

    @Override
    @SuppressWarnings("deprecated")
    public void writeFloat(float v) {
        this.require(Constants.FLOAT_LEN);
        UNSAFE.putFloat(buffer,
                        Unsafe.ARRAY_BYTE_BASE_OFFSET + this.position, v);
        this.position += Constants.FLOAT_LEN;
    }

    @Override
    @SuppressWarnings("deprecated")
    public void writeDouble(double v) {
        this.require(Constants.DOUBLE_LEN);
        UNSAFE.putDouble(buffer,
                         Unsafe.ARRAY_BYTE_BASE_OFFSET + this.position, v);
        this.position += Constants.DOUBLE_LEN;
    }

    @Override
    public void writeBytes(String s) {
        int len = s.length();
        this.require(len);
        for (int i = 0; i < len; i++) {
            this.buffer[position] = (byte) s.charAt(i);
            this.position++;
        }
    }

    @Override
    @SuppressWarnings("deprecated")
    public void writeChars(String s) {
        int len = s.length();
        this.require(len * Constants.CHAR_LEN);
        for (int i = 0; i < len; i++) {
            char v = s.charAt(i);
            UNSAFE.putChar(buffer, Unsafe.ARRAY_BYTE_BASE_OFFSET + position, v);
            position += Constants.CHAR_LEN;
        }
    }

    @Override
    public void writeUTF(String s) throws IOException {
        // Note that this code is mostly copied from DataOutputStream
        int strLen = s.length();
        int utfLen = 0;
        char c;
        int count = 0;

        /* use charAt instead of copying String to char array */
        for (int i = 0; i < strLen; i++) {
            c = s.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                utfLen++;
            } else if (c > 0x07FF) {
                utfLen += 3;
            } else {
                utfLen += 2;
            }
        }

        if (utfLen > 65535)
            throw new UTFDataFormatException(
                      "Encoded string too long: " + utfLen + " bytes");

        byte[] bytes = new byte[utfLen];

        int i;
        for (i = 0; i < strLen; i++) {
            c = s.charAt(i);
            if (!((c >= 0x0001) && (c <= 0x007F))) {
                break;
            } else {
                bytes[count++] = (byte) c;
            }
        }

        for (;i < strLen; i++) {
            c = s.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                bytes[count++] = (byte) c;
            } else if (c > 0x07FF) {
                bytes[count++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
                bytes[count++] = (byte) (0x80 | ((c >> 6) & 0x3F));
                bytes[count++] = (byte) (0x80 | (c & 0x3F));
            } else {
                bytes[count++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
                bytes[count++] = (byte) (0x80 | (c & 0x3F));
            }
        }
        this.writeShort(utfLen);
        this.require(utfLen);
        System.arraycopy(bytes, 0, this.buffer, this.position, bytes.length);
        this.position += bytes.length;
    }

    private void require(int size) {
        if (this.position + size > this.capacity) {
            byte[] newBuf = new byte[(this.buffer.length + size) << 1];
            System.arraycopy(this.buffer, 0, newBuf, 0, this.position);
            this.buffer = newBuf;
            this.capacity = this.buffer.length;
        }
    }

    public int position() {
        return this.position;
    }

    public void skipBytes(int bytesToSkip) {
        this.require(bytesToSkip);
        this.position +=  bytesToSkip;
    }

    /**
     * @return the internal byte array, can't modify the returned byte array
     */
    public byte[] getByteArray() {
        return this.buffer;
    }

    /**
     * @return Copied byte array.
     */
    public byte[] toByteArray() {
        return Arrays.copyOf(this.buffer, this.position);
    }
}
