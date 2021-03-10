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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.util.E;

/**
 * This class acted as new DataOutputStream(new BufferedOutputStream(new File
 * (file))). It has two functions. The first is buffer the content until the
 * buffer is full. The second is unsafe data output.
 * This class is not thread safe.
 */
public class BufferedFileOutput extends UnsafeByteArrayOutput {

    private final int bufferCapacity;
    private final RandomAccessFile file;
    private long fileOffset;

    public BufferedFileOutput(File file) throws FileNotFoundException {
        this(new RandomAccessFile(file, Constants.FILE_MODE_WRITE),
             Constants.DEFAULT_BUFFER_SIZE);
    }

    public BufferedFileOutput(RandomAccessFile file, int bufferCapacity) {
        super(bufferCapacity);
        E.checkArgument(bufferCapacity >= 8,
                        "The parameter bufferSize must be >= 8");
        this.bufferCapacity = bufferCapacity;
        this.file = file;
        this.fileOffset = 0L;
    }

    @Override
    public void write(byte[] b) throws IOException {
        this.write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (len <= this.bufferAvailable()) {
            super.write(b, off, len);
            return;
        }
        this.flushBuffer();
        if (len <= this.bufferCapacity) {
            super.write(b, off, len);
        } else {
            // The len > buffer size, write out directly
            this.file.write(b, off, len);
            this.fileOffset += len;
        }
    }

    @Override
    public void writeInt(long position, int v) throws IOException {
        long latestPosition = this.position();
        this.seek(position);
        super.writeInt(v);
        this.seek(latestPosition);
    }

    @Override
    public long position() {
        return this.fileOffset + super.position();
    }

    @Override
    public void seek(long position) throws IOException {
        if (this.fileOffset <= position &&
            position <= this.fileOffset + this.bufferCapacity) {
            super.seek(position - this.fileOffset);
            return;
        }
        this.flushBuffer();
        this.file.seek(position);
        this.fileOffset = position;
    }

    @Override
    public long skip(long bytesToSkip) throws IOException {
        E.checkArgument(bytesToSkip >= 0,
                        "The parameter bytesToSkip must be >= 0, but got %s",
                        bytesToSkip);
        long positionBeforeSkip = this.fileOffset + super.position();
        if (bytesToSkip <= this.bufferAvailable()) {
            super.skip(bytesToSkip);
            return positionBeforeSkip;
        }

        this.flushBuffer();
        if (bytesToSkip <= this.bufferCapacity) {
            super.skip(bytesToSkip);
        } else {
            this.fileOffset += bytesToSkip;
            this.file.seek(this.fileOffset);
        }
        return positionBeforeSkip;
    }

    @Override
    protected void require(int size) throws IOException {
        E.checkArgument(size <= this.bufferCapacity,
                        "The parameter size must be <= %s",
                        this.bufferCapacity);
        if (size <= this.bufferAvailable()) {
            return;
        }
        this.flushBuffer();
        /*
         * The buffer capacity must be >= 8, write primitive data like int,
         * long, float, double can be write to buffer after flush buffer.
         * Only write bytes may exceed the limit, and write bytes using
         * write(byte[] b) is overrode in this class. In conclusion, the
         * required size can be supplied after flushBuffer.
         */
        if (size > this.bufferAvailable()) {
            throw new IOException(String.format(
                      "Write %s bytes to position %s overflows buffer %s",
                      size, this.position(), this.bufferCapacity));
        }
    }

    private void flushBuffer() throws IOException {
        int bufferPosition = (int) super.position();
        if (bufferPosition == 0) {
            return;
        }
        this.file.write(this.buffer(), 0, bufferPosition);
        this.fileOffset += bufferPosition;
        super.seek(0);
    }

    public void close() throws IOException {
        this.flushBuffer();
        this.file.close();
    }

    private final int bufferAvailable() {
        return this.bufferCapacity - (int) super.position();
    }
}
