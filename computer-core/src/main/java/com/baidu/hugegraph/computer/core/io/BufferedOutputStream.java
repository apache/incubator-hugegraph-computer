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
import java.io.OutputStream;

import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.util.E;

/**
 * This is used to buffer and output the buffer to output stream when buffer
 * is full.
 */
public class BufferedOutputStream extends UnsafeByteArrayOutput {

    private final int bufferSize;
    private final OutputStream out;
    private long osOffset;

    public BufferedOutputStream(OutputStream out) {
        this(out, Constants.DEFAULT_BUFFER_SIZE);
    }

    public BufferedOutputStream(OutputStream out, int bufferSize) {
        super(bufferSize);
        E.checkArgument(bufferSize >= 8,
                        "The parameter bufferSize must be >= 8");
        this.bufferSize = bufferSize;
        this.out = out;
        this.osOffset = 0L;
    }

    @Override
    public void write(byte[] b) throws IOException {
        this.write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (this.bufferAvailable() >= len) {
            super.write(b, off, len);
            return;
        }
        this.writeBuffer();
        if (this.bufferSize >= len) {
            super.write(b, off, len);
        } else {
            // The len is bigger than the buffer size, write out directly
            this.out.write(b, off, len);
            this.osOffset += len;
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
        return this.osOffset + super.position();
    }

    @Override
    public void seek(long position) throws IOException {
        if (this.osOffset <= position &&
            position < this.osOffset + this.bufferSize) {
            super.seek(position - this.osOffset);
            return;
        }
        if (position >= this.osOffset + this.bufferSize) {
            this.skip(position - this.position());
        } else {
                throw new IOException("The position " + position + " is out " +
                                      "of range [" + this.osOffset + ", " +
                                      (this.osOffset + this.bufferSize) + ")");
        }
    }

    @Override
    public long skip(long size) throws IOException {
        E.checkArgument(size <= Integer.MAX_VALUE,
                        "The parameter bytesToSkip must be <= " +
                        "Integer.MAX_VALUE");
        long positionBeforeSkip = this.osOffset + super.position();
        long bufferPosition = super.position();
        long bufferAvailable = this.bufferSize - bufferPosition;
        if (bufferAvailable >= size) {
            super.skip(size);
            return positionBeforeSkip;
        }

        this.writeBuffer();
        if (size <= this.bufferSize) {
            super.skip(size);
        } else {
            this.osOffset += size;
            byte[] buffer = super.buffer();
            int writeSize = (int) size;
            while (writeSize > 0) {
                int len = Math.min(buffer.length, writeSize);
                this.out.write(buffer, 0, len);
                writeSize -= len;
            }
        }
        return positionBeforeSkip;
    }

    @Override
    protected void require(int size) throws IOException {
        E.checkArgument(size <= this.bufferSize, "size must be <=8");
        long position = super.position();
        long bufferAvailable = this.bufferSize - position;
        if (bufferAvailable >= size) {
            return;
        }
        this.writeBuffer();
    }

    private void writeBuffer() throws IOException {
        int bufferPosition = (int) super.position();
        if (bufferPosition == 0) {
            return;
        }
        this.out.write(this.buffer(), 0, bufferPosition);
        this.osOffset += bufferPosition;
        super.seek(0);
    }

    public void close() throws IOException {
        this.writeBuffer();
        this.out.close();
    }

    private int bufferAvailable() {
        return this.buffer().length - (int) super.position();
    }
}
