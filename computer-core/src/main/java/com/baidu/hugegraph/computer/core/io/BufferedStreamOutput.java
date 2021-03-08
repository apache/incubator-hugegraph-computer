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
public class BufferedStreamOutput extends UnsafeByteArrayOutput {

    private final int bufferSize;
    private final OutputStream output;
    private long outputOffset;

    public BufferedStreamOutput(OutputStream output) {
        this(output, Constants.DEFAULT_BUFFER_SIZE);
    }

    public BufferedStreamOutput(OutputStream output, int bufferSize) {
        super(bufferSize);
        E.checkArgument(bufferSize >= 8,
                        "The parameter bufferSize must be >= 8");
        this.bufferSize = bufferSize;
        this.output = output;
        this.outputOffset = 0L;
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
        this.flushBuffer();
        if (this.bufferSize >= len) {
            super.write(b, off, len);
        } else {
            // The len > the buffer size, write out directly
            this.output.write(b, off, len);
            this.outputOffset += len;
        }
    }

    /**
     * The valid range of position is [the output position correspond to buffer
     * start, the output position correspond to the current position - 4], it
     * can't write data to the position before the buffer or after
     * the current position.
     */
    @Override
    public void writeInt(long position, int v) throws IOException {
        if (this.outputOffset <= position &&
            position <= this.position() - 4) {
            super.writeInt(position - this.outputOffset, v);
        } else {
            throw new IOException(String.format(
                      "The position %s is out of range [%s, %s]",
                      position, this.outputOffset,
                      this.position() - 4));
        }
    }

    @Override
    public long position() {
        return this.outputOffset + super.position();
    }

    /**
     * The valid range of position is [the output position correspond to buffer
     * start, the output position correspond to the current position], it
     * can't seek to the position before the buffer or after
     * the current position.
     */
    @Override
    public void seek(long position) throws IOException {
        /*
         * It can seek the end of buffer. If the buffer size is 128, you can
         * seek the position 128, the buffer will be write out while write data.
         */
        if (this.outputOffset <= position &&
            position <= this.position()) {
            super.seek(position - this.outputOffset);
        } else {
            throw new IOException(String.format(
                      "The position %s is out of range [%s, %s]",
                      position, this.outputOffset,
                      this.outputOffset + this.bufferSize));
        }
    }

    @Override
    public long skip(long bytesToSkip) throws IOException {
        E.checkArgument(bytesToSkip >= 0,
                        "The parameter bytesToSkip must be >=0, but got %s",
                        bytesToSkip);
        long positionBeforeSkip = this.outputOffset + super.position();
        long bufferPosition = super.position();
        long bufferAvailable = this.bufferSize - bufferPosition;
        if (bufferAvailable >= bytesToSkip) {
            super.skip(bytesToSkip);
            return positionBeforeSkip;
        }

        this.flushBuffer();
        if (bytesToSkip <= this.bufferSize) {
            super.skip(bytesToSkip);
        } else {
            this.outputOffset += bytesToSkip;
            byte[] buffer = super.buffer();
            int writeSize = (int) bytesToSkip;
            while (writeSize > 0) {
                int len = Math.min(buffer.length, writeSize);
                this.output.write(buffer, 0, len);
                writeSize -= len;
            }
        }
        return positionBeforeSkip;
    }

    @Override
    protected void require(int size) throws IOException {
        E.checkArgument(size <= this.bufferSize,
                        "The parameter size must be <= %s",
                        this.bufferSize);
        long position = super.position();
        long bufferAvailable = this.bufferSize - position;
        if (bufferAvailable >= size) {
            return;
        }
        this.flushBuffer();
    }

    private void flushBuffer() throws IOException {
        int bufferPosition = (int) super.position();
        if (bufferPosition == 0) {
            return;
        }
        this.output.write(this.buffer(), 0, bufferPosition);
        this.outputOffset += bufferPosition;
        super.seek(0);
    }

    public void close() throws IOException {
        this.flushBuffer();
        this.output.close();
    }

    private final int bufferAvailable() {
        return this.bufferSize - (int) super.position();
    }
}
