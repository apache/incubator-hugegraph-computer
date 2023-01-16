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
import java.io.OutputStream;

import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.util.E;

/**
 * This is used to buffer and output the buffer to output stream when buffer
 * is full.
 */
public class BufferedStreamOutput extends UnsafeBytesOutput {

    private final int bufferCapacity;
    private final OutputStream output;
    private long outputOffset;

    public BufferedStreamOutput(OutputStream output) {
        this(output, Constants.BIG_BUF_SIZE);
    }

    public BufferedStreamOutput(OutputStream output, int bufferCapacity) {
        super(bufferCapacity);
        E.checkArgument(bufferCapacity >= 8,
                        "The parameter bufferSize must be >= 8");
        this.bufferCapacity = bufferCapacity;
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
        if (this.bufferCapacity >= len) {
            super.write(b, off, len);
        } else {
            // The len > the buffer size, write out directly
            this.output.write(b, off, len);
            this.outputOffset += len;
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
                      this.position()));
        }
    }

    @Override
    public long skip(long bytesToSkip) throws IOException {
        E.checkArgument(bytesToSkip >= 0L,
                        "The parameter bytesToSkip must be >= 0, but got %s",
                        bytesToSkip);
        long positionBeforeSkip = this.outputOffset + super.position();
        if (this.bufferAvailable() >= bytesToSkip) {
            super.skip(bytesToSkip);
            return positionBeforeSkip;
        }

        this.flushBuffer();
        if (bytesToSkip <= this.bufferCapacity) {
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

    /**
     * The valid range of position is [the output position correspond to buffer
     * start, the output position correspond to
     * the current position - Constants.INT_LEN], it can't write data to the
     * position before the buffer or after
     * the current position.
     */
    @Override
    public void writeFixedInt(long position, int v) throws IOException {
        if (position >= this.outputOffset &&
            position <= this.position() - Constants.INT_LEN) {
            super.writeFixedInt(position - this.outputOffset, v);
        } else if (position < this.outputOffset) {
            throw new IOException(String.format(
                      "Write int to position %s underflows the " +
                      "start position %s of the buffer",
                      position, this.outputOffset));
        } else {
            throw new IOException(String.format(
                      "Write int to position %s overflows the write " +
                      "position %s", position, this.position()));
        }
    }

    @Override
    protected void require(int size) throws IOException {
        E.checkArgument(size <= this.bufferCapacity,
                        "The parameter size must be <= %s",
                        this.bufferCapacity);
        if (this.bufferAvailable() >= size) {
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

    @Override
    public void close() throws IOException {
        this.flushBuffer();
        this.output.close();
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

    private final int bufferAvailable() {
        return this.bufferCapacity - (int) super.position();
    }
}
