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
import java.io.InputStream;

import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.util.E;

public class BufferedStreamInput extends UnsafeByteArrayInput {

    private final int bufferSize;
    private final InputStream input;
    private long inputOffset;

    public BufferedStreamInput(InputStream input) throws IOException {
        this(input, Constants.DEFAULT_BUFFER_SIZE);
    }

    public BufferedStreamInput(InputStream input, int bufferSize)
                               throws IOException {
        super(new byte[bufferSize], 0);
        E.checkArgument(bufferSize >= 8,
                        "The parameter bufferSize must be >= 8");
        this.input = input;
        this.bufferSize = bufferSize;
        this.shiftAndFillBuffer();
    }

    @Override
    public long position() {
        return this.inputOffset - super.remaining();
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        this.readFully(b, 0, b.length);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        if (len <= super.remaining()) {
            super.readFully(b, off, len);
        } else if (len <= this.bufferSize) {
            this.shiftAndFillBuffer();
            super.readFully(b, off, len);
        } else {
            int remaining = super.remaining();
            super.readFully(b, off, remaining);
            int expectedLen = len - remaining;
            while (expectedLen > 0) {
                int readLen = this.input.read(b, off + remaining, expectedLen);
                if (readLen == -1) {
                    throw new IOException("There is no enough data in input " +
                                          "stream");
                }
                expectedLen -= readLen;
            }
            this.inputOffset += len;
        }
    }

    @Override
    public void seek(long position) throws IOException {
        if (position < this.inputOffset &&
            position >= this.inputOffset - this.bufferSize) {
            super.seek(this.bufferSize - (this.inputOffset - position));
            return;
        }
        /*
         * The reason for seeking beyond the current buffer location is that
         * the user may need to skip unread data and know the offset of the
         * required data.
         */
        if (position >= this.inputOffset) {
            int skipLen = (int) (position - this.inputOffset);
            this.inputOffset += skipLen;
            byte[] buffer = this.buffer();
            while (skipLen > 0) {
                int expectLen = Math.min(skipLen, this.bufferSize);
                int readLen = this.input.read(buffer, 0, expectLen);
                if (readLen == -1) {
                    throw new IOException(String.format(
                              "Can't seek at position %s, reach the end of " +
                              "input stream",
                              position));
                }
                skipLen -= expectLen;
            }
            super.seek(0);
            super.limit(0);
            this.fillBuffer();
        } else {
            throw new IOException(String.format(
                      "The seek position %s underflows the start position " +
                      "%s of the buffer",
                      position, this.inputOffset - this.limit()));
        }
    }

    public long skip(long bytesToSkip) throws IOException {
        E.checkArgument(bytesToSkip >= 0L,
                        "The parameter bytesToSkip must be >= 0, but got %s",
                        bytesToSkip);
        long positionBeforeSkip = this.position();
        if (bytesToSkip <= this.remaining()) {
            super.skip(bytesToSkip);
            return positionBeforeSkip;
        } else {
            bytesToSkip -= this.remaining();
            long position = this.inputOffset + bytesToSkip;
            this.seek(position);
            return positionBeforeSkip;
        }
    }

    @Override
    public void close() throws IOException {
        this.input.close();
    }

    protected void require(int size) throws IOException {
        if (this.remaining() >= size) {
            return;
        }
        this.shiftAndFillBuffer();
        if (size > this.limit()) {
            throw new IOException(String.format(
                      "Read %s bytes from position overflows buffer %s",
                      size, this.limit()));
        }
    }

    private void shiftAndFillBuffer() throws IOException {
        this.shiftBuffer();
        this.fillBuffer();
    }

    private void fillBuffer() throws IOException {
        int expectLen = this.bufferSize - this.limit();
        int readLen = this.input.read(this.buffer(), this.limit(), expectLen);
        if (readLen > 0) {
            this.limit(this.limit() + readLen);
            this.inputOffset += readLen;
        }
    }
}
