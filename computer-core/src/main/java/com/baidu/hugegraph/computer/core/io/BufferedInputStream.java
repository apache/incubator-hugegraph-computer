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

public class BufferedInputStream  extends UnsafeByteArrayInput {

    private final int bufferSize;
    private final InputStream in;
    private long inputOffset;

    public BufferedInputStream(InputStream in) throws IOException {
        this(in, Constants.DEFAULT_BUFFER_SIZE);
    }

    public BufferedInputStream(InputStream in, int bufferSize)
                               throws IOException {
        super(new byte[bufferSize], 0);
        E.checkArgument(bufferSize >= 8,
                        "The parameter bufferSize must be >= 8");
        this.in = in;
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
            int readLen = this.in.read(b, off + remaining, expectedLen);
            if (readLen != expectedLen) {
                throw new IOException("There is no enough data in input " +
                                      "stream");
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
        if (position >= this.inputOffset) {
            int skipLen = (int) (position - this.inputOffset);
            this.inputOffset += skipLen;
            byte[] buffer = this.buffer();
            while (skipLen > 0) {
                int expectLen = Math.min(skipLen, this.bufferSize);
                int readLen = this.in.read(buffer, 0, expectLen);
                if (readLen != expectLen) {
                    throw new IOException("Can't seek at position " + position);
                }
                skipLen -= expectLen;
            }
            super.seek(0);
            super.limit(0);
            this.fillBuffer();

        } else {
            throw new IOException("The position is beyond the buffer");
        }
    }

    public long skip(long n) throws IOException {
        E.checkArgument(n >= 0, "The parameter n must be >=0, but got %s", n);
        long positionBeforeSkip = this.position();
        if (this.remaining() >= n) {
            super.skip(n);
            return positionBeforeSkip;
        }
        n -= this.remaining();
        long position = this.inputOffset + n;
        this.seek(position);
        return positionBeforeSkip;
    }

    @Override
    public void close() throws IOException {
        this.in.close();
    }

    protected void require(int size) throws IOException {
        if (this.remaining() >= size) {
            return;
        }
        this.shiftAndFillBuffer();
        if (size > super.limit()) {
            throw new IOException("Can't read " + size + " bytes");
        }
    }

    private void shiftAndFillBuffer() throws IOException {
        this.shiftBuffer();
        this.fillBuffer();
    }

    private int fillBuffer() throws IOException {
        int expectLen = this.bufferSize - super.limit();
        int readLen = this.in.read(this.buffer(), super.limit(), expectLen);
        if (readLen > 0) {
            super.limit(super.limit() + readLen);
            this.inputOffset += readLen;
        }
        return readLen;
    }
}
