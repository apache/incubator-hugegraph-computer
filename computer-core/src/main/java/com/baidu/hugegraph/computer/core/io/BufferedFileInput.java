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

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.util.E;

public class BufferedFileInput extends UnsafeByteArrayInput {

    private final int bufferSize;
    private final RandomAccessFile file;
    private long fileOffset;

    public BufferedFileInput(File file) throws IOException {
        this(new RandomAccessFile(file, "r"), Constants.DEFAULT_BUFFER_SIZE);
    }

    public BufferedFileInput(RandomAccessFile file, int bufferSize)
                             throws IOException {
        super(new byte[bufferSize], 0);
        E.checkArgument(bufferSize >= 8,
                        "The parameter bufferSize must be >= 8");
        this.file = file;
        this.bufferSize = bufferSize;
        this.fillBuffer();
    }

    @Override
    public long position() {
        return this.fileOffset - super.remaining();
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        this.readFully(b, 0, b.length);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        int remaining = super.remaining();
        if (len <= remaining) {
            super.readFully(b, off, len);
        } else if (len <= this.bufferSize) {
            this.shiftAndFillBuffer();
            super.readFully(b, off, len);
        } else {
            super.readFully(b, off, remaining);
            this.file.readFully(b, off + remaining, len - remaining);
            this.fileOffset += len;
        }
    }

    @Override
    public void seek(long position) throws IOException {
        if (position < this.fileOffset &&
            position >= this.fileOffset - this.bufferSize) {
            super.seek(this.limit() - (this.fileOffset - position));
            return;
        }
        if (position >= this.file.length()) {
            throw new EOFException(String.format(
                                   "Can't seek to %s, reach the end of file",
                                   position));
        } else {
            this.file.seek(position);
            super.seek(0L);
            this.limit(0);
            this.fileOffset = position;
            this.fillBuffer();
        }

    }

    public long skip(long bytesToSkip) throws IOException {
        E.checkArgument(bytesToSkip >= 0,
                        "The parameter bytesToSkip must be >=0, but got %s",
                        bytesToSkip);
        long positionBeforeSkip = this.position();
        if (this.remaining() >= bytesToSkip) {
            super.skip(bytesToSkip);
            return positionBeforeSkip;
        }
        bytesToSkip -= this.remaining();
        long position = this.fileOffset + bytesToSkip;
        this.seek(position);
        return positionBeforeSkip;
    }

    @Override
    public void close() throws IOException {
        this.file.close();
    }

    protected void require(int size) throws IOException {
        if (this.remaining() >= size) {
            return;
        }
        if (this.bufferSize >= size) {
            this.shiftAndFillBuffer();
        } else {
            throw new ComputerException("Should not reach here");
        }
    }

    private void shiftAndFillBuffer() throws IOException {
        this.shiftBuffer();
        this.fillBuffer();
    }

    private void fillBuffer() throws IOException {
        long fileLength = this.file.length();
        int readLen = Math.min(this.bufferSize - this.limit(),
                               (int) (fileLength - this.fileOffset));
        this.fileOffset += readLen;
        this.file.readFully(this.buffer(), this.limit(), readLen);
        this.limit(this.limit() + readLen);
    }
}
