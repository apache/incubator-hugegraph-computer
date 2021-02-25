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

public class BufferedFileDataInput extends UnsafeByteArrayInput
                                   implements RandomAccessInput {

    private long fileOffset;
    private int bufferSize;
    private final RandomAccessFile file;
    private final long fileLength;

    public BufferedFileDataInput(File file) throws IOException {
        this(new RandomAccessFile(file, "r"), Constants.DEFAULT_BUFFER_SIZE);
    }

    public BufferedFileDataInput(RandomAccessFile file, int bufferSize)
                                 throws IOException {
        super(new byte[bufferSize], 0);
        E.checkArgument(bufferSize >= 8,
                        "The parameter bufferSize must be >= 8");
        this.file = file;
        this.fileLength = file.length();
        this.bufferSize = bufferSize;
        int readLen = (int) Math.min(bufferSize, this.fileLength);
        this.file.readFully(this.buffer(), 0, readLen);
        this.fileOffset = readLen;
        super.limit(readLen);
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
        if (len <= super.remaining()) {
            super.readFully(b, off, len);
        } else if (len <= this.bufferSize) {
            this.cutAndRead();
            super.readFully(b, off, len);
        } else {
            int remaining = super.remaining();
            super.readFully(b, off, remaining);
            this.file.readFully(b, off + remaining, len - remaining);
            this.fileOffset += len;
        }
    }

    @Override
    public void seek(long position) throws IOException {
        if (position < this.fileOffset &&
            position >= this.fileOffset - this.bufferSize) {
            super.seek(this.bufferSize - (this.fileOffset - position));
            return;
        }
        if (position < this.fileLength) {
            this.file.seek(position);
            int readLen = (int) Math.min(this.bufferSize,
                                         this.fileLength - position);
            super.seek(0L);
            this.file.readFully(this.buffer(), 0, readLen);
            super.limit(readLen);
            this.fileOffset = position + readLen;
        } else {
            throw new EOFException("Reach the end of file");
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
        long position = this.fileOffset + n;
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
            cutAndRead();
        } else {
            throw new ComputerException("Should not reach here");
        }
    }

    private void cutAndRead() throws IOException {
        this.cutReadBuffer();
        int readLen = Math.min(this.bufferSize - super.limit(),
                               (int) (this.fileLength - this.fileOffset));
        this.fileOffset += readLen;
        this.file.readFully(this.buffer(), super.limit(), readLen);
        super.limit(super.limit() + readLen);
    }
}
