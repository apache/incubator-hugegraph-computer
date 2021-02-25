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
public class BufferedFileDataOutput extends UnsafeByteArrayOutput {

    private final RandomAccessFile file;
    private final int bufferSize;
    private long fileOffset;

    public BufferedFileDataOutput(File file) throws FileNotFoundException {
        this(new RandomAccessFile(file, "rw"), Constants.DEFAULT_BUFFER_SIZE);
    }

    public BufferedFileDataOutput(RandomAccessFile file, int bufferSize) {
        super(bufferSize);
        E.checkArgument(bufferSize >= 8,
                        "The parameter bufferSize must be >= 8");
        this.bufferSize = bufferSize;
        this.file = file;
        this.fileOffset = 0L;
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
            this.file.write(b, off, len);
            this.fileOffset += len;
        }
    }

    @Override
    public void writeInt(long position, int v) throws IOException {
        this.seek(position);
        super.writeInt(v);
    }

    @Override
    public long position() {
        return this.fileOffset + super.position();
    }

    @Override
    public void seek(long position) throws IOException {
        if (this.fileOffset <= position &&
            position <= this.fileOffset + this.bufferSize) {
            super.seek(position - this.fileOffset);
            return;
        }
        this.writeBuffer();
        this.file.seek(position);
        this.fileOffset = position;
    }

    @Override
    public long skip(long size) throws IOException {
        E.checkArgument(size <= Integer.MAX_VALUE,
                        "The parameter bytesToSkip must be <= " +
                        "Integer.MAX_VALUE");
        long positionBeforeSkip = this.fileOffset + super.position();
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
            this.fileOffset += size;
            this.file.seek(this.fileOffset);
        }
        return positionBeforeSkip;
    }

    /**
     * If the data is  non primitive type, it is override in this class. So
     * this size <= 8.
     */
    @Override
    protected void require(int size) throws IOException {
        E.checkArgument(size <= this.bufferSize, "size must be <=8");
        long position = super.position();
        long bufferAvailable = this.bufferSize - position;
        if (bufferAvailable >= size) {
            super.require(size);
            return;
        }
        this.writeBuffer();
        return;
    }

    private void writeBuffer() throws IOException {
        int bufferPosition = (int) super.position();
        if (bufferPosition == 0) {
            return;
        }
        this.file.write(this.buffer(), 0, bufferPosition);
        this.fileOffset += bufferPosition;
        super.seek(0);
    }

    public void close() throws IOException {
        this.writeBuffer();
        this.file.close();
    }

    private int bufferAvailable() {
        return this.buffer().length - (int) super.position();
    }
}
