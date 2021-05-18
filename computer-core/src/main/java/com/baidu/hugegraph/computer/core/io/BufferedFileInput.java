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

import javax.ws.rs.NotSupportedException;

import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.computer.core.util.BytesUtil;
import com.baidu.hugegraph.testutil.Whitebox;
import com.baidu.hugegraph.util.E;

public class BufferedFileInput extends UnsafeBytesInput {

    private final int bufferCapacity;
    private final RandomAccessFile file;
    private long fileOffset;

    public BufferedFileInput(File file) throws IOException {
        this(new RandomAccessFile(file, Constants.FILE_MODE_READ),
             Constants.DEFAULT_BUFFER_SIZE);
    }

    public BufferedFileInput(RandomAccessFile file, int bufferCapacity)
                             throws IOException {
        super(new byte[bufferCapacity], 0);
        E.checkArgument(bufferCapacity >= 8,
                        "The parameter bufferSize must be >= 8");
        this.file = file;
        this.bufferCapacity = bufferCapacity;
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
        } else if (len <= this.bufferCapacity) {
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
        long bufferStart = this.fileOffset - this.limit();
        if (position >= bufferStart && position < this.fileOffset) {
            super.seek(position - bufferStart);
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

    @Override
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

    @Override
    protected void require(int size) throws IOException {
        if (this.remaining() >= size) {
            return;
        }
        if (this.bufferCapacity >= size) {
            this.shiftAndFillBuffer();
        }
        /*
         * The buffer capacity must be >= 8, read primitive data like int,
         * long, float, double can be read from buffer. Only read bytes may
         * exceed the limit, and read bytes using readFully is overrode
         * in this class. In conclusion, the required data can be
         * supplied after shiftAndFillBuffer.
         */
        if (size > this.limit()) {
            throw new IOException(String.format(
                      "Read %s bytes from position %s overflows buffer %s",
                       size, this.position(), this.limit()));
        }
    }

    @Override
    public long available() throws IOException {
        return this.file.length() - this.position();
    }

    private void shiftAndFillBuffer() throws IOException {
        this.shiftBuffer();
        this.fillBuffer();
    }

    private void fillBuffer() throws IOException {
        long fileLength = this.file.length();
        int readLen = Math.min(this.bufferCapacity - this.limit(),
                               (int) (fileLength - this.fileOffset));
        this.fileOffset += readLen;
        this.file.readFully(this.buffer(), this.limit(), readLen);
        this.limit(this.limit() + readLen);
    }

    @Override
    public BufferedFileInput duplicate() throws IOException {
        String path = Whitebox.getInternalState(this.file, "path");
        BufferedFileInput input = new BufferedFileInput(new File(path));
        input.seek(this.position());
        return input;
    }

    @Override
    public int compare(long offset, long length, RandomAccessInput other,
                       long otherOffset, long otherLength) throws IOException {
        if (other.getClass() != BufferedFileInput.class) {
            throw new NotSupportedException();
        }

        BufferedFileInput otherInput = (BufferedFileInput) other;
        if ((offset + length) <= this.limit()) {
            if ((otherOffset + otherLength) <= otherInput.limit()) {
                return BytesUtil.compare(this.buffer(), (int) offset,
                                         (int) length, otherInput.buffer(),
                                         (int) otherOffset, (int) otherLength);
            } else {
                long otherPosition = other.position();
                other.seek(otherOffset);
                byte[] bytes = other.readBytes((int) otherLength);
                other.seek(otherPosition);
                return BytesUtil.compare(this.buffer(), (int) offset,
                                         (int) length, bytes, 0,
                                         bytes.length);
            }
        } else {
            long position = this.position();
            this.seek(offset);
            byte[] bytes1 = this.readBytes((int) length);
            this.seek(position);

            if ((otherOffset + otherLength) <= otherInput.limit()) {
                return BytesUtil.compare(bytes1, 0, bytes1.length,
                                         otherInput.buffer(), (int) otherOffset,
                                         (int) otherLength);
            } else {
                long otherPosition = other.position();
                other.seek(otherOffset);
                byte[] bytes2 = other.readBytes((int) otherLength);
                other.seek(otherPosition);
                return BytesUtil.compare(bytes1, 0, bytes1.length,
                                         bytes2, 0, bytes2.length);
            }
        }
    }
}
