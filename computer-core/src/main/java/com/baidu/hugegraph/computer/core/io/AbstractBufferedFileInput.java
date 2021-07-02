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

import com.baidu.hugegraph.computer.core.util.BytesUtil;
import com.baidu.hugegraph.exception.NotSupportException;
import com.baidu.hugegraph.util.E;

public abstract class AbstractBufferedFileInput extends UnsafeBytesInput {

    private final int bufferCapacity;
    private final long fileLength;

    protected long fileOffset;

    public AbstractBufferedFileInput(int bufferCapacity, long fileLength) {
        super(new byte[bufferCapacity], 0, 0);

        this.bufferCapacity = bufferCapacity;
        this.fileLength = fileLength;
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
    public long skip(long bytesToSkip) throws IOException {
        E.checkArgument(bytesToSkip >= 0,
                        "The parameter bytesToSkip must be >= 0, but got %s",
                        bytesToSkip);
        E.checkArgument(this.available() >= bytesToSkip,
                        "Fail to skip '%s' bytes, because don't have " +
                        "enough data");

        long positionBeforeSkip = this.position();
        this.seek(this.position() + bytesToSkip);
        return positionBeforeSkip;
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
        return this.fileLength - this.position();
    }

    protected void shiftAndFillBuffer() throws IOException {
        this.shiftBuffer();
        this.fillBuffer();
    }

    protected abstract void fillBuffer() throws IOException;

    @Override
    public int compare(long offset, long length, RandomAccessInput other,
                       long otherOffset, long otherLength) throws IOException {
        assert other != null;
        if (!(other instanceof AbstractBufferedFileInput)) {
            throw new NotSupportException("Parameter other must be instance " +
                                          "of AbstractBufferedFileInput");
        }

        AbstractBufferedFileInput oInput = (AbstractBufferedFileInput) other;

        long bufferStart = this.fileOffset - this.limit();
        if (bufferStart <= offset && offset <= this.fileOffset &&
            super.limit() >= length) {
            long oBufferStart = oInput.fileOffset - oInput.limit();
            // Compare all in buffer
            if (oBufferStart <= otherOffset &&
                otherOffset <= oInput.fileOffset &&
                oInput.limit() >= otherLength) {
                return BytesUtil.compare(this.buffer(),
                                         (int) (offset - bufferStart),
                                         (int) length, oInput.buffer(),
                                         (int) (otherOffset - oBufferStart),
                                         (int) otherLength);
            } else {
                long oOldPosition = oInput.position();
                oInput.seek(otherOffset);
                byte[] otherBytes = oInput.readBytes((int) otherLength);
                oInput.seek(oOldPosition);

                return BytesUtil.compare(this.buffer(),
                                         (int) (offset - bufferStart),
                                         (int) length, otherBytes,
                                         0, otherBytes.length);
            }
        } else {
            long oldPosition = this.position();
            this.seek(offset);
            byte[] bytes = this.readBytes((int) length);
            this.seek(oldPosition);

            long oBufferStart = oInput.fileOffset - oInput.limit();
            if (oBufferStart <= otherOffset &&
                otherOffset <= oInput.fileOffset &&
                oInput.limit() >= otherLength) {
                return BytesUtil.compare(bytes,
                                         0, bytes.length,
                                         oInput.buffer(),
                                         (int) (otherOffset - oBufferStart),
                                         (int) otherLength);
            } else {
                long oOldPosition = oInput.position();
                oInput.seek(otherOffset);
                byte[] otherBytes = oInput.readBytes((int) otherLength);
                oInput.seek(oOldPosition);

                return BytesUtil.compare(bytes, 0, bytes.length,
                                         otherBytes, 0, otherBytes.length);
            }
        }
    }

    protected int bufferCapacity() {
        return this.bufferCapacity;
    }

    protected long fileLength() {
        return this.fileLength;
    }
}
