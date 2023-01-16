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

import org.apache.hugegraph.computer.core.util.BytesUtil;
import org.apache.hugegraph.util.E;

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
                        "Failed to skip '%s' bytes, because don't have " +
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
                      "Reading %s bytes from position %s overflows " +
                      "buffer length %s",
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
        byte[] bytes1, bytes2;
        int compareOffset1, compareOffset2;

        /*
         * Set the compare information of the current object.
         * Ture: The compare range of the current object is within the buffer.
         */
        if (rangeInBuffer(this, offset, length)) {
            bytes1 = this.buffer();
            compareOffset1 = (int) (offset - bufferStartPosition(this));
        } else {
            long oldPosition = this.position();
            this.seek(offset);
            bytes1 = this.readBytes((int) length);
            compareOffset1 = 0;
            this.seek(oldPosition);
        }

        /*
         * Set the compare information of the compared object.
         * Ture: The compare range of the compared object is within the buffer
         * and compared object instance of AbstractBufferedFileInput.
         */
        AbstractBufferedFileInput otherInput;
        if (other instanceof AbstractBufferedFileInput &&
            rangeInBuffer(otherInput = (AbstractBufferedFileInput) other,
                          otherOffset, otherLength)) {
            bytes2 = otherInput.buffer();
            long otherBufferStart = bufferStartPosition(otherInput);
            compareOffset2 = (int) (otherOffset - otherBufferStart);
        } else {
            long oldPosition = other.position();
            other.seek(otherOffset);
            bytes2 = other.readBytes((int) otherLength);
            compareOffset2 = 0;
            other.seek(oldPosition);
        }

        return BytesUtil.compare(bytes1, compareOffset1, (int) length,
                                 bytes2, compareOffset2, (int) otherLength);
    }

    private static long bufferStartPosition(AbstractBufferedFileInput input) {
        return input.fileOffset - input.limit();
    }

    private static boolean rangeInBuffer(AbstractBufferedFileInput input,
                                         long offset, long length) {
        long bufferStart = bufferStartPosition(input);
        return bufferStart <= offset && offset <= input.fileOffset &&
               input.limit() >= length;
    }

    protected int bufferCapacity() {
        return this.bufferCapacity;
    }

    protected long fileLength() {
        return this.fileLength;
    }
}
