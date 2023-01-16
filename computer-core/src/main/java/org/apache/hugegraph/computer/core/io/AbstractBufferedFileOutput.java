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

import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.util.E;

public abstract class AbstractBufferedFileOutput extends UnsafeBytesOutput {

    private final int bufferCapacity;

    protected long fileOffset;

    public AbstractBufferedFileOutput(int bufferCapacity) {
        super(bufferCapacity);

        this.bufferCapacity = bufferCapacity;
        this.fileOffset = 0L;
    }

    @Override
    public void writeFixedInt(long position, int v) throws IOException {
        if (this.fileOffset <= position &&
            position <= this.position() - Constants.INT_LEN) {
            super.writeFixedInt(position - this.fileOffset, v);
            return;
        }

        long latestPosition = this.position();
        this.seek(position);
        super.writeInt(v);
        this.seek(latestPosition);
    }

    @Override
    public long position() {
        return this.fileOffset + super.position();
    }

    @Override
    public long skip(long bytesToSkip) throws IOException {
        E.checkArgument(bytesToSkip >= 0,
                        "The parameter bytesToSkip must be >= 0, but got %s",
                        bytesToSkip);
        long positionBeforeSkip = this.position();
        this.seek(positionBeforeSkip + bytesToSkip);
        return positionBeforeSkip;
    }

    @Override
    protected void require(int size) throws IOException {
        E.checkArgument(size <= this.bufferCapacity,
                        "The parameter size must be <= %s",
                        this.bufferCapacity);
        if (size <= this.bufferAvailable()) {
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

    protected int bufferSize() {
        return (int) super.position();
    }

    protected int bufferAvailable() {
        return this.bufferCapacity - (int) super.position();
    }

    protected int bufferCapacity() {
        return this.bufferCapacity;
    }

    protected abstract void flushBuffer() throws IOException;
}
