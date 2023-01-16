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

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.util.E;

public class BufferedFileInput extends AbstractBufferedFileInput {

    private final int bufferCapacity;
    private final RandomAccessFile file;

    public BufferedFileInput(File file) throws IOException {
        this(new RandomAccessFile(file, Constants.FILE_MODE_READ),
             Constants.BIG_BUF_SIZE);
    }

    public BufferedFileInput(RandomAccessFile file, int bufferCapacity)
                             throws IOException {
        super(bufferCapacity, file.length());
        E.checkArgument(bufferCapacity >= 8,
                        "The parameter bufferSize must be >= 8");
        this.file = file;
        this.bufferCapacity = bufferCapacity;
        this.fillBuffer();
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
            len -= remaining;
            this.file.readFully(b, off + remaining, len);
            this.fileOffset += len;
        }
    }

    @Override
    public void seek(long position) throws IOException {
        if (position == this.position()) {
            return;
        }
        long bufferStart = this.fileOffset - this.limit();
        if (position >= bufferStart && position < this.fileOffset) {
            super.seek(position - bufferStart);
            return;
        }
        if (position > this.fileLength()) {
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
    public void close() throws IOException {
        this.file.close();
    }

    @Override
    protected void fillBuffer() throws IOException {
        int readLen = (int) Math.min(this.bufferCapacity - this.limit(),
                                     this.fileLength() - this.fileOffset);
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
}
