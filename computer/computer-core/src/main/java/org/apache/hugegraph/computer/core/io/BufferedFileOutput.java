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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.util.E;

/**
 * This class acted as new DataOutputStream(new BufferedOutputStream(new File
 * (file))). It has two functions. The first is buffer the content until the
 * buffer is full. The second is unsafe data output.
 * This class is not thread safe.
 */
public class BufferedFileOutput extends AbstractBufferedFileOutput {

    private final int bufferCapacity;
    private final RandomAccessFile file;

    public BufferedFileOutput(File file) throws IOException {
        this(new RandomAccessFile(file, Constants.FILE_MODE_WRITE),
             Constants.BIG_BUF_SIZE);
    }

    public BufferedFileOutput(RandomAccessFile file, int bufferCapacity) {
        super(bufferCapacity);
        E.checkArgument(bufferCapacity >= 8,
                        "The parameter bufferSize must be >= 8");
        this.bufferCapacity = bufferCapacity;
        this.file = file;
    }

    @Override
    public void write(byte[] b) throws IOException {
        this.write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (len <= this.bufferAvailable()) {
            super.write(b, off, len);
            return;
        }
        this.flushBuffer();
        if (len <= this.bufferCapacity) {
            super.write(b, off, len);
        } else {
            // The len > buffer size, write out directly
            this.file.write(b, off, len);
            this.fileOffset += len;
        }
    }

    @Override
    public void seek(long position) throws IOException {
        if (this.fileOffset <= position && position <= this.position()) {
            super.seek(position - this.fileOffset);
            return;
        }
        this.flushBuffer();
        this.file.seek(position);
        this.fileOffset = position;
    }

    @Override
    protected void flushBuffer() throws IOException {
        int bufferSize = super.bufferSize();
        if (bufferSize == 0) {
            return;
        }
        this.file.write(this.buffer(), 0, bufferSize);
        this.fileOffset += bufferSize;
        super.seek(0);
    }

    @Override
    public void close() throws IOException {
        this.flushBuffer();
        this.file.close();
    }
}
