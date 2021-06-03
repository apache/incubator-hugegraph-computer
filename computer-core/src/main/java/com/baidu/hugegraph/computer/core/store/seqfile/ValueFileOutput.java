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

package com.baidu.hugegraph.computer.core.store.seqfile;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.io.UnsafeBytesOutput;
import com.baidu.hugegraph.util.E;

public class ValueFileOutput extends UnsafeBytesOutput {

    private final long segmentMaxSize;
    private final File dir;
    private final int bufferCapacity;
    private final List<File> segments;

    private int segmentIndex;
    private RandomAccessFile currentSegment;
    private long fileOffset;

    public ValueFileOutput(Config config, File dir) throws IOException {
        this(config, dir, Constants.BIG_BUF_SIZE);
    }

    public ValueFileOutput(Config config, File dir, int bufferCapacity)
                           throws IOException {
        super(bufferCapacity);

        this.segmentMaxSize = config.get(
                              ComputerOptions.VALUE_FILE_MAX_SEGMENT_SIZE);
        E.checkArgument(this.segmentMaxSize <= Integer.MAX_VALUE,
                        "Max size of segment must be smaller than '%s' " +
                        "but get '%s'", Integer.MAX_VALUE, this.segmentMaxSize);
        E.checkArgument(bufferCapacity >= 8 &&
                        bufferCapacity <= this.segmentMaxSize,
                        "The parameter bufferSize must be >= 8 " +
                        "and <= %s", this.segmentMaxSize);
        E.checkArgument(dir.isDirectory(),
                        "The parameter dir must be a directory");
        this.dir = dir;
        this.bufferCapacity = bufferCapacity;
        this.segments = ValueFile.scanSegment(dir);

        this.segmentIndex = -1;
        this.currentSegment = this.nextSegment();
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

        this.flush();
        if (this.bufferCapacity >= len) {
            super.write(b, off, len);
            return;
        }

        int remain = this.currentSegmentRemain();
        if (remain >= len) {
            this.currentSegment.write(b, off, len);
        } else {
            int tempLen = len;
            int offset = off;
            this.currentSegment.write(b, offset, remain);

            tempLen -= remain;
            offset += remain;

            while (true) {
                this.currentSegment.close();
                this.currentSegment = this.nextSegment();
                if (tempLen > this.segmentMaxSize) {
                    this.currentSegment.write(b, offset,
                                              (int) this.segmentMaxSize);
                    tempLen -= this.segmentMaxSize;
                    offset += this.segmentMaxSize;
                } else {
                    this.currentSegment.write(b, offset, tempLen);
                    break;
                }
            }
        }
        this.fileOffset += len;
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
    protected void require(int size) throws IOException {
        if (size <= this.bufferAvailable()) {
            return;
        }
        this.flush();
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

    @Override
    public long position() {
        return this.fileOffset + super.position();
    }

    @Override
    public void seek(long position) throws IOException {
        E.checkArgument(position >= 0,
                        "Parameter position must >= 0, but get '%s'",
                        position);
        if (this.fileOffset <= position && position <= this.position()) {
            super.seek(position - this.fileOffset);
            return;
        }

        int segmentIndex = (int) (position / this.segmentMaxSize);
        if (segmentIndex >= this.segments.size()) {
            throw new EOFException(String.format(
                                   "Can't seek to %s, reach the end of file",
                                   position));
        }

        this.flush();
        if (segmentIndex != this.segmentIndex) {
            this.currentSegment.close();
            this.currentSegment = new RandomAccessFile(
                                  this.segments.get(segmentIndex),
                                  Constants.FILE_MODE_WRITE);
            this.segmentIndex = segmentIndex;
        }
        long seekPosition = position - segmentIndex * this.segmentMaxSize;
        this.currentSegment.seek(seekPosition);
        this.fileOffset = position;
    }

    @Override
    public long skip(long n) throws IOException {
        long positionBeforeSkip = this.position();
        this.seek(positionBeforeSkip + n);
        return positionBeforeSkip;
    }

    @Override
    public void close() throws IOException {
        this.flush();
        this.currentSegment.close();
    }

    public void flush() throws IOException {
        int segmentRemain = this.currentSegmentRemain();
        int bufferPosition = (int) super.position();

        if (segmentRemain >= bufferPosition) {
            this.currentSegment.write(super.buffer(), 0, bufferPosition);
        } else {
            this.currentSegment.write(super.buffer(), 0, segmentRemain);
            this.currentSegment.close();
            this.currentSegment = this.nextSegment();
            this.currentSegment.write(super.buffer(), segmentRemain,
                                      bufferPosition - segmentRemain);
        }
        this.fileOffset += bufferPosition;
        super.seek(0L);
    }

    private RandomAccessFile nextSegment() throws IOException {
        File segment;
        if (++this.segmentIndex < this.segments.size()) {
            segment = this.segments.get(this.segmentIndex);
        } else {
            segment = ValueFile.segmentFromId(this.dir, this.segmentIndex);
            this.segments.add(segment);
        }

        if (!segment.exists()) {
            boolean result = segment.createNewFile();
            E.checkState(result, "Fail to create segment");
        }
        return new RandomAccessFile(segment, Constants.FILE_MODE_WRITE);
    }

    private int currentSegmentRemain() throws IOException {
        long remain = this.segmentMaxSize -
                      this.currentSegment.getFilePointer();
        return (int) remain;
    }

    private int bufferAvailable() {
        return this.bufferCapacity - (int) super.position();
    }
}
