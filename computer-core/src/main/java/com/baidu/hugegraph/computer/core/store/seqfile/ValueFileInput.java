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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;

import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.UnsafeBytesInput;
import com.baidu.hugegraph.computer.core.util.BytesUtil;
import com.baidu.hugegraph.exception.NotSupportException;
import com.baidu.hugegraph.util.E;

public class ValueFileInput extends UnsafeBytesInput {

    private final Config config;
    private final long segmentMaxSize;
    private final int bufferCapacity;
    private final File dir;
    private final List<File> segments;
    private final long fileLength;

    private int segmentIndex;
    private RandomAccessFile currentSegment;
    private long fileOffset;

    public ValueFileInput(Config config, File dir) throws IOException {
        this(config, dir, Constants.BIG_BUF_SIZE);
    }

    public ValueFileInput(Config config, File dir, int bufferCapacity)
                          throws IOException {
        super(new byte[bufferCapacity], 0);

        this.config = config;
        this.segmentMaxSize = config.get(
                              ComputerOptions.VALUE_FILE_MAX_SEGMENT_SIZE);
        E.checkArgument(bufferCapacity >= 8 &&
                        bufferCapacity <= this.segmentMaxSize,
                        "The parameter bufferSize must be >= 8 " +
                        "and <= %s", this.segmentMaxSize);
        E.checkArgument(dir.isDirectory(),
                        "The parameter dir must be a directory");
        this.bufferCapacity = bufferCapacity;
        this.dir = dir;
        this.segments = ValueFile.scanSegment(dir);
        E.checkState(CollectionUtils.isNotEmpty(this.segments),
                     "Can't found segment in dir '%s'", dir.getAbsolutePath());
        this.fileLength = this.segments.stream().mapToLong(File::length).sum();

        this.segmentIndex = -1;
        this.currentSegment = this.nextSegment();
        this.fileOffset = 0L;

        this.fillBuffer();
    }

    @Override
    public long position() {
        return this.fileOffset - super.remaining();
    }

    @Override
    public void seek(long position) throws IOException {
        if (position == this.position()) {
            return;
        }
        if (position > this.fileLength) {
            throw new EOFException(String.format(
                                   "Can't seek to %s, reach the end of file",
                                   position));
        }

        long bufferStart = this.fileOffset - this.limit();
        if (bufferStart <= position && position <= this.fileOffset) {
            super.seek(position - bufferStart);
            return;
        }

        int segmentIndex = (int) (position / this.segmentMaxSize);
        if (segmentIndex != this.segmentIndex) {
            this.currentSegment.close();
            this.currentSegment = new RandomAccessFile(
                                  this.segments.get(segmentIndex),
                                  Constants.FILE_MODE_READ);
            this.segmentIndex = segmentIndex;
        }
        long seekPosition = position - segmentIndex * this.segmentMaxSize;
        this.currentSegment.seek(seekPosition);

        super.seek(0L);
        this.limit(0);
        this.fileOffset = position;
        this.fillBuffer();
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        this.readFully(b, 0, b.length);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        int bufferRemain = super.remaining();
        int segmentRemain = this.currentSegmentRemain();

        if (bufferRemain >= len) {
            super.readFully(b, off, len);
        } else if (this.bufferCapacity >= len) {
            this.shiftAndFillBuffer();
            super.readFully(b, off, len);
        } else if ((bufferRemain + segmentRemain) >= len) {
            super.readFully(b, off, bufferRemain);
            this.currentSegment.readFully(b, off + bufferRemain,
                                          len - bufferRemain);
            this.fileOffset += len - bufferRemain;
        } else {
            int offset = off;
            int length = len;

            super.readFully(b, offset, bufferRemain);
            offset += bufferRemain;
            length -= bufferRemain;

            this.currentSegment.readFully(b, offset, segmentRemain);
            offset += segmentRemain;
            length -= segmentRemain;

            while (true) {
                this.currentSegment.close();
                this.currentSegment = this.nextSegment();
                if (length > this.segmentMaxSize) {
                    this.currentSegment.readFully(b, offset,
                                                  (int) this.segmentMaxSize);
                    offset += this.segmentMaxSize;
                    length -= this.segmentMaxSize;
                } else {
                    this.currentSegment.readFully(b, offset,
                                                  length);
                    break;
                }
            }
            this.fileOffset += (len - bufferRemain);
        }
    }

    @Override
    public long skip(long bytesToSkip) throws IOException {
        E.checkArgument(this.available() >= bytesToSkip,
                        "Fail to skip '%s' bytes, because don't have " +
                        "enough data");

        long positionBeforeSeek = this.position();
        this.seek(this.position() + bytesToSkip);
        return positionBeforeSeek;
    }

    @Override
    public long available() throws IOException {
        return this.fileLength - this.position();
    }

    @Override
    public UnsafeBytesInput duplicate() throws IOException {
        ValueFileInput input = new ValueFileInput(this.config, this.dir,
                                                  this.bufferCapacity);
        input.seek(this.position());
        return input;
    }

    @Override
    public int compare(long offset, long length, RandomAccessInput other,
                       long otherOffset, long otherLength) throws IOException {
        assert other != null;
        if (other.getClass() != ValueFileInput.class) {
            throw new NotSupportException("ValueFileInput must be compare " +
                                          "with ValueFileInput");
        }

        ValueFileInput oInput = (ValueFileInput) other;

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
    public void close() throws IOException {
        this.currentSegment.close();
    }

    public long fileLength() {
        return this.fileLength;
    }

    private void shiftAndFillBuffer() throws IOException {
        this.shiftBuffer();
        this.fillBuffer();
    }

    private void fillBuffer() throws IOException {
        int readLen = Math.min(this.bufferCapacity - this.limit(),
                               (int) (this.fileLength - this.fileOffset));
        int remain = this.currentSegmentRemain();
        if (readLen <= remain) {
            this.currentSegment.readFully(this.buffer(), this.limit(), readLen);
        } else {
            this.currentSegment.readFully(this.buffer(), this.limit(), remain);
            this.currentSegment.close();
            this.currentSegment = this.nextSegment();
            this.currentSegment.readFully(this.buffer(),
                                          this.limit() + remain,
                                          readLen - remain);
        }
        this.fileOffset += readLen;
        this.limit(this.limit() + readLen);
    }

    private int currentSegmentRemain() throws IOException {
        long remain = this.segmentMaxSize -
                      this.currentSegment.getFilePointer();
        return (int) remain;
    }

    private RandomAccessFile nextSegment() throws FileNotFoundException {
        File segment = this.segments.get(++this.segmentIndex);
        return new RandomAccessFile(segment, Constants.FILE_MODE_READ);
    }
}
