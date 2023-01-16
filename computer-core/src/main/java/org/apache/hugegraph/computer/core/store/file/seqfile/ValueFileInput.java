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

package org.apache.hugegraph.computer.core.store.file.seqfile;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.io.AbstractBufferedFileInput;
import org.apache.hugegraph.computer.core.io.UnsafeBytesInput;
import org.apache.hugegraph.util.E;

public class ValueFileInput extends AbstractBufferedFileInput {

    private final Config config;
    private final long maxSegmentSize;
    private final File dir;
    private final List<File> segments;

    private int segmentIndex;
    private RandomAccessFile currentSegment;

    public ValueFileInput(Config config, File dir) throws IOException {
        this(config, dir, Constants.BIG_BUF_SIZE);
    }

    public ValueFileInput(Config config, File dir, int bufferCapacity)
                          throws IOException {
        super(bufferCapacity, ValueFile.fileLength(dir));

        this.config = config;
        this.maxSegmentSize = config.get(
                ComputerOptions.VALUE_FILE_MAX_SEGMENT_SIZE);
        E.checkArgument(bufferCapacity >= 8 &&
                        bufferCapacity <= this.maxSegmentSize,
                        "The parameter bufferSize must be >= 8 " +
                        "and <= %s", this.maxSegmentSize);
        E.checkArgument(dir.isDirectory(),
                        "The parameter dir must be a directory");
        this.dir = dir;
        this.segments = ValueFile.scanSegment(dir);
        E.checkArgument(CollectionUtils.isNotEmpty(this.segments),
                        "Can't find any segment in dir '%s'",
                        dir.getAbsolutePath());

        this.segmentIndex = -1;
        this.currentSegment = this.nextSegment();
        this.fileOffset = 0L;

        this.fillBuffer();
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        int bufferRemain = super.remaining();
        int segmentRemain = this.currentSegmentRemain();

        if (bufferRemain >= len) {
            super.readFully(b, off, len);
        } else if (this.bufferCapacity() >= len) {
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
                if (length > this.maxSegmentSize) {
                    this.currentSegment.readFully(b, offset,
                                                  (int) this.maxSegmentSize);
                    offset += this.maxSegmentSize;
                    length -= this.maxSegmentSize;
                } else {
                    this.currentSegment.readFully(b, offset, length);
                    break;
                }
            }
            this.fileOffset += (len - bufferRemain);
        }
    }

    @Override
    public void seek(long position) throws IOException {
        if (position == this.position()) {
            return;
        }
        if (position > this.fileLength()) {
            throw new EOFException(String.format(
                                   "Can't seek to %s, reach the end of file",
                                   position));
        }

        // Seek position in buffer
        long bufferStart = this.fileOffset - this.limit();
        if (bufferStart <= position && position <= this.fileOffset) {
            super.seek(position - bufferStart);
            return;
        }

        /*
         * Calculate the segment corresponding to position and
         * seek to the corresponding position of the segment
         */
        int segmentIndex = (int) (position / this.maxSegmentSize);
        if (segmentIndex != this.segmentIndex) {
            this.currentSegment.close();
            this.currentSegment = new RandomAccessFile(
                                  this.segments.get(segmentIndex),
                                  Constants.FILE_MODE_READ);
            this.segmentIndex = segmentIndex;
        }
        long seekPosition = position - segmentIndex * this.maxSegmentSize;
        this.currentSegment.seek(seekPosition);

        // Reset buffer after seek
        super.seek(0L);
        this.limit(0);
        this.fileOffset = position;

        this.fillBuffer();
    }

    @Override
    public void close() throws IOException {
        this.currentSegment.close();
    }

    @Override
    protected void fillBuffer() throws IOException {
        int readLen = Math.min(this.bufferCapacity() - this.limit(),
                               (int) (this.fileLength() - this.fileOffset));
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

    @Override
    public UnsafeBytesInput duplicate() throws IOException {
        ValueFileInput input = new ValueFileInput(this.config, this.dir,
                                                  this.bufferCapacity());
        input.seek(this.position());
        return input;
    }

    private int currentSegmentRemain() throws IOException {
        long remain = this.maxSegmentSize -
                      this.currentSegment.getFilePointer();
        return (int) remain;
    }

    private RandomAccessFile nextSegment() throws FileNotFoundException {
        File segment = this.segments.get(++this.segmentIndex);
        return new RandomAccessFile(segment, Constants.FILE_MODE_READ);
    }
}
