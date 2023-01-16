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
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.io.AbstractBufferedFileOutput;
import org.apache.hugegraph.util.E;

public class ValueFileOutput extends AbstractBufferedFileOutput {

    private final long maxSegmentSize;
    private final File dir;
    private final List<File> segments;

    private int segmentIndex;
    private RandomAccessFile currentSegment;

    public ValueFileOutput(Config config, File dir) throws IOException {
        this(config, dir, Constants.BIG_BUF_SIZE);
    }

    public ValueFileOutput(Config config, File dir, int bufferCapacity)
                           throws IOException {
        super(bufferCapacity);

        this.maxSegmentSize = config.get(
                ComputerOptions.VALUE_FILE_MAX_SEGMENT_SIZE);
        E.checkArgument(this.maxSegmentSize <= Integer.MAX_VALUE,
                        "Max size of segment must be smaller than '%s' " +
                        "but get '%s'", Integer.MAX_VALUE, this.maxSegmentSize);
        E.checkArgument(bufferCapacity >= 8 &&
                        bufferCapacity <= this.maxSegmentSize,
                        "The parameter bufferCapacity must be >= 8 " +
                        "and <= %s", this.maxSegmentSize);
        E.checkArgument(dir.isDirectory(),
                        "The parameter dir must be a directory");
        this.dir = dir;
        this.segments = ValueFile.scanSegment(dir);
        this.segmentIndex = -1;
        this.currentSegment = this.nextSegment();
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

        this.flushBuffer();
        if (this.bufferCapacity() >= len) {
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

            /*
             * The read length maybe greater than the size of multiple segments.
             * So need to write multiple segments.
             */
            while (true) {
                this.currentSegment.close();
                this.currentSegment = this.nextSegment();
                if (tempLen > this.maxSegmentSize) {
                    this.currentSegment.write(b, offset,
                                              (int) this.maxSegmentSize);
                    tempLen -= this.maxSegmentSize;
                    offset += this.maxSegmentSize;
                } else {
                    this.currentSegment.write(b, offset, tempLen);
                    break;
                }
            }
        }
        this.fileOffset += len;
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

        int segmentIndex = (int) (position / this.maxSegmentSize);
        if (segmentIndex >= this.segments.size()) {
            throw new EOFException(String.format(
                      "Can't seek to %s, reach the end of file",
                      position));
        }

        this.flushBuffer();
        if (segmentIndex != this.segmentIndex) {
            this.currentSegment.close();
            this.currentSegment = new RandomAccessFile(
                                  this.segments.get(segmentIndex),
                                  Constants.FILE_MODE_WRITE);
            this.segmentIndex = segmentIndex;
        }
        long seekPosition = position - segmentIndex * this.maxSegmentSize;
        this.currentSegment.seek(seekPosition);
        this.fileOffset = position;
    }

    @Override
    public void close() throws IOException {
        this.flushBuffer();
        this.currentSegment.close();
    }

    @Override
    public void flushBuffer() throws IOException {
        int segmentRemain = this.currentSegmentRemain();
        int bufferSize = super.bufferSize();

        if (segmentRemain >= bufferSize) {
            this.currentSegment.write(super.buffer(), 0, bufferSize);
        } else {
            this.currentSegment.write(super.buffer(), 0, segmentRemain);
            this.currentSegment.close();
            this.currentSegment = this.nextSegment();
            this.currentSegment.write(super.buffer(), segmentRemain,
                                      bufferSize - segmentRemain);
        }
        this.fileOffset += bufferSize;
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
            E.checkState(result, "Failed to create segment '%s'",
                         segment.getAbsolutePath());
        }
        return new RandomAccessFile(segment, Constants.FILE_MODE_WRITE);
    }

    private int currentSegmentRemain() throws IOException {
        long remain = this.maxSegmentSize -
                      this.currentSegment.getFilePointer();
        return (int) remain;
    }
}
