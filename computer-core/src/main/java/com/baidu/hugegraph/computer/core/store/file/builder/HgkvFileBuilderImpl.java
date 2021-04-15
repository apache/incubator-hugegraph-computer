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

package com.baidu.hugegraph.computer.core.store.file.builder;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.NotSupportedException;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.io.BufferedFileOutput;
import com.baidu.hugegraph.computer.core.io.RandomAccessOutput;
import com.baidu.hugegraph.computer.core.store.base.Pointer;
import com.baidu.hugegraph.computer.core.store.file.HgkvFile;
import com.baidu.hugegraph.computer.core.store.file.HgkvFileImpl;
import com.baidu.hugegraph.util.E;

public class HgkvFileBuilderImpl implements HgkvFileBuilder {

    // Max entries size of a block
    private static final int BLOCK_SIZE_THRESHOLD;

    private final HgkvFile hgkvFile;
    private final BufferedFileOutput output;
    private final BlockBuilder dataBlockBuilder;
    private boolean finished;
    private long entriesSize;
    private long fileSize;

    private final List<Pointer> indexBlock;
    private long numEntries;
    private long dataBlockLength;
    private long indexBlockLength;
    private long footerLength;
    private long maxKeyOffset;
    private final long minKeyOffset;

    static {
        Config config = ComputerContext.instance().config();
        BLOCK_SIZE_THRESHOLD = config.get(
                ComputerOptions.DATABLOCK_SIZE);
    }

    public HgkvFileBuilderImpl(String path) throws IOException {
        this.hgkvFile = HgkvFileImpl.create(path);
        RandomAccessFile file = new RandomAccessFile(path,
                                                     Constants.FILE_MODE_WRITE);
        this.output = new BufferedFileOutput(file, BLOCK_SIZE_THRESHOLD);
        this.dataBlockBuilder = new DataBlockBuilderImpl(this.output);
        this.finished = false;
        this.entriesSize = 0;
        this.fileSize = 0;

        this.indexBlock = new ArrayList<>();
        this.numEntries = 0;
        this.dataBlockLength = 0;
        this.indexBlockLength = 0;
        this.maxKeyOffset = 0;
        this.minKeyOffset = 0;
    }

    @Override
    public void add(Pointer key, Pointer value) throws IOException {
        if (this.finished) {
            throw new NotSupportedException("HgkvFile build finished, " +
                                            "can't add new entry.");
        }
        E.checkNotNull(key, "key");
        E.checkNotNull(value, "value");

        this.blockAddEntry(this.dataBlockBuilder, this.indexBlock, key, value);
        this.changeAfterAdd(key, value);
    }

    @Override
    public long sizeOfEntry(Pointer key, Pointer value) {
        long keySize = Integer.BYTES + key.length();
        long valueSize = Integer.BYTES + value.length();
        return keySize + valueSize;
    }

    @Override
    public void finish() throws IOException {
        if (this.finished) {
            return;
        }

        this.dataBlockBuilder.finish();
        this.indexBlockLength = this.writeIndexBlock(this.indexBlock);
        this.footerLength = this.writeFooter(this.output, this.numEntries,
                                             this.maxKeyOffset,
                                             this.minKeyOffset,
                                             this.dataBlockLength,
                                             this.indexBlockLength);
        this.output.close();
        this.dataBlockLength = this.entriesSize;
        this.fileSize += (this.indexBlockLength + this.footerLength);
        this.finished = true;
    }

    @Override
    public long entriesSize() {
        return this.entriesSize;
    }

    @Override
    public long fileSize() {
        return this.fileSize;
    }

    @Override
    public void close() throws IOException {
        this.finish();
    }

    private void changeAfterAdd(Pointer key, Pointer value) {
        this.numEntries++;
        this.maxKeyOffset = this.entriesSize;

        this.entriesSize += this.sizeOfEntry(key, value);
        this.fileSize = this.entriesSize;
        this.dataBlockLength = this.entriesSize;
    }

    private void blockAddEntry(BlockBuilder blockBuilder,
                               List<Pointer> indexBlock, Pointer key,
                               Pointer value) throws IOException {
        // Finish and reset builder if the block is full.
        long entrySize = blockBuilder.sizeOfEntry(key, value);
        long blockSize = blockBuilder.size();
        if ((entrySize + blockSize) >= BLOCK_SIZE_THRESHOLD) {
            blockBuilder.finish();
            blockBuilder.reset();
            indexBlock.add(key);
        }
        blockBuilder.add(key, value);
    }

    private long writeIndexBlock(List<Pointer> indexBlock) throws IOException {
        long indexBlockLength = 0L;
        for (Pointer key : indexBlock) {
            this.output.writeInt((int) key.length());
            this.output.write(key.input(), key.offset(), key.length());
            indexBlockLength += (Integer.BYTES + key.length());
        }
        return indexBlockLength;
    }

    private long writeFooter(RandomAccessOutput output, long numEntries,
                             long maxKeyOffset, long minKeyOffset,
                             long dataBlockLength, long indexBlockLength)
            throws IOException {
        int footerLength = 0;
        // Entries number
        output.writeLong(numEntries);
        footerLength += Long.BYTES;

        // Max key offset
        output.writeLong(maxKeyOffset);
        footerLength += Long.BYTES;

        // Min key offset
        output.writeLong(minKeyOffset);
        footerLength += Long.BYTES;

        // Length of dataBlock
        output.writeLong(dataBlockLength);
        footerLength += Long.BYTES;

        // Length of indexBlock
        output.writeLong(indexBlockLength);
        footerLength += Long.BYTES;

        // Version
        int versionLength = HgkvFileImpl.VERSION.length();
        output.writeInt(versionLength);
        footerLength += Integer.BYTES;
        output.writeBytes(HgkvFileImpl.VERSION);
        footerLength += versionLength;

        // Footer Length
        long position = output.position();
        output.writeInt(0);
        footerLength += Integer.BYTES;

        // Magic
        output.writeBytes(HgkvFileImpl.MAGIC);
        footerLength += HgkvFileImpl.MAGIC.length();

        output.writeInt(position, footerLength);
        return footerLength;
    }
}
