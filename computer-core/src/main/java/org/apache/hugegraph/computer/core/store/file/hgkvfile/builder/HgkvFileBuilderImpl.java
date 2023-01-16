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

package org.apache.hugegraph.computer.core.store.file.hgkvfile.builder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.io.RandomAccessOutput;
import org.apache.hugegraph.computer.core.store.entry.KvEntry;
import org.apache.hugegraph.computer.core.store.file.hgkvfile.HgkvFile;
import org.apache.hugegraph.computer.core.store.file.hgkvfile.HgkvFileImpl;
import org.apache.hugegraph.util.E;

public class HgkvFileBuilderImpl implements HgkvFileBuilder {

    // Max entries size of a block
    private final long maxDataBlockSize;

    private final HgkvFile file;
    private final RandomAccessOutput output;
    private final BlockBuilder dataBlockBuilder;
    private final IndexBlockBuilder indexBlockBuilder;
    private boolean buildFinished;

    private final List<byte[]> indexBlock;
    private long numEntries;
    private long numSubEntries;
    private long dataLength;
    private int footerLength;
    private long maxKeyOffset;
    private final long minKeyOffset;

    public HgkvFileBuilderImpl(Config config, String path) throws IOException {
        this.maxDataBlockSize = config.get(ComputerOptions.HGKV_DATABLOCK_SIZE);
        this.file = HgkvFileImpl.create(path);
        this.output = this.file.output();
        this.dataBlockBuilder = new DataBlockBuilderImpl(this.output);
        this.indexBlockBuilder = new IndexBlockBuilderImpl(this.output);
        this.buildFinished = false;
        this.dataLength = 0L;

        this.indexBlock = new ArrayList<>();
        this.numEntries = 0L;
        this.numSubEntries = 0L;
        this.footerLength = 0;
        this.maxKeyOffset = 0L;
        this.minKeyOffset = 0L;
    }

    @Override
    public void add(KvEntry entry) throws IOException {
        E.checkState(!this.buildFinished,
                     "Failed to write entry, builder is finished");
        E.checkArgument(entry != null,
                        "Parameter entry can't be null");

        this.blockAddEntry(entry);
        this.changeMetaAfterAdd(entry);
    }

    @Override
    public long sizeOfEntry(KvEntry entry) {
        long keySize = Integer.BYTES + entry.key().length();
        long valueSize = Integer.BYTES + entry.value().length();
        return keySize + valueSize;
    }

    @Override
    public void finish() throws IOException {
        if (this.buildFinished) {
            return;
        }

        this.dataBlockBuilder.finish();
        this.writeIndexBlock();
        this.writeFooter();
        this.output.close();
        this.file.close();
        this.buildFinished = true;
    }

    @Override
    public long dataLength() {
        return this.dataLength;
    }

    @Override
    public long indexLength() {
        return this.indexBlockBuilder.length();
    }

    @Override
    public int headerLength() {
        return this.footerLength;
    }

    @Override
    public void close() throws IOException {
        this.finish();
    }

    private void changeMetaAfterAdd(KvEntry entry) {
        this.numEntries++;
        this.numSubEntries += entry.numSubEntries();
        this.maxKeyOffset = this.dataLength;

        this.dataLength += this.sizeOfEntry(entry);
    }

    private void blockAddEntry(KvEntry entry) throws IOException {
        // Finish and reset builder if the block is full.
        long entrySize = this.dataBlockBuilder.sizeOfEntry(entry);
        long blockSize = this.dataBlockBuilder.size();
        if ((entrySize + blockSize) >= this.maxDataBlockSize) {
            this.dataBlockBuilder.finish();
            this.dataBlockBuilder.reset();

            this.indexBlock.add(entry.key().bytes());
        }
        this.dataBlockBuilder.add(entry);
    }

    private void writeIndexBlock() throws IOException {
        for (byte[] index : this.indexBlock) {
            this.indexBlockBuilder.add(index);
        }
        this.indexBlockBuilder.finish();
    }

    private void writeFooter() throws IOException {
        long startPostion = this.output.position();
        // Write magic
        this.output.writeBytes(HgkvFileImpl.MAGIC);
        // Write numEntries
        this.output.writeLong(this.numEntries);
        // Write numSubEntries
        this.output.writeLong(this.numSubEntries);
        // Write length of dataBlock
        this.output.writeLong(this.dataLength);
        // Write length of indexBlock
        this.output.writeLong(this.indexLength());
        // Write max key offset
        this.output.writeLong(this.maxKeyOffset);
        // Write min key offset
        this.output.writeLong(this.minKeyOffset);
        // Write version
        this.output.writeShort(HgkvFileImpl.MAJOR_VERSION);
        this.output.writeShort(HgkvFileImpl.MINOR_VERSION);
        // Write footerLength
        this.footerLength = (int) (this.output.position() - startPostion);
        this.footerLength += Integer.BYTES;
        this.output.writeFixedInt(this.footerLength);
    }
}
