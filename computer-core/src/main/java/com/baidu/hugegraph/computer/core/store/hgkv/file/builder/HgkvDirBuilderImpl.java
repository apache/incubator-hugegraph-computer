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

package com.baidu.hugegraph.computer.core.store.hgkv.file.builder;

import java.io.File;
import java.io.IOException;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.store.value.entry.KvEntry;
import com.baidu.hugegraph.computer.core.store.value.entry.Pointer;
import com.baidu.hugegraph.computer.core.store.hgkv.file.HgkvDir;
import com.baidu.hugegraph.computer.core.store.hgkv.file.HgkvDirImpl;
import com.baidu.hugegraph.util.E;

public class HgkvDirBuilderImpl implements HgkvDirBuilder {

    private final Config config;
    // The max byte size of hgkv-file data
    private final long maxEntriesBytes;

    private final HgkvDir dir;
    private int fileId;
    private HgkvFileBuilder segmentBuilder;
    private boolean finished;

    public HgkvDirBuilderImpl(String path, Config config) {
        try {
            this.config = config;
            this.maxEntriesBytes = config.get(
                                          ComputerOptions.HGKV_MAX_FILE_SIZE);
            this.dir = HgkvDirImpl.create(path);
            this.fileId = 0;
            this.segmentBuilder = this.nextSegmentBuilder(this.dir, config);
            this.finished = false;
        } catch (IOException e) {
            throw new ComputerException(e.getMessage(), e);
        }
    }

    @Override
    public void write(KvEntry entry) throws IOException {
        E.checkState(!this.finished,
                     "build finished, can't continue to add data");
        E.checkArgument(entry != null && entry.key() != null &&
                        entry.value() != null,
                        "entry or entry-values must not be empty");

        Pointer key = entry.key();
        Pointer value = entry.value();
        // If the segment size is larger than FILE_MAX_SIZE after add entry
        // Stop build of the current segment and create a new segment.
        long entrySize = this.segmentBuilder.sizeOfEntry(key, value);
        long segmentSize = this.segmentBuilder.dataLength();
        if ((entrySize + segmentSize) > this.maxEntriesBytes) {
            this.segmentBuilder.finish();
            // Create new hgkvFile.
            this.segmentBuilder = this.nextSegmentBuilder(this.dir,
                                                          this.config);
        }
        this.segmentBuilder.add(key, value);
    }

    @Override
    public void finish() throws IOException {
        if (this.finished) {
            return;
        }
        this.segmentBuilder.finish();
        this.finished = true;
    }

    @Override
    public void close() throws IOException {
        this.finish();
    }

    private HgkvFileBuilder nextSegmentBuilder(HgkvDir dir, Config config)
                                               throws IOException {
        String path = dir.path() + File.separator + HgkvDirImpl.NAME_PREFIX +
                      (++this.fileId) + HgkvDirImpl.EXTEND_NAME;
        return new HgkvFileBuilderImpl(path, config);
    }
}
