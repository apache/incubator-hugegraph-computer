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
import java.util.Iterator;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.store.base.KvEntry;
import com.baidu.hugegraph.computer.core.store.base.Pointer;
import com.baidu.hugegraph.computer.core.store.file.HgkvDir;
import com.baidu.hugegraph.computer.core.store.file.HgkvDirImpl;
import com.baidu.hugegraph.util.E;

public class HgkvDirBuilderImpl implements HgkvDirBuilder {

    // The max byte size of hgkv-file data
    private static final long MAX_ENTRIES_SIZE;

    private HgkvFileBuilder segmentBuilder;
    private final HgkvDir dir;
    private boolean finished;

    static {
        Config config = ComputerContext.instance().config();
        MAX_ENTRIES_SIZE = config.get(ComputerOptions.HGKVFILE_SIZE);
    }

    public HgkvDirBuilderImpl(String path) throws IOException {
        this.dir = HgkvDirImpl.create(path);
        this.segmentBuilder = this.nextSegmentBuilder(this.dir);
        this.finished = false;
    }

    @Override
    public void write(KvEntry kvEntry) throws IOException {
        E.checkState(!this.finished,
                     "build finished, can't continue to add data");
        E.checkArgument(kvEntry != null && kvEntry.values() != null &&
                        kvEntry.values().hasNext(),
                        "entry or entry-values must not be empty");

        Pointer key = kvEntry.key();
        Iterator<Pointer> values = kvEntry.values();
        while (values.hasNext()) {
            Pointer value = values.next();
            // If the segment size is larger than FILE_MAX_SIZE after add entry
            // Stop build of the current segment and create a new segment.
            long entrySize = this.segmentBuilder.sizeOfEntry(key, value);
            long segmentSize = this.segmentBuilder.entriesSize();
            if ((entrySize + segmentSize) > MAX_ENTRIES_SIZE) {
                this.segmentBuilder.finish();
                // Create new hgkvFile.
                this.segmentBuilder = this.nextSegmentBuilder(this.dir);
            }
            this.segmentBuilder.add(key, value);
        }
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

    private HgkvFileBuilder nextSegmentBuilder(HgkvDir dir) throws IOException {
        return new HgkvFileBuilderImpl(dir.nextSegmentPath());
    }
}
