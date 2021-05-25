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

package com.baidu.hugegraph.computer.core.store.hgkvfile.file.builder;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.commons.lang3.StringUtils;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.HgkvDir;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.HgkvDirImpl;
import com.baidu.hugegraph.util.E;

public class HgkvDirBuilderImpl implements HgkvDirBuilder {

    private final Config config;
    // The max byte size of hgkv-file data
    private final long maxEntriesBytes;

    private final HgkvDir dir;
    private int segmentId;
    private HgkvFileBuilder segmentBuilder;
    private boolean buildFinished;

    public HgkvDirBuilderImpl(String path, Config config) {
        try {
            this.config = config;
            this.maxEntriesBytes = config.get(
                                   ComputerOptions.HGKV_MAX_FILE_SIZE);
            this.dir = HgkvDirImpl.create(path);
            this.segmentId = 0;
            this.segmentBuilder = this.nextSegmentBuilder(this.dir, config);
            this.buildFinished = false;
        } catch (IOException e) {
            throw new ComputerException(e.getMessage(), e);
        }
    }

    @Override
    public void write(KvEntry entry) throws IOException {
        E.checkState(!this.buildFinished,
                     "Can't write entry because it has been finished");
        E.checkArgument(entry != null && entry.key() != null &&
                        entry.value() != null,
                        "Parameter entry must not be empty");

        /*
         * If the segment size is larger than FILE_MAX_SIZE after add entry
         * Stop build of the current segment and create a new segment.
         */
        long entrySize = this.segmentBuilder.sizeOfEntry(entry);
        long segmentSize = this.segmentBuilder.dataLength();
        if ((entrySize + segmentSize) > this.maxEntriesBytes) {
            this.segmentBuilder.finish();
            // Create new hgkvFile.
            this.segmentBuilder = this.nextSegmentBuilder(this.dir,
                                                          this.config);
        }
        this.segmentBuilder.add(entry);
    }

    @Override
    public void finish() throws IOException {
        if (this.buildFinished) {
            return;
        }
        this.segmentBuilder.finish();
        this.buildFinished = true;
    }

    @Override
    public void close() throws IOException {
        this.finish();
    }

    private HgkvFileBuilder nextSegmentBuilder(HgkvDir dir, Config config)
                                               throws IOException {
        String fileName = StringUtils.join(HgkvDirImpl.FILE_NAME_PREFIX,
                                           String.valueOf(++this.segmentId),
                                           HgkvDirImpl.FILE_EXTEND_NAME);
        String path = Paths.get(dir.path(), fileName).toString();
        return new HgkvFileBuilderImpl(path, config);
    }
}
