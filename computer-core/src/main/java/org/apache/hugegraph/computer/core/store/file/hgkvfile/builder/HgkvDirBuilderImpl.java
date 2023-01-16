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
import java.nio.file.Paths;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.store.KvEntryFileWriter;
import org.apache.hugegraph.computer.core.store.entry.KvEntry;
import org.apache.hugegraph.computer.core.store.file.hgkvfile.HgkvDir;
import org.apache.hugegraph.computer.core.store.file.hgkvfile.HgkvDirImpl;
import org.apache.hugegraph.util.E;

public class HgkvDirBuilderImpl implements KvEntryFileWriter {

    private final Config config;
    // The max byte size of hgkv-file data
    private final long maxEntriesBytes;

    private final HgkvDir dir;
    private int segmentId;
    private HgkvFileBuilder segmentBuilder;
    private boolean buildFinished;

    public HgkvDirBuilderImpl(Config config, String path) {
        try {
            this.config = config;
            this.maxEntriesBytes = config.get(
                    ComputerOptions.HGKV_MAX_FILE_SIZE);
            this.dir = HgkvDirImpl.create(path);
            this.segmentId = 0;
            this.segmentBuilder = nextSegmentBuilder(config, this.dir,
                                                     ++this.segmentId);
            this.buildFinished = false;
        } catch (IOException e) {
            throw new ComputerException(e.getMessage(), e);
        }
    }

    @Override
    public void write(KvEntry entry) throws IOException {
        E.checkState(!this.buildFinished,
                     "Failed to write entry, builder is finished");
        E.checkArgument(entry != null && entry.key() != null &&
                        entry.value() != null,
                        "Parameter entry can't be empty");

        /*
         * If the segment size is larger than FILE_MAX_SIZE after add entry
         * Stop build of the current segment and create a new segment.
         */
        long entrySize = this.segmentBuilder.sizeOfEntry(entry);
        long segmentSize = this.segmentBuilder.dataLength();
        if ((entrySize + segmentSize) > this.maxEntriesBytes) {
            // Create new hgkvFile.
            this.segmentBuilder = nextSegmentBuilder(this.config, this.dir,
                                                     ++this.segmentId);
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

    private HgkvFileBuilder nextSegmentBuilder(Config config, HgkvDir dir,
                                               int segmentId)
                                               throws IOException {
        if (this.segmentBuilder != null) {
            this.segmentBuilder.finish();
        }
        String fileName = StringUtils.join(HgkvDirImpl.FILE_NAME_PREFIX,
                                           String.valueOf(segmentId),
                                           HgkvDirImpl.FILE_EXTEND_NAME);
        String path = Paths.get(dir.path(), fileName).toString();
        return new HgkvFileBuilderImpl(config, path);
    }
}
