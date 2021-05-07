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

package com.baidu.hugegraph.computer.core.store;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.manager.Manager;
import com.baidu.hugegraph.util.Log;

public class DataFileManager implements DataFileGenerator, Manager {

    private static final Logger LOG = Log.logger(DataFileManager.class);

    public static final String NAME = "data_dir";

    private List<File> dirs;
    private AtomicInteger sequence;

    public DataFileManager() {
        this.dirs = new ArrayList<>();
        this.sequence = new AtomicInteger();
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void init(Config config) {
        String jobId = config.get(ComputerOptions.JOB_ID);
        List<String> paths = config.get(ComputerOptions.WORKER_DATA_DIRS);
        LOG.info("The directories '{}' configured to persist data", paths);
        for (String path : paths) {
            File dir = new File(path);
            File jobDir = new File(dir, jobId);
            this.mkdirs(jobDir);
            LOG.info("Initialized directory {} to directory list", jobDir);
            this.dirs.add(jobDir);
        }
        /*
         * Shuffle dirs to prevent al workers of the same computer start from
         * same dir.
         */
        Collections.shuffle(this.dirs);
    }

    @Override
    public void close(Config config) {
        for (File dir : this.dirs) {
            FileUtils.deleteQuietly(dir);
        }
    }

    @Override
    public File nextDir() {
        int index = this.sequence.incrementAndGet();
        assert index >= 0;
        return this.dirs.get(index % this.dirs.size());
    }

    @Override
    public File nextFile(String type, int superstep) {
        File dir = this.nextDir();
        File labelDir = new File(dir, type);
        File superStepDir = new File(labelDir, Integer.toString(superstep));
        this.mkdirs(superStepDir);
        return new File(superStepDir, UUID.randomUUID().toString());
    }

    /**
     * Creates the directory named by specified dir.
     */
    private void mkdirs(File dir) {
        if (!dir.mkdirs() && !dir.exists()) {
            throw new ComputerException("Can't create dir %s",
                                        dir.getAbsolutePath());
        }
    }
}
