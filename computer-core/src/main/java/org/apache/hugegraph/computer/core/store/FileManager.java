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

package org.apache.hugegraph.computer.core.store;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.manager.Manager;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public class FileManager implements FileGenerator, Manager {

    private static final Logger LOG = Log.logger(FileManager.class);

    public static final String NAME = "data_dir";

    private List<String> dirs;
    private AtomicInteger sequence;

    public FileManager() {
        this.dirs = new ArrayList<>();
        this.sequence = new AtomicInteger();
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void init(Config config) {
        /*
         * Multiple workers run on same computer will not see each other's
         * directories.
         * If runs on K8S, the worker-container can only see the directories
         * assigned to itself.
         * If runs on YARN, the worker-container's data-dir is set the format
         * "$ROOT/${job_id}/{$container_id}", so each container can only see
         * the directories assigned to itself too. The main class is
         * responsible for converting the setting from YARN to
         * HugeGraph-Computer.
         */
        String jobId = config.get(ComputerOptions.JOB_ID);
        List<String> paths = config.get(ComputerOptions.WORKER_DATA_DIRS);
        LOG.info("The directories '{}' configured to persist data for job {}",
                 paths, jobId);
        for (String path : paths) {
            File dir = new File(path);
            File jobDir = new File(dir, jobId);
            mkdirs(jobDir);
            LOG.debug("Initialized directory '{}' to directory list", jobDir);
            this.dirs.add(jobDir.toString());
        }
        /*
         * Shuffle dirs to prevent al workers of the same computer start from
         * same dir.
         */
        Collections.shuffle(this.dirs);
    }

    @Override
    public void close(Config config) {
        for (String dir : this.dirs) {
            FileUtils.deleteQuietly(new File(dir));
        }
    }

    @Override
    public String nextDirectory() {
        int index = this.sequence.incrementAndGet();
        assert index >= 0;
        return this.dirs.get(index % this.dirs.size());
    }

    @Override
    public List<String> dirs() {
        return this.dirs;
    }

    /**
     * Creates the directory named by specified dir.
     */
    private static void mkdirs(File dir) {
        if (!dir.mkdirs() && !dir.exists()) {
            throw new ComputerException("Can't create dir %s",
                                        dir.getAbsolutePath());
        }
    }
}
