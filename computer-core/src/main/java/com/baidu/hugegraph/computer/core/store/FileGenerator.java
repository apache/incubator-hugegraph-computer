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

import java.nio.file.Paths;

public interface FileGenerator {

    /**
     * FileGenerator manages the local base directories of a container.
     * The local base directories can be got from config.
     * For example, the local base directories configured
     * ["/disk1/job_001/container_001", "/disk2/job_001/container_001"].
     * It indicates there are two local base directories and one directory for
     * one local disks.
     *
     * Note: Can't request a directory and write many files into it, this will
     *       cause the io pressure can't distributed over several disks.
     *
     * @return The directory of allocated local base directory.
     */
    String nextBaseDirectory();

    /**
     * @param paths The paths as sub-directory.
     * @return A string representation of a directory "#nextBaseDirectory() +
     * paths"
     */
    default String nextDirectory(String... paths) {
        return Paths.get(nextBaseDirectory(), paths).toString();
    }
}
