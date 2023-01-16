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

import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;

public interface FileGenerator {

    /**
     * Allocate a base directory each call. There may be multi base
     * directories configured by user, generally each base directory
     * represent a disk, allocated by round mode.
     *
     * For example, the base directories configured
     * ["/disk1/job_001/container_001", "/disk2/job_001/container_001"].
     * It indicates there are two base directories and each base directory
     * for one disk.
     * First call returns "/disk1/job_001/container_001",
     * second call returns "/disk2/job_001/container_001"
     * and third call returns "/disk1/job_001/container_001" and so on in
     * round mode.
     *
     * Note: Can't request a directory and write many files into it, this will
     *       cause the io pressure can't distributed over several disks.
     *
     * @return The directory of allocated local base directory.
     */
    String nextDirectory();

    /**
     * Allocate a base directory each call, return allocated base directory +
     * joined string of paths.
     *
     * @param paths The paths as sub-directory.
     * @return A string representation of a directory "#nextDirectory() +
     *         joined string of paths"
     */
    default String nextDirectory(String... paths) {
        return Paths.get(this.nextDirectory(), paths).toString();
    }

    default String randomDirectory(String... paths) {
        return Paths.get(this.nextDirectory(paths),
                         UUID.randomUUID().toString())
                    .toString();
    }

    List<String> dirs();
}
