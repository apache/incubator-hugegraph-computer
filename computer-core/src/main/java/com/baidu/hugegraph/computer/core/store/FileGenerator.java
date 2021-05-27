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

public interface FileGenerator {

    /**
     * @return the next data directory to persist data like vertices, edges and
     * messages. If pass parentDirectories such as ["message", "1"], it
     * return directory end with message/1.
     */
    File nextDir(String... parentDirectories);

    /**
     * @return the next file to persist data like vertices, edges and messages.
     * The returned file is unique.
     */
    File nextFile(String... parentDirectories);
}
