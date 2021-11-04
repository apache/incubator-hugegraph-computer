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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class SuperstepFileGenerator {

    private final FileGenerator fileGenerator;
    private final int superstep;

    public SuperstepFileGenerator(FileGenerator fileGenerator,
                                  int superstep) {
        this.fileGenerator = fileGenerator;
        this.superstep = superstep;
    }

    public String nextPath(String type) {
        String[] paths = {type, Integer.toString(this.superstep),
                          UUID.randomUUID().toString()};
        return this.fileGenerator.nextDirectory(paths);
    }

    public List<String> preStepDirs(String type) {
        List<String> preStepDirs = new ArrayList<>();
        String[] paths = {type, Integer.toString(this.superstep - 1)};
        for (String dir : this.fileGenerator.dirs()) {
            preStepDirs.add(Paths.get(dir, paths).toString());
        }
        return preStepDirs;
    }

    public List<String> curStepDirs(String type) {
        List<String> curStepDirs = new ArrayList<>();
        String[] paths = {type, Integer.toString(this.superstep)};
        for (String dir : this.fileGenerator.dirs()) {
            curStepDirs.add(Paths.get(dir, paths).toString());
        }
        return curStepDirs;
    }

    public List<String> superstepDirs(int superstep, String type) {
        List<String> superstepDirs = new ArrayList<>();
        String[] paths = {type, Integer.toString(superstep)};
        for (String dir : this.fileGenerator.dirs()) {
            superstepDirs.add(Paths.get(dir, paths).toString());
        }
        return superstepDirs;
    }
}
