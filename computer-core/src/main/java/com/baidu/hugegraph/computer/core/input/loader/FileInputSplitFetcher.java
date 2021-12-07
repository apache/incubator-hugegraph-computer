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

package com.baidu.hugegraph.computer.core.input.loader;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;

import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.input.InputSplit;
import com.baidu.hugegraph.computer.core.input.InputSplitFetcher;
import com.baidu.hugegraph.loader.constant.ElemType;
import com.baidu.hugegraph.loader.mapping.InputStruct;
import com.baidu.hugegraph.loader.mapping.LoadMapping;
import com.baidu.hugegraph.loader.source.file.FileSource;

public abstract class FileInputSplitFetcher implements InputSplitFetcher {

    private final Config config;
    private final List<InputStruct> vertexInputStructs;
    private final List<InputStruct> edgeInputStructs;

    public FileInputSplitFetcher(Config config) {
        this.config = config;
        String inputStructFile = this.config.get(
                                      ComputerOptions.INPUT_STRUCT_PATH);
        LoadMapping mapping = LoadMapping.of(inputStructFile);
        this.vertexInputStructs = new ArrayList<>();
        this.edgeInputStructs = new ArrayList<>();
        this.splitStructs(mapping.structs());
    }

    @Override
    public List<InputSplit> fetchVertexInputSplits() {
        List<InputSplit> splits = new ArrayList<>();
        for (InputStruct vertexInputStruct : this.vertexInputStructs) {
            FileSource source = (FileSource) vertexInputStruct.input();
            List<String> paths = this.scanPaths(source);
            if (CollectionUtils.isNotEmpty(paths)) {
                for (String path : paths) {
                    FileInputSplit split = new FileInputSplit(ElemType.VERTEX,
                                                              vertexInputStruct,
                                                              path);
                    splits.add(split);
                }
            }
        }
        return splits;
    }

    @Override
    public List<InputSplit> fetchEdgeInputSplits() {
        List<InputSplit> splits = new ArrayList<>();
        for (InputStruct edgeInputStruct : this.edgeInputStructs) {
            FileSource source = (FileSource) edgeInputStruct.input();
            List<String> paths = this.scanPaths(source);
            if (CollectionUtils.isNotEmpty(paths)) {
                for (String path : paths) {
                    FileInputSplit split = new FileInputSplit(ElemType.EDGE,
                                                              edgeInputStruct,
                                                              path);
                    splits.add(split);
                }
            }
        }
        return splits;
    }

    private void splitStructs(List<InputStruct> structs) {
        for (InputStruct struct : structs) {
            InputStruct result = struct.extractVertexStruct();
            if (result != InputStruct.EMPTY) {
                this.vertexInputStructs.add(result);
            }
        }
        for (InputStruct struct : structs) {
            InputStruct result = struct.extractEdgeStruct();
            if (result != InputStruct.EMPTY) {
                this.edgeInputStructs.add(result);
            }
        }
    }

    protected abstract List<String> scanPaths(FileSource source);

    @Override
    public void close() {
        // pass
    }
}
