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

package com.baidu.hugegraph.computer.core.input;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.input.hg.HugeGraphFetcher;
import com.baidu.hugegraph.computer.core.input.hg.HugeInputSplitFetcher;
import com.baidu.hugegraph.computer.core.input.loader.LoaderFileInputSplitFetcher;
import com.baidu.hugegraph.computer.core.input.loader.LoaderGraphFetcher;
import com.baidu.hugegraph.computer.core.rpc.InputSplitRpcService;

public class InputSourceFactory {

    public static InputSplitFetcher createInputSplitFetcher(Config config) {
        String type = config.get(ComputerOptions.INPUT_SOURCE_TYPE);
        switch (type) {
            case "hugegraph":
                return new HugeInputSplitFetcher(config);
            case "loader":
                return new LoaderFileInputSplitFetcher(config);
            default:
                throw new ComputerException("Unexpected source type %s", type);
        }
    }

    public static GraphFetcher createGraphFetcher(Config config,
                                                  InputSplitRpcService srv) {
        String type = config.get(ComputerOptions.INPUT_SOURCE_TYPE);
        switch (type) {
            case "hugegraph":
                return new HugeGraphFetcher(config, srv);
            case "loader":
                return new LoaderGraphFetcher(config, srv);
            default:
                throw new ComputerException("Unexpected source type %s", type);
        }
    }
}
