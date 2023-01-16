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

package org.apache.hugegraph.computer.core.input;

import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.input.hg.HugeGraphFetcher;
import org.apache.hugegraph.computer.core.input.hg.HugeInputSplitFetcher;
import org.apache.hugegraph.computer.core.input.loader.LoaderFileInputSplitFetcher;
import org.apache.hugegraph.computer.core.input.loader.LoaderGraphFetcher;
import org.apache.hugegraph.computer.core.rpc.InputSplitRpcService;

public class InputSourceFactory {

    public static InputSplitFetcher createInputSplitFetcher(Config config) {
        String type = config.get(ComputerOptions.INPUT_SOURCE_TYPE);
        switch (type) {
            case "hugegraph-server":
                return new HugeInputSplitFetcher(config);
            case "hugegraph-loader":
                return new LoaderFileInputSplitFetcher(config);
            default:
                throw new ComputerException("Unexpected source type %s", type);
        }
    }

    public static GraphFetcher createGraphFetcher(Config config,
                                                  InputSplitRpcService srv) {
        String type = config.get(ComputerOptions.INPUT_SOURCE_TYPE);
        switch (type) {
            case "hugegraph-server":
                return new HugeGraphFetcher(config, srv);
            case "hugegraph-loader":
                return new LoaderGraphFetcher(config, srv);
            default:
                throw new ComputerException("Unexpected source type %s", type);
        }
    }
}
