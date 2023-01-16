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

package org.apache.hugegraph.computer.algorithm;

import static org.apache.hugegraph.computer.core.config.ComputerOptions.COMPUTER_PROHIBIT_USER_OPTIONS;

import java.util.Map;

import org.apache.hugegraph.config.ConfigOption;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public interface AlgorithmParams {

    Logger LOG = Log.logger(AlgorithmParams.class);

    String BYTESID_CLASS_NAME = "org.apache.hugegraph.computer.core.graph.id.BytesId";
    String HDFS_OUTPUT_CLASS_NAME = "org.apache.hugegraph.computer.core.output.hdfs.HdfsOutput";
    String HUGEGRAPH_ID_OUTPUT_CLASS_NAME = "org.apache.hugegraph.computer.core" +
                                            ".output.hg.HugeGraphIdOutput";
    String LOG_OUTPUT_CLASS_NAME = "org.apache.hugegraph.computer.core.output.LogOutput";
    String HUGEGRAPH_DOUBLE_OUTPUT_CLASS_NAME = "org.apache.hugegraph.computer.core" +
                                                ".output.hg.HugeGraphDoubleOutput";
    String HUGEGRAPH_FLOAT_OUTPUT_CLASS_NAME = "org.apache.hugegraph.computer.core" +
                                               ".output.hg.HugeGraphFloatOutput";
    String HUGEGRAPH_INT_OUTPUT_CLASS_NAME = "org.apache.hugegraph.computer.core" +
                                             ".output.hg.HugeGraphIntOutput";
    String HUGEGRAPH_LONG_OUTPUT_CLASS_NAME = "org.apache.hugegraph.computer.core" +
                                              ".output.hg.HugeGraphLongOutput";
    String HUGEGRAPH_STRING_OUTPUT_CLASS_NAME = "org.apache.hugegraph.computer.core" +
                                                ".output.hg.HugeGraphStringOutput";
    String HUGEGRAPH_LIST_OUTPUT_CLASS_NAME = "org.apache.hugegraph.computer.core" +
                                                ".output.hg.HugeGraphListOutput";
    String DEFAULTINPUTFILTER_CLASS_NAME = "org.apache.hugegraph.computer" +
                                           ".core.input.filter" +
                                           ".DefaultInputFilter";
    String EXTRACTALLPROPERTYINPUTFILTER_CLASS_NAME = "org.apache.hugegraph.computer" +
                                                      ".core.input.filter" +
                                                      ".ExtractAllPropertyInputFilter";

    /**
     * set algorithm's specific configuration
     * @param params
     */
    void setAlgorithmParameters(Map<String, String> params);

    default void setIfAbsent(Map<String, String> params, String key,
                             String value) {
        if (!params.containsKey(key) && !COMPUTER_PROHIBIT_USER_OPTIONS.contains(key)) {
            LOG.debug("Put parameters key={}, value={}", key, value);
            params.put(key, value);
        }
    }

    default void setIfAbsent(Map<String, String> params,
                             ConfigOption<?> keyOption,
                             String value) {
        this.setIfAbsent(params, keyOption.name(), value);
    }
}
