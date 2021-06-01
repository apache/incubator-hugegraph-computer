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

package com.baidu.hugegraph.computer.core.combiner;

import java.io.IOException;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.graph.properties.Properties;
import com.baidu.hugegraph.computer.core.io.GraphInput;
import com.baidu.hugegraph.computer.core.io.GraphOutput;
import com.baidu.hugegraph.computer.core.io.OptimizedUnsafeBytesOutput;
import com.baidu.hugegraph.computer.core.io.StreamGraphInput;
import com.baidu.hugegraph.computer.core.io.StreamGraphOutput;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.InlinePointer;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.Pointer;

public class PropertiesPointerCombiner implements Combiner<Pointer> {

    private Properties properties1;
    private Properties properties2;
    private Combiner<Properties> propertiesCombiner;
    private OptimizedUnsafeBytesOutput bytesOutput;
    private GraphOutput graphOutput;
    private ComputerContext context;

    public PropertiesPointerCombiner(Properties properties1,
                                     Properties properties2,
                                     Combiner<Properties> propertiesCombiner) {
        this.properties1 = properties1;
        this.properties2 = properties2;
        this.propertiesCombiner = propertiesCombiner;
        this.bytesOutput = new OptimizedUnsafeBytesOutput();
        this.context = ComputerContext.instance();
        this.graphOutput = new StreamGraphOutput(this.context,
                                                 this.bytesOutput);
    }

    @Override
    public Pointer combine(Pointer v1, Pointer v2) {
        try {
            this.properties1.read(v1.input());
            this.properties2.read(v2.input());
            Properties combinedProperties = this.propertiesCombiner.combine(
                                            this.properties1, this.properties2);
            this.bytesOutput.seek(0L);
            this.graphOutput.writeProperties(combinedProperties);
            return new InlinePointer(this.bytesOutput.buffer(),
                                     this.bytesOutput.position());
        } catch (IOException e) {
            throw new ComputerException(
                      "Failed to combine pointer1(offset=%s, length=%s) and" +
                      " pointer2(offset=%s, length=%s)'",
                      e, v1.offset(), v1.length(), v2.offset(), v2.length());
        }
    }
}
