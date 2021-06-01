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

package com.baidu.hugegraph.computer.core.io;

import java.io.IOException;

import org.apache.commons.lang3.NotImplementedException;
import org.junit.Test;

import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.graph.id.LongId;
import com.baidu.hugegraph.computer.core.graph.value.LongValue;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.suite.unit.UnitTestBase;
import com.baidu.hugegraph.testutil.Assert;

public class StreamGraphOutputInputTest extends UnitTestBase {

    @Test
    public void testWriteReadVertex() throws IOException {
        UnitTestBase.updateOptions(
            ComputerOptions.VALUE_TYPE, "LONG",
            ComputerOptions.VALUE_NAME, "value"
        );

        LongId longId = new LongId(100L);
        LongValue longValue = new LongValue(999L);
        Vertex vertex1 = graphFactory().createVertex(longId, longValue);
        byte[] bytes;
        try (UnsafeBytesOutput bao = new UnsafeBytesOutput()) {
            StreamGraphOutput output = newStreamGraphOutput(bao);
            output.writeVertex(vertex1);
            bytes = bao.toByteArray();
        }

        Assert.assertThrows(NotImplementedException.class, () -> {
            try (UnsafeBytesInput bai = new UnsafeBytesInput(bytes)) {
                StreamGraphInput input = newStreamGraphInput(bai);
                input.readVertex();
            }
        });
    }
}
