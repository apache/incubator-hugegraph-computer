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

package com.baidu.hugegraph.computer.core;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.commons.io.output.ByteArrayOutputStream;

import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.id.IdFactory;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.value.ValueFactory;
import com.baidu.hugegraph.computer.core.io.GraphInput;
import com.baidu.hugegraph.computer.core.io.GraphOutput;
import com.baidu.hugegraph.computer.core.io.StreamGraphInput;
import com.baidu.hugegraph.computer.core.io.StreamGraphOutput;
import com.baidu.hugegraph.testutil.Assert;

public class BaseCoreTest {

    public static void assertIdEqualAfterWriteAndRead(Id oldId)
                                                      throws IOException {
        byte[] bytes;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            GraphOutput output = new StreamGraphOutput(baos);
            oldId.write(output);
            bytes = baos.toByteArray();
        }

        Id newId = IdFactory.createID(oldId.type());
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes)) {
            GraphInput input = new StreamGraphInput(bais);
            newId.read(input);
            Assert.assertEquals(oldId, newId);
        }
    }

    public static void assertValueEqualAfterWriteAndRead(Value oldValue)
                                                         throws IOException {
        byte[] bytes;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            GraphOutput output = new StreamGraphOutput(baos);
            oldValue.write(output);
            bytes = baos.toByteArray();
        }

        Value newValue = ValueFactory.createValue(oldValue.cardinality(),
                                                  oldValue.type());
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes)) {
            GraphInput input = new StreamGraphInput(bais);
            newValue.read(input);
            Assert.assertEquals(oldValue, newValue);
        }
    }
}
