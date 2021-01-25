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
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.output.ByteArrayOutputStream;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.id.IdFactory;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.value.ValueFactory;
import com.baidu.hugegraph.computer.core.io.OptimizedStreamGraphInput;
import com.baidu.hugegraph.computer.core.io.OptimizedStreamGraphOutput;
import com.baidu.hugegraph.computer.core.io.Readable;
import com.baidu.hugegraph.computer.core.io.StreamGraphInput;
import com.baidu.hugegraph.computer.core.io.StreamGraphOutput;
import com.baidu.hugegraph.config.ConfigOption;
import com.baidu.hugegraph.computer.core.io.Writable;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.util.E;

public class UnitTestBase {

    public static void assertIdEqualAfterWriteAndRead(Id oldId)
                                                      throws IOException {
        byte[] bytes;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            StreamGraphOutput output = new OptimizedStreamGraphOutput(baos);
            oldId.write(output);
            bytes = baos.toByteArray();
        }

        Id newId = IdFactory.createID(oldId.type());
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes)) {
            StreamGraphInput input = new OptimizedStreamGraphInput(bais);
            newId.read(input);
            Assert.assertEquals(oldId, newId);
        }
    }

    public static void assertValueEqualAfterWriteAndRead(Value oldValue)
                                                         throws IOException {
        byte[] bytes;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            StreamGraphOutput output = new OptimizedStreamGraphOutput(baos);
            oldValue.write(output);
            bytes = baos.toByteArray();
        }

        Value newValue = ValueFactory.createValue(oldValue.type());
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes)) {
            StreamGraphInput input = new OptimizedStreamGraphInput(bais);
            newValue.read(input);
            Assert.assertEquals(oldValue, newValue);
        }
    }

    public static void updateOptions(Object... optionKeyValues) {
        if (optionKeyValues == null || optionKeyValues.length == 0) {
            throw new ComputerException("Options can't be null or empty");
        }
        if ((optionKeyValues.length & 0x01) == 1) {
            throw new ComputerException("Options length must be even");
        }
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < optionKeyValues.length; i += 2) {
            Object key = optionKeyValues[i];
            E.checkArgument(key instanceof ConfigOption,
                            "The option key must be ConfigOption class");
            Object value = optionKeyValues[i + 1];
            E.checkArgument(value instanceof String,
                            "The option value must be String class");
            map.put(((ConfigOption) key).name(), (String) value);
        }
        ComputerContext.updateOptions(map);
    }

    public static void assertEqualAfterWriteAndRead(Writable writeObj,
                                                    Readable readObj)
                                                    throws IOException {
        byte[] bytes;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            StreamGraphOutput output = new OptimizedStreamGraphOutput(baos);
            writeObj.write(output);
            bytes = baos.toByteArray();
        }

        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes)) {
            StreamGraphInput input = new OptimizedStreamGraphInput(bais);
            readObj.read(input);
            Assert.assertEquals(writeObj, readObj);
        }
    }
}
