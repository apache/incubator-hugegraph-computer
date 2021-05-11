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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.GraphFactory;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.id.IdFactory;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.value.ValueFactory;
import com.baidu.hugegraph.computer.core.io.GraphOutputFactory;
import com.baidu.hugegraph.computer.core.io.OutputFormat;
import com.baidu.hugegraph.computer.core.io.Readable;
import com.baidu.hugegraph.computer.core.io.StreamGraphInput;
import com.baidu.hugegraph.computer.core.io.StreamGraphOutput;
import com.baidu.hugegraph.computer.core.io.UnsafeBytesInput;
import com.baidu.hugegraph.computer.core.io.UnsafeBytesOutput;
import com.baidu.hugegraph.computer.core.io.Writable;
import com.baidu.hugegraph.computer.core.util.ComputerContextUtil;
import com.baidu.hugegraph.config.ConfigOption;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.util.E;

public class UnitTestBase {

    private static final String CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
                                        "0123456789" +
                                        "abcdefghijklmnopqrstuvxyz";

    public static void assertIdEqualAfterWriteAndRead(Id oldId)
                                                      throws IOException {
        byte[] bytes;
        try (UnsafeBytesOutput bao = new UnsafeBytesOutput()) {
            oldId.write(bao);
            bytes = bao.toByteArray();
        }

        Id newId = IdFactory.createId(oldId.type());
        try (UnsafeBytesInput bai = new UnsafeBytesInput(bytes)) {
            newId.read(bai);
            Assert.assertEquals(oldId, newId);
        }
    }

    public static void assertValueEqualAfterWriteAndRead(Value<?> oldValue)
                                                         throws IOException {
        byte[] bytes;
        try (UnsafeBytesOutput bao = new UnsafeBytesOutput()) {
            oldValue.write(bao);
            bytes = bao.toByteArray();
        }

        Value<?> newValue = valueFactory().createValue(oldValue.type());
        try (UnsafeBytesInput bai = new UnsafeBytesInput(bytes)) {
            newValue.read(bai);
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
            map.put(((ConfigOption<?>) key).name(), (String) value);
        }
        ComputerContextUtil.initContext(map);
    }

    public static synchronized Config updateWithRequiredOptions(
                                      Object... options) {
        Object[] requiredOptions = new Object[] {
            ComputerOptions.VALUE_NAME, "rank",
            ComputerOptions.EDGES_NAME, "value",
            ComputerOptions.VALUE_TYPE, "LONG"};
        Object[] allOptions = new Object[requiredOptions.length +
                                         options.length];
        System.arraycopy(requiredOptions, 0, allOptions, 0,
                         requiredOptions.length);
        System.arraycopy(options, 0, allOptions,
                         requiredOptions.length, options.length);
        UnitTestBase.updateOptions(allOptions);
        return ComputerContext.instance().config();
    }

    public static void assertEqualAfterWriteAndRead(Writable writeObj,
                                                    Readable readObj)
                                                    throws IOException {
        byte[] bytes;
        try (UnsafeBytesOutput bao = new UnsafeBytesOutput()) {
            writeObj.write(bao);
            bytes = bao.toByteArray();
        }

        try (UnsafeBytesInput bai = new UnsafeBytesInput(bytes)) {
            readObj.read(bai);
            Assert.assertEquals(writeObj, readObj);
        }
    }

    public static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) {
            // Ignore InterruptedException
        }
    }

    public static byte[] randomBytes(int size) {
        Random random = new Random();
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++) {
            bytes[i] = (byte) random.nextInt();
        }
        return bytes;
    }

    public static String randomString(int size) {
        Random random = new Random();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size; i++) {
            sb.append(CHARS.charAt(random.nextInt(CHARS.length())));
        }
        return sb.toString();
    }

    protected static ComputerContext context() {
        return ComputerContext.instance();
    }

    protected static ValueFactory valueFactory() {
        return context().valueFactory();
    }

    protected static GraphFactory graphFactory() {
        return context().graphFactory();
    }

    protected static StreamGraphInput newStreamGraphInput(
                                      UnsafeBytesInput bai) {
        return new StreamGraphInput(context(), bai);
    }

    protected static StreamGraphOutput newStreamGraphOutput(
                                       UnsafeBytesOutput bao) {
        return (StreamGraphOutput) GraphOutputFactory.create(context(),
                                                             OutputFormat.BIN,
                                                             bao);
    }
}
