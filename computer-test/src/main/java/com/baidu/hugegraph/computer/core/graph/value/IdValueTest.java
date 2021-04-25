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

package com.baidu.hugegraph.computer.core.graph.value;

import java.io.IOException;

import org.junit.Test;

import com.baidu.hugegraph.computer.core.UnitTestBase;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.id.LongId;
import com.baidu.hugegraph.computer.core.graph.id.Utf8Id;
import com.baidu.hugegraph.computer.core.io.StreamGraphInput;
import com.baidu.hugegraph.computer.core.io.StreamGraphOutput;
import com.baidu.hugegraph.computer.core.io.UnsafeByteArrayInput;
import com.baidu.hugegraph.computer.core.io.UnsafeByteArrayOutput;
import com.baidu.hugegraph.testutil.Assert;

public class IdValueTest extends UnitTestBase {

    @Test
    public void testCompare() {
        IdValue value1 = new LongId(123L).idValue();
        IdValue value2 = new LongId(123L).idValue();
        IdValue value3 = new LongId(321L).idValue();
        Assert.assertEquals(0, value1.compareTo(value2));
        Assert.assertLt(0, value2.compareTo(value3));
        Assert.assertGt(0, value3.compareTo(value1));
    }

    @Test
    public void testBytes() throws IOException {
        IdValue value1 = new Utf8Id("long id").idValue();
        IdValue value2 = new Utf8Id("short").idValue();
        byte[] bytes;
        try (UnsafeByteArrayOutput bao = new UnsafeByteArrayOutput();
             StreamGraphOutput output = newOptimizedStreamGraphOutput(bao)) {
            value1.write(output);
            value2.write(output);
            bytes = bao.toByteArray();
        }
        IdValue value3 = new Utf8Id().idValue();
        try (UnsafeByteArrayInput bai = new UnsafeByteArrayInput(bytes);
             StreamGraphInput input = newOptimizedStreamGraphInput(bai)) {
            value3.read(input);
            Assert.assertEquals(value1, value3);
            value3.read(input);
            Assert.assertEquals(value2, value3);
        }
    }

    @Test
    public void testWriteId() throws IOException {
        Id id1 = new Utf8Id("long id");
        Id id2 = new Utf8Id("short");
        IdValue value1 = id1.idValue();
        IdValue value2 = id2.idValue();
        byte[] bytes;
        try (UnsafeByteArrayOutput bao = new UnsafeByteArrayOutput();
             StreamGraphOutput output = newOptimizedStreamGraphOutput(bao)) {
            value1.writeId(output);
            value2.writeId(output);
            bytes = bao.toByteArray();
        }
        try (UnsafeByteArrayInput bai = new UnsafeByteArrayInput(bytes);
             StreamGraphInput input = newOptimizedStreamGraphInput(bai)) {
            Id id3 = input.readId();
            Assert.assertEquals(id1, id3);
            Id id4 = input.readId();
            Assert.assertEquals(id2, id4);
        }
        Assert.assertEquals("02076c6f6e67206964", value1.toString());
    }
}
