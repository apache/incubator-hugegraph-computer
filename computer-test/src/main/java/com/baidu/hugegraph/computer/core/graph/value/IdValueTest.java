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
import com.baidu.hugegraph.computer.core.io.OptimizedUnsafeBytesInput;
import com.baidu.hugegraph.computer.core.io.OptimizedUnsafeBytesOutput;
import com.baidu.hugegraph.computer.core.io.StreamGraphInput;
import com.baidu.hugegraph.computer.core.io.StreamGraphOutput;
import com.baidu.hugegraph.computer.core.io.UnsafeBytesInput;
import com.baidu.hugegraph.computer.core.io.UnsafeBytesOutput;
import com.baidu.hugegraph.computer.core.util.IdValueUtil;
import com.baidu.hugegraph.testutil.Assert;

public class IdValueTest extends UnitTestBase {

    @Test
    public void testCompare() {
        IdValue value1 = new LongId(123L).idValue();
        IdValue value2 = new LongId(123L).idValue();
        IdValue value3 = new LongId(321L).idValue();
        IdValue value4 = new LongId(322L).idValue();
        Assert.assertEquals(0, value1.compareTo(value2));
        Assert.assertLt(0, value2.compareTo(value3));
        Assert.assertGt(0, value3.compareTo(value1));
        Assert.assertLt(0, value3.compareTo(value4));
    }

    @Test
    public void testBytes() throws IOException {
        IdValue value1 = new Utf8Id("long id").idValue();
        IdValue value2 = new Utf8Id("short").idValue();
        byte[] bytes;
        try (UnsafeBytesOutput bao = new OptimizedUnsafeBytesOutput()) {
            value1.write(bao);
            value2.write(bao);
            bytes = bao.toByteArray();
        }
        IdValue value3 = new Utf8Id().idValue();
        try (UnsafeBytesInput bai = new OptimizedUnsafeBytesInput(bytes)) {
            value3.read(bai);
            Assert.assertEquals(value1, value3);
            value3.read(bai);
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
        try (UnsafeBytesOutput bao = new UnsafeBytesOutput();
             StreamGraphOutput output = newStreamGraphOutput(bao)) {
            output.writeId(IdValueUtil.toId(value1));
            output.writeId(IdValueUtil.toId(value2));
            bytes = bao.toByteArray();
        }
        try (UnsafeBytesInput bai = new UnsafeBytesInput(bytes);
             StreamGraphInput input = newStreamGraphInput(bai)) {
            Id id3 = input.readId();
            Assert.assertEquals(id1, id3);
            Id id4 = input.readId();
            Assert.assertEquals(id2, id4);
        }
        Assert.assertEquals("02076c6f6e67206964", value1.toString());
    }
}
