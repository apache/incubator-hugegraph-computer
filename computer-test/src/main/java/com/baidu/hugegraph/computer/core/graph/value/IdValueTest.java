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

import com.baidu.hugegraph.computer.core.graph.id.LongId;
import com.baidu.hugegraph.computer.core.graph.id.Utf8Id;
import com.baidu.hugegraph.computer.core.graph.id.UuidId;
import com.baidu.hugegraph.computer.core.io.OptimizedUnsafeBytesInput;
import com.baidu.hugegraph.computer.core.io.OptimizedUnsafeBytesOutput;
import com.baidu.hugegraph.computer.core.io.UnsafeBytesInput;
import com.baidu.hugegraph.computer.core.io.UnsafeBytesOutput;
import com.baidu.hugegraph.computer.suite.unit.UnitTestBase;
import com.baidu.hugegraph.testutil.Assert;

public class IdValueTest extends UnitTestBase {

    @Test
    public void testType() {
        IdValue value1 = new IdValue();
        IdValue value2 = new IdValue(new byte[]{1, 2, 3});

        Assert.assertEquals(ValueType.ID_VALUE, value1.type());
        Assert.assertEquals(ValueType.ID_VALUE, value2.type());
    }

    @Test
    public void testValue() {
        IdValue value1 = new IdValue();
        IdValue value2 = new IdValue(new byte[]{1, 2, 3});
        IdValue value3 = new IdValue(new byte[]{'1', '2', '3'});
        IdValue value4 = new IdValue(new byte[]{1, 2, 3}, 2);

        Assert.assertArrayEquals(new byte[]{}, value1.bytes());
        Assert.assertArrayEquals(new byte[]{1, 2, 3}, value2.bytes());
        Assert.assertArrayEquals(new byte[]{'1', '2', '3'}, value3.bytes());
        Assert.assertArrayEquals(new byte[]{1, 2}, value4.bytes());
    }

    @Test
    public void testAssign() {
        IdValue value1 = new IdValue();
        IdValue value2 = new IdValue(new byte[]{1, 2, 3});
        IdValue value3 = new IdValue(new byte[]{'1', '2', '3'});

        Assert.assertArrayEquals(new byte[]{}, value1.bytes());
        value1.assign(value2);
        Assert.assertArrayEquals(new byte[]{1, 2, 3}, value1.bytes());
        Assert.assertArrayEquals(new byte[]{1, 2, 3}, value2.bytes());

        value2.assign(value3);
        Assert.assertArrayEquals(new byte[]{1, 2, 3}, value1.bytes());
        Assert.assertArrayEquals(new byte[]{'1', '2', '3'}, value2.bytes());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            @SuppressWarnings({ "unchecked", "rawtypes" })
            Value<IdValue> v = (Value) new FloatValue();
            value2.assign(v);
        }, e -> {
            Assert.assertContains("Can't assign '0.0'(FloatValue) to IdValue",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            @SuppressWarnings({ "unchecked", "rawtypes" })
            Value<IdValue> v = (Value) new LongValue();
            value2.assign(v);
        }, e -> {
            Assert.assertContains("Can't assign '0'(LongValue) to IdValue",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            value2.assign(null);
        }, e -> {
            Assert.assertContains("Can't assign null to IdValue",
                                  e.getMessage());
        });
    }

    @Test
    public void testCopy() {
        IdValue value1 = new IdValue(new byte[]{1, 2, 3});
        IdValue value2 = new IdValue(new byte[]{'1', '2', '3'});

        IdValue copy = value1.copy();
        Assert.assertArrayEquals(new byte[]{1, 2, 3}, value1.bytes());
        Assert.assertArrayEquals(new byte[]{1, 2, 3}, copy.bytes());

        copy.assign(value2);
        Assert.assertArrayEquals(new byte[]{'1', '2', '3'}, copy.bytes());
        Assert.assertArrayEquals(new byte[]{1, 2, 3}, value1.bytes());
    }

    @Test
    public void testReadWrite() throws IOException {
        assertValueEqualAfterWriteAndRead(new Utf8Id("text").idValue());
        assertValueEqualAfterWriteAndRead(new Utf8Id("text2").idValue());
        assertValueEqualAfterWriteAndRead(new LongId(123456L).idValue());
        assertValueEqualAfterWriteAndRead(new UuidId().idValue());
    }

    @Test
    public void testReadWriteUtf8IdValue() throws IOException {
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
    public void testCompare() {
        IdValue value1 = new LongId(123L).idValue();
        IdValue value2 = new LongId(123L).idValue();
        IdValue value3 = new LongId(321L).idValue();
        IdValue value4 = new LongId(322L).idValue();

        Assert.assertEquals(0, value1.compareTo(value2));
        Assert.assertLt(0, value2.compareTo(value3));
        Assert.assertGt(0, value3.compareTo(value1));
        Assert.assertLt(0, value3.compareTo(value4));

        IdValue value5 = new Utf8Id("123").idValue();
        IdValue value6 = new Utf8Id("456").idValue();

        Assert.assertLt(0, value5.compareTo(value6));
        Assert.assertGt(0, value6.compareTo(value5));

        Assert.assertGt(0, value5.compareTo(value1));
        Assert.assertGt(0, value6.compareTo(value1));

        Assert.assertLt(0, value1.compareTo(value5));
        Assert.assertLt(0, value1.compareTo(value6));
    }

    @Test
    public void testEquals() {
        IdValue value1 = new IdValue();
        Assert.assertTrue(value1.equals(value1));
        Assert.assertTrue(value1.equals(new IdValue(new byte[]{})));
        Assert.assertFalse(value1.equals(new IdValue(new byte[]{1})));

        Assert.assertFalse(value1.equals(new IntValue(1)));
        Assert.assertFalse(value1.equals(null));
    }

    @Test
    public void testHashCode() {
        IdValue value1 = new IdValue();
        IdValue value2 = new IdValue(new byte[]{1, 2, 3});
        IdValue value3 = new IdValue(new byte[]{'1', '2', '3'});

        Assert.assertEquals(1, value1.hashCode());
        Assert.assertEquals(30817, value2.hashCode());
        Assert.assertEquals(78481, value3.hashCode());
    }

    @Test
    public void testToString() {
        IdValue value1 = new IdValue();
        IdValue value2 = new IdValue(new byte[]{1, 2, 3});
        IdValue value3 = new IdValue(new byte[]{'1', '2', '3'});

        Assert.assertEquals("", value1.toString());
        Assert.assertEquals("010203", value2.toString());
        Assert.assertEquals("313233", value3.toString());
    }
}
