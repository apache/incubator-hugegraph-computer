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
import java.util.UUID;

import org.junit.Test;

import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.computer.core.graph.id.BytesId;
import com.baidu.hugegraph.computer.core.io.BytesInput;
import com.baidu.hugegraph.computer.core.io.BytesOutput;
import com.baidu.hugegraph.computer.core.io.IOFactory;
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
        assertValueEqualAfterWriteAndRead(BytesId.of("text").idValue());
        assertValueEqualAfterWriteAndRead(BytesId.of("text2").idValue());
        assertValueEqualAfterWriteAndRead(BytesId.of(123456L).idValue());
        assertValueEqualAfterWriteAndRead(BytesId.of(new UUID(0L, 0L))
                                                 .idValue());
    }

    @Test
    public void testReadWriteUtf8IdValue() throws IOException {
        IdValue value1 = BytesId.of("long id").idValue();
        IdValue value2 = BytesId.of("short").idValue();
        byte[] bytes;
        try (BytesOutput bao = IOFactory.createBytesOutput(
                               Constants.SMALL_BUF_SIZE)) {
            value1.write(bao);
            value2.write(bao);
            bytes = bao.toByteArray();
        }
        IdValue value3 = BytesId.of(Constants.EMPTY_STR).idValue();
        try (BytesInput bai = IOFactory.createBytesInput(bytes)) {
            value3.read(bai);
            Assert.assertEquals(value1, value3);
            value3.read(bai);
            Assert.assertEquals(value2, value3);
        }
    }

    @Test
    public void testCompare() {
        IdValue value1 = BytesId.of(123L).idValue();
        IdValue value2 = BytesId.of(123L).idValue();
        IdValue value3 = BytesId.of(321L).idValue();
        IdValue value4 = BytesId.of(322L).idValue();

        Assert.assertEquals(0, value1.compareTo(value2));
        Assert.assertLt(0, value2.compareTo(value3));
        Assert.assertGt(0, value3.compareTo(value1));
        Assert.assertLt(0, value3.compareTo(value4));

        IdValue value5 = BytesId.of("123").idValue();
        IdValue value6 = BytesId.of("456").idValue();

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
