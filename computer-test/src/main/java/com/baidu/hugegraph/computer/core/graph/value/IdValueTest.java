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
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.id.IdType;
import com.baidu.hugegraph.computer.core.io.BytesInput;
import com.baidu.hugegraph.computer.core.io.BytesOutput;
import com.baidu.hugegraph.computer.core.io.IOFactory;
import com.baidu.hugegraph.computer.suite.unit.UnitTestBase;
import com.baidu.hugegraph.testutil.Assert;

public class IdValueTest extends UnitTestBase {

    @Test
    public void testType() {
        Value<Id> value1 = BytesId.of();
        Value<Id> value2 = new BytesId(IdType.LONG, new byte[]{1, 2, 3});

        Assert.assertEquals(ValueType.ID_VALUE, value1.type());
        Assert.assertEquals(ValueType.ID_VALUE, value2.type());
    }

    @Test
    public void testAssign() {
        Value<Id> value1 = BytesId.of();
        Value<Id> value2 = new BytesId(IdType.LONG, new byte[]{1, 2, 3});
        Value<Id> value3 = new BytesId(IdType.LONG, new byte[]{'1', '2', '3'});

        value1.assign(value2);
        Assert.assertEquals(value2, value1);

        value2.assign(value3);
        Assert.assertEquals(value3, value2);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            @SuppressWarnings({ "unchecked", "rawtypes" })
            Value<Id> v = (Value) new FloatValue();
            value2.assign(v);
        }, e -> {
            Assert.assertContains("Can't assign '0.0'(FloatValue) to BytesId",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            @SuppressWarnings({ "unchecked", "rawtypes" })
            Value<Id> v = (Value) new LongValue();
            value2.assign(v);
        }, e -> {
            Assert.assertContains("Can't assign '0'(LongValue) to BytesId",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            value2.assign(null);
        }, e -> {
            Assert.assertContains("Can't assign null to BytesId",
                                  e.getMessage());
        });
    }

    @Test
    public void testCopy() {
        Value<Id> value1 = new BytesId(IdType.LONG, new byte[]{1, 2, 3});
        Value<Id> value2 = new BytesId(IdType.LONG, new byte[]{'1', '2', '3'});

        Value<Id> copy = value1.copy();
        Assert.assertEquals(value1, copy);

        copy.assign(value2);
        Assert.assertEquals(value2, copy);
    }

    @Test
    public void testReadWrite() throws IOException {
        assertValueEqualAfterWriteAndRead(BytesId.of("text"));
        assertValueEqualAfterWriteAndRead(BytesId.of("text2"));
        assertValueEqualAfterWriteAndRead(BytesId.of(123456L));
        assertValueEqualAfterWriteAndRead(BytesId.of(new UUID(0L, 0L)));
    }

    @Test
    public void testReadWriteUtf8IdValue() throws IOException {
        Value<Id> value1 = BytesId.of("long id");
        Value<Id> value2 = BytesId.of("short");
        byte[] bytes;
        try (BytesOutput bao = IOFactory.createBytesOutput(
                               Constants.SMALL_BUF_SIZE)) {
            value1.write(bao);
            value2.write(bao);
            bytes = bao.toByteArray();
        }
        Value<Id> value3 = BytesId.of(Constants.EMPTY_STR);
        try (BytesInput bai = IOFactory.createBytesInput(bytes)) {
            value3.read(bai);
            Assert.assertEquals(value1, value3);
            value3.read(bai);
            Assert.assertEquals(value2, value3);
        }
    }

    @Test
    public void testCompare() {
        Value<Id> value1 = BytesId.of(123L);
        Value<Id> value2 = BytesId.of(123L);
        Value<Id> value3 = BytesId.of(321L);
        Value<Id> value4 = BytesId.of(322L);

        Assert.assertEquals(0, value1.compareTo((Id) value2));
        Assert.assertLt(0, value2.compareTo((Id) value3));
        Assert.assertGt(0, value3.compareTo((Id) value1));
        Assert.assertLt(0, value3.compareTo((Id) value4));

        Value<Id> value5 = BytesId.of("123");
        Value<Id> value6 = BytesId.of("456");

        Assert.assertLt(0, value5.compareTo((Id) value6));
        Assert.assertGt(0, value6.compareTo((Id) value5));

        Assert.assertGt(0, value5.compareTo((Id) value1));
        Assert.assertGt(0, value6.compareTo((Id) value1));

        Assert.assertLt(0, value1.compareTo((Id) value5));
        Assert.assertLt(0, value1.compareTo((Id) value6));
    }

    @Test
    public void testEquals() {
        Value<Id> value1 = BytesId.of();
        Assert.assertEquals(value1, value1);
        Assert.assertEquals(value1, BytesId.of());
        Assert.assertNotEquals(value1,
                               new BytesId(IdType.LONG, new byte[] {1}));

        Assert.assertNotEquals(value1, new IntValue(1));
        Assert.assertNotEquals(null, value1);
    }

    @Test
    public void testHashCode() {
        Value<Id> value1 = BytesId.of();
        Value<Id> value2 = new BytesId(IdType.LONG, new byte[]{1, 2, 3});
        Value<Id> value3 = new BytesId(IdType.LONG, new byte[]{'1', '2', '3'});

        Assert.assertNotEquals(value1.hashCode(), value2.hashCode());
        Assert.assertNotEquals(value1.hashCode(), value3.hashCode());
        Assert.assertNotEquals(value2.hashCode(), value3.hashCode());
    }

    @Test
    public void testToString() {
        Value<Id> value1 = BytesId.of();
        Value<Id> value2 = BytesId.of(123);
        Value<Id> value3 = BytesId.of("123");

        Assert.assertEquals("0", value1.toString());
        Assert.assertEquals("123", value2.toString());
        Assert.assertEquals("123", value3.toString());
    }
}
