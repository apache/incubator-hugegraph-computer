/*
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

package org.apache.hugegraph.computer.core.graph.value;

import java.io.IOException;
import java.util.UUID;

import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.computer.core.graph.id.BytesId;
import org.apache.hugegraph.computer.core.graph.id.IdType;
import org.apache.hugegraph.computer.core.io.BytesInput;
import org.apache.hugegraph.computer.core.io.BytesOutput;
import org.apache.hugegraph.computer.core.io.IOFactory;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class IdValueTest extends UnitTestBase {

    @Test
    public void testType() {
        Value value1 = new BytesId();
        Value value2 = new BytesId(IdType.LONG, new byte[]{1, 2, 3});
        Value value3 = BytesId.of(1L);
        Value value4 = BytesId.of("1");

        Assert.assertEquals(ValueType.ID, value1.valueType());
        Assert.assertEquals(ValueType.ID, value2.valueType());
        Assert.assertEquals(ValueType.ID, value3.valueType());
        Assert.assertEquals(ValueType.ID, value4.valueType());
    }

    @Test
    public void testAssign() {
        Value value1 = new BytesId();
        Value value2 = new BytesId(IdType.LONG, new byte[]{1, 2, 3});
        Value value3 = new BytesId(IdType.LONG, new byte[]{'1', '2', '3'});

        value1.assign(value2);
        Assert.assertEquals(value2, value1);

        value2.assign(value3);
        Assert.assertEquals(value3, value2);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Value v = new FloatValue();
            value2.assign(v);
        }, e -> {
            Assert.assertContains("Can't assign '0.0'(FloatValue) to BytesId",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Value v = new LongValue();
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
        Value value1 = new BytesId(IdType.LONG, new byte[]{1, 2, 3});
        Value value2 = new BytesId(IdType.LONG, new byte[]{'1', '2', '3'});

        Value copy = value1.copy();
        Assert.assertEquals(value1, copy);

        copy.assign(value2);
        Assert.assertEquals(value2, copy);
    }

    @Test
    public void testValue() {
        Value value1 = BytesId.of(1234L);
        Value value2 = BytesId.of("1234");
        Value value3 = BytesId.of("12345");
        Value value4 = new BytesId();

        Assert.assertEquals(1234L, value1.value());
        Assert.assertEquals("1234", value2.value());
        Assert.assertEquals("12345", value3.value());
        Assert.assertEquals("", value4.value());
    }

    @Test
    public void testString() {
        Value value1 = BytesId.of(1234L);
        Value value2 = BytesId.of("1234");
        Value value3 = BytesId.of("12345");
        Value value4 = new BytesId();

        Assert.assertEquals("1234", value1.string());
        Assert.assertEquals("1234", value2.string());
        Assert.assertEquals("12345", value3.string());
        Assert.assertEquals("", value4.string());
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
        Value value1 = BytesId.of("long id");
        Value value2 = BytesId.of("short");
        byte[] bytes;
        try (BytesOutput bao = IOFactory.createBytesOutput(
                Constants.SMALL_BUF_SIZE)) {
            value1.write(bao);
            value2.write(bao);
            bytes = bao.toByteArray();
        }
        Value value3 = BytesId.of(Constants.EMPTY_STR);
        try (BytesInput bai = IOFactory.createBytesInput(bytes)) {
            value3.read(bai);
            Assert.assertEquals(value1, value3);
            value3.read(bai);
            Assert.assertEquals(value2, value3);
        }
    }

    @Test
    public void testCompare() {
        Value value1 = BytesId.of(123L);
        Value value2 = BytesId.of(123L);
        Value value3 = BytesId.of(321L);
        Value value4 = BytesId.of(322L);

        Assert.assertEquals(0, value1.compareTo(value2));
        Assert.assertLt(0, value2.compareTo(value3));
        Assert.assertGt(0, value3.compareTo(value1));
        Assert.assertLt(0, value3.compareTo(value4));

        Value value5 = BytesId.of("123");
        Value value6 = BytesId.of("456");

        Assert.assertLt(0, value5.compareTo(value6));
        Assert.assertGt(0, value6.compareTo(value5));

        Assert.assertGt(0, value5.compareTo(value1));
        Assert.assertGt(0, value6.compareTo(value1));

        Assert.assertLt(0, value1.compareTo(value5));
        Assert.assertLt(0, value1.compareTo(value6));

        Assert.assertGt(0, value1.compareTo(NullValue.get()));
        Assert.assertGt(0, value1.compareTo(new BooleanValue()));
        Assert.assertGt(0, value1.compareTo(new IntValue(123)));
        Assert.assertGt(0, value1.compareTo(new FloatValue(123)));
        Assert.assertGt(0, value1.compareTo(new DoubleValue(123)));
        Assert.assertGt(0, value1.compareTo(new StringValue("123")));
        Assert.assertLt(0, value1.compareTo(new ListValue<>(ValueType.INT)));
    }

    @Test
    public void testEquals() {
        Value value1 = new BytesId();
        Assert.assertEquals(value1, value1);
        Assert.assertEquals(value1, new BytesId());
        Assert.assertNotEquals(value1,
                               new BytesId(IdType.LONG, new byte[] {1}));

        Assert.assertNotEquals(value1, new IntValue(1));
        Assert.assertNotEquals(null, value1);
    }

    @Test
    public void testHashCode() {
        Value value1 = new BytesId();
        Value value2 = new BytesId(IdType.LONG, new byte[]{1, 2, 3});
        Value value3 = new BytesId(IdType.LONG, new byte[]{'1', '2', '3'});

        Assert.assertNotEquals(value1.hashCode(), value2.hashCode());
        Assert.assertNotEquals(value1.hashCode(), value3.hashCode());
        Assert.assertNotEquals(value2.hashCode(), value3.hashCode());
    }

    @Test
    public void testToString() {
        Value value1 = new BytesId();
        Value value2 = BytesId.of(123);
        Value value3 = BytesId.of("123");

        Assert.assertEquals("", value1.toString());
        Assert.assertEquals("123", value2.toString());
        Assert.assertEquals("123", value3.toString());
    }
}
