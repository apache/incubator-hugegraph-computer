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

import com.baidu.hugegraph.computer.suite.unit.UnitTestBase;
import com.baidu.hugegraph.testutil.Assert;

public class LongValueTest extends UnitTestBase {

    @Test
    public void testType() {
        LongValue value1 = new LongValue();
        LongValue value2 = new LongValue(Long.MIN_VALUE);

        Assert.assertEquals(ValueType.LONG, value1.type());
        Assert.assertEquals(ValueType.LONG, value2.type());
    }

    @Test
    public void testValue() {
        LongValue value1 = new LongValue();
        LongValue value2 = new LongValue(123456L);
        LongValue value3 = new LongValue(Long.MIN_VALUE);
        LongValue value4 = new LongValue(Long.MAX_VALUE);

        Assert.assertEquals(0L, value1.value());
        Assert.assertEquals(123456L, value2.value());
        Assert.assertEquals(Long.MIN_VALUE, value3.value());
        Assert.assertEquals(Long.MAX_VALUE, value4.value());

        value3.value(Long.MAX_VALUE);
        Assert.assertEquals(Long.MAX_VALUE, value3.value());
        Assert.assertEquals(value3, value4);

        LongValue value5 = new LongValue(value2.value());
        Assert.assertEquals(value2, value5);
    }

    @Test
    public void testAssign() {
        LongValue value1 = new LongValue();
        LongValue value2 = new LongValue(123456L);
        LongValue value3 = new LongValue(Long.MIN_VALUE);
        LongValue value4 = new LongValue(Long.MAX_VALUE);

        Assert.assertEquals(0L, value1.value());
        value1.assign(value2);
        Assert.assertEquals(123456L, value1.value());
        Assert.assertEquals(123456L, value2.value());

        value2.assign(value3);
        Assert.assertEquals(123456L, value1.value());
        Assert.assertEquals(Long.MIN_VALUE, value2.value());

        value2.assign(value4);
        Assert.assertEquals(123456L, value1.value());
        Assert.assertEquals(Long.MAX_VALUE, value2.value());
        Assert.assertEquals(Long.MIN_VALUE, value3.value());
        Assert.assertEquals(Long.MAX_VALUE, value4.value());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            @SuppressWarnings({ "unchecked", "rawtypes" })
            Value<LongValue> v = (Value) new FloatValue();
            value2.assign(v);
        }, e -> {
            Assert.assertContains("Can't assign '0.0'(FloatValue) to LongValue",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            @SuppressWarnings({ "unchecked", "rawtypes" })
            Value<LongValue> v = (Value) new IntValue();
            value2.assign(v);
        }, e -> {
            Assert.assertContains("Can't assign '0'(IntValue) to LongValue",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            value2.assign(null);
        }, e -> {
            Assert.assertContains("Can't assign null to LongValue",
                                  e.getMessage());
        });
    }

    @Test
    public void testCopy() {
        LongValue value1 = new LongValue();
        LongValue value2 = new LongValue(123456L);

        LongValue copy = value1.copy();
        Assert.assertEquals(0L, value1.value());
        Assert.assertEquals(0L, copy.value());

        copy.assign(value2);
        Assert.assertEquals(123456L, copy.value());
        Assert.assertEquals(0L, value1.value());
    }

    @Test
    public void testReadWrite() throws IOException {
        assertValueEqualAfterWriteAndRead(new LongValue());
        assertValueEqualAfterWriteAndRead(new LongValue(123456L));
        assertValueEqualAfterWriteAndRead(new LongValue(Long.MIN_VALUE));
        assertValueEqualAfterWriteAndRead(new LongValue(Long.MAX_VALUE));
    }

    @Test
    public void testCompare() {
        LongValue value1 = new LongValue(123L);
        LongValue value2 = new LongValue(123L);
        LongValue value3 = new LongValue(321L);
        Assert.assertEquals(0, value1.compareTo(value2));
        Assert.assertLt(0, value1.compareTo(value3));
        Assert.assertGt(0, value3.compareTo(value1));
    }

    @Test
    public void testEquals() {
        LongValue value1 = new LongValue();
        Assert.assertTrue(value1.equals(value1));
        Assert.assertTrue(value1.equals(new LongValue(0L)));
        Assert.assertFalse(value1.equals(new LongValue(1L)));
        Assert.assertFalse(value1.equals(new FloatValue(1f)));
        Assert.assertFalse(value1.equals(null));
    }

    @Test
    public void testHashCode() {
        LongValue value1 = new LongValue();
        LongValue value2 = new LongValue(123456L);
        LongValue value3 = new LongValue(Long.MIN_VALUE);
        LongValue value4 = new LongValue(Long.MAX_VALUE);

        Assert.assertEquals(Long.hashCode(0L),
                            value1.hashCode());
        Assert.assertEquals(Long.hashCode(123456L),
                            value2.hashCode());
        Assert.assertEquals(Long.hashCode(Long.MIN_VALUE),
                            value3.hashCode());
        Assert.assertEquals(Long.hashCode(Long.MAX_VALUE),
                            value4.hashCode());
    }

    @Test
    public void testToString() {
        LongValue value1 = new LongValue();
        LongValue value2 = new LongValue(123456L);
        LongValue value3 = new LongValue(Long.MIN_VALUE);
        LongValue value4 = new LongValue(Long.MAX_VALUE);

        Assert.assertEquals("0", value1.toString());
        Assert.assertEquals("123456", value2.toString());
        Assert.assertEquals("-9223372036854775808", value3.toString());
        Assert.assertEquals("9223372036854775807", value4.toString());
    }
}
