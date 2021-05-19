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
import com.baidu.hugegraph.testutil.Assert;

public class FloatValueTest extends UnitTestBase {

    @Test
    public void testType() {
        FloatValue value1 = new FloatValue();
        FloatValue value2 = new FloatValue(Float.MIN_VALUE);

        Assert.assertEquals(ValueType.FLOAT, value1.type());
        Assert.assertEquals(ValueType.FLOAT, value2.type());
    }

    @Test
    public void testValue() {
        FloatValue value1 = new FloatValue();
        FloatValue value2 = new FloatValue(1234.56f);
        FloatValue value3 = new FloatValue(Float.MIN_VALUE);
        FloatValue value4 = new FloatValue(Float.MAX_VALUE);

        Assert.assertEquals(0f, value1.value(), 0f);
        Assert.assertEquals(1234.56f, value2.value(), 0f);
        Assert.assertEquals(Float.MIN_VALUE, value3.value(), 0f);
        Assert.assertEquals(Float.MAX_VALUE, value4.value(), 0f);

        value3.value(Float.MAX_VALUE);
        Assert.assertEquals(Float.MAX_VALUE, value3.value(), 0f);
        Assert.assertEquals(value3, value4);

        FloatValue value5 = new FloatValue(value2.value());
        Assert.assertEquals(value2, value5);
    }

    @Test
    public void testAssign() {
        FloatValue value1 = new FloatValue();
        FloatValue value2 = new FloatValue(1234.56f);
        FloatValue value3 = new FloatValue(Float.MIN_VALUE);
        FloatValue value4 = new FloatValue(Float.MAX_VALUE);

        Assert.assertEquals(0f, value1.value(), 0f);
        value1.assign(value2);
        Assert.assertEquals(1234.56f, value1.value(), 0f);
        Assert.assertEquals(1234.56f, value2.value(), 0f);

        value2.assign(value3);
        Assert.assertEquals(1234.56f, value1.value(), 0f);
        Assert.assertEquals(Float.MIN_VALUE, value2.value(), 0f);

        value2.assign(value4);
        Assert.assertEquals(1234.56f, value1.value(), 0f);
        Assert.assertEquals(Float.MAX_VALUE, value2.value(), 0f);
        Assert.assertEquals(Float.MIN_VALUE, value3.value(), 0f);
        Assert.assertEquals(Float.MAX_VALUE, value4.value(), 0f);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            @SuppressWarnings({ "unchecked", "rawtypes" })
            Value<FloatValue> v = (Value) new IntValue();
            value2.assign(v);
        }, e -> {
            Assert.assertContains("Can't assign '0'(IntValue) to FloatValue",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            @SuppressWarnings({ "unchecked", "rawtypes" })
            Value<FloatValue> v = (Value) new DoubleValue();
            value2.assign(v);
        }, e -> {
            Assert.assertContains("Can't assign '0.0'(DoubleValue) to " +
                                  "FloatValue", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            value2.assign(null);
        }, e -> {
            Assert.assertContains("Can't assign null to FloatValue",
                                  e.getMessage());
        });
    }

    @Test
    public void testCopy() {
        FloatValue value1 = new FloatValue();
        FloatValue value2 = new FloatValue(1234.56f);

        FloatValue copy = value1.copy();
        Assert.assertEquals(0f, value1.value(), 0f);
        Assert.assertEquals(0f, copy.value(), 0f);

        copy.assign(value2);
        Assert.assertEquals(1234.56f, copy.value(), 0f);
        Assert.assertEquals(0f, value1.value(), 0f);
    }

    @Test
    public void testReadWrite() throws IOException {
        assertValueEqualAfterWriteAndRead(new FloatValue());
        assertValueEqualAfterWriteAndRead(new FloatValue(1234.56f));
        assertValueEqualAfterWriteAndRead(new FloatValue(-1234.56f));
        assertValueEqualAfterWriteAndRead(new FloatValue(Float.MIN_VALUE));
        assertValueEqualAfterWriteAndRead(new FloatValue(Float.MAX_VALUE));
    }

    @Test
    public void testCompare() {
        FloatValue value1 = new FloatValue(123f);
        FloatValue value2 = new FloatValue(123f);
        FloatValue value3 = new FloatValue(321f);
        Assert.assertEquals(0, value1.compareTo(value2));
        Assert.assertLt(0, value1.compareTo(value3));
        Assert.assertGt(0, value3.compareTo(value1));
    }

    @Test
    public void testEquals() {
        FloatValue value1 = new FloatValue(0f);
        Assert.assertTrue(value1.equals(value1));
        Assert.assertTrue(value1.equals(new FloatValue(0f)));
        Assert.assertFalse(value1.equals(new FloatValue(1)));
        Assert.assertFalse(value1.equals(new FloatValue(1.0f)));

        Assert.assertFalse(value1.equals(new IntValue(0)));
        Assert.assertFalse(value1.equals(null));
    }

    @Test
    public void testHashCode() {
        FloatValue value1 = new FloatValue();
        FloatValue value2 = new FloatValue(1234.56f);
        FloatValue value3 = new FloatValue(Float.MIN_VALUE);
        FloatValue value4 = new FloatValue(Float.MAX_VALUE);

        Assert.assertEquals(Float.hashCode(0f),
                            value1.hashCode());
        Assert.assertEquals(Float.hashCode(1234.56f),
                            value2.hashCode());
        Assert.assertEquals(Float.hashCode(Float.MIN_VALUE),
                            value3.hashCode());
        Assert.assertEquals(Float.hashCode(Float.MAX_VALUE),
                            value4.hashCode());
    }

    @Test
    public void testToString() {
        FloatValue value1 = new FloatValue();
        FloatValue value2 = new FloatValue(1234.56f);
        FloatValue value3 = new FloatValue(Float.MIN_VALUE);
        FloatValue value4 = new FloatValue(Float.MAX_VALUE);

        Assert.assertEquals("0.0", value1.toString());
        Assert.assertEquals("1234.56", value2.toString());
        Assert.assertEquals("1.4E-45", value3.toString());
        Assert.assertEquals("3.4028235E38", value4.toString());
    }
}
