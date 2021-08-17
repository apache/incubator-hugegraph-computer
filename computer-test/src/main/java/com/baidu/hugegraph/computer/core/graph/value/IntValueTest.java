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

public class IntValueTest extends UnitTestBase {

    @Test
    public void testType() {
        IntValue intValue1 = new IntValue();
        IntValue intValue2 = new IntValue(Integer.MIN_VALUE);

        Assert.assertEquals(ValueType.INT, intValue1.valueType());
        Assert.assertEquals(ValueType.INT, intValue2.valueType());
    }

    @Test
    public void testValue() {
        IntValue intValue1 = new IntValue();
        IntValue intValue2 = new IntValue(123456);
        IntValue intValue3 = new IntValue(Integer.MIN_VALUE);
        IntValue intValue4 = new IntValue(Integer.MAX_VALUE);

        Assert.assertEquals(0, intValue1.value());
        Assert.assertEquals(123456, intValue2.value());
        Assert.assertEquals(Integer.MIN_VALUE, intValue3.value());
        Assert.assertEquals(Integer.MAX_VALUE, intValue4.value());

        intValue3.value(Integer.MAX_VALUE);
        Assert.assertEquals(Integer.MAX_VALUE, intValue3.value());
        Assert.assertEquals(intValue3, intValue4);

        IntValue intValue5 = new IntValue(intValue2.value());
        Assert.assertEquals(intValue2, intValue5);
    }

    @Test
    public void testAssign() {
        IntValue intValue1 = new IntValue();
        IntValue intValue2 = new IntValue(123456);
        IntValue intValue3 = new IntValue(Integer.MIN_VALUE);
        IntValue intValue4 = new IntValue(Integer.MAX_VALUE);

        Assert.assertEquals(0, intValue1.value());
        intValue1.assign(intValue2);
        Assert.assertEquals(123456, intValue1.value());
        Assert.assertEquals(123456, intValue2.value());

        intValue2.assign(intValue3);
        Assert.assertEquals(123456, intValue1.value());
        Assert.assertEquals(Integer.MIN_VALUE, intValue2.value());

        intValue2.assign(intValue4);
        Assert.assertEquals(123456, intValue1.value());
        Assert.assertEquals(Integer.MAX_VALUE, intValue2.value());
        Assert.assertEquals(Integer.MIN_VALUE, intValue3.value());
        Assert.assertEquals(Integer.MAX_VALUE, intValue4.value());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            @SuppressWarnings({ "unchecked", "rawtypes" })
            Value<IntValue> v = (Value) new FloatValue();
            intValue2.assign(v);
        }, e -> {
            Assert.assertContains("Can't assign '0.0'(FloatValue) to IntValue",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            @SuppressWarnings({ "unchecked", "rawtypes" })
            Value<IntValue> v = (Value) new LongValue();
            intValue2.assign(v);
        }, e -> {
            Assert.assertContains("Can't assign '0'(LongValue) to IntValue",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            intValue2.assign(null);
        }, e -> {
            Assert.assertContains("Can't assign null to IntValue",
                                  e.getMessage());
        });
    }

    @Test
    public void testAssignWithSubClass() {
        IntValue intValue1 = new IntValue(123456);
        Assert.assertEquals(123456, intValue1.value());

        SubIntValue subIntValue = new SubIntValue(13579);
        Assert.assertEquals(13579, subIntValue.value());
        Assert.assertEquals("SubIntValue:13579", subIntValue.toString());
        intValue1.assign(subIntValue);
        Assert.assertEquals(13579, intValue1.value());
        Assert.assertEquals("13579", intValue1.toString());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            subIntValue.assign(intValue1);
        }, e -> {
            Assert.assertContains("Can't assign '13579'(IntValue) to " +
                                  "SubIntValue", e.getMessage());
        });
    }

    @Test
    public void testCopy() {
        IntValue intValue1 = new IntValue();
        IntValue intValue2 = new IntValue(123456);

        IntValue copy = intValue1.copy();
        Assert.assertEquals(0, intValue1.value());
        Assert.assertEquals(0, copy.value());

        copy.assign(intValue2);
        Assert.assertEquals(123456, copy.value());
        Assert.assertEquals(0, intValue1.value());
    }

    @Test
    public void testReadWrite() throws IOException {
        assertValueEqualAfterWriteAndRead(new IntValue());
        assertValueEqualAfterWriteAndRead(new IntValue(123456));
        assertValueEqualAfterWriteAndRead(new IntValue(Integer.MIN_VALUE));
        assertValueEqualAfterWriteAndRead(new IntValue(Integer.MAX_VALUE));
    }

    @Test
    public void testCompare() {
        IntValue intValue1 = new IntValue(123);
        IntValue intValue2 = new IntValue(123);
        IntValue intValue3 = new IntValue(321);
        Assert.assertEquals(0, intValue1.compareTo(intValue2));
        Assert.assertLt(0, intValue1.compareTo(intValue3));
        Assert.assertGt(0, intValue3.compareTo(intValue1));
    }

    @Test
    public void testEquals() {
        IntValue intValue1 = new IntValue();
        Assert.assertTrue(intValue1.equals(intValue1));
        Assert.assertTrue(intValue1.equals(new IntValue(0)));
        Assert.assertFalse(intValue1.equals(new IntValue(1)));
        Assert.assertFalse(intValue1.equals(new FloatValue(1f)));
        Assert.assertFalse(intValue1.equals(null));
    }

    @Test
    public void testHashCode() {
        IntValue intValue1 = new IntValue();
        IntValue intValue2 = new IntValue(123456);
        IntValue intValue3 = new IntValue(Integer.MIN_VALUE);
        IntValue intValue4 = new IntValue(Integer.MAX_VALUE);

        Assert.assertEquals(Integer.hashCode(0),
                            intValue1.hashCode());
        Assert.assertEquals(Integer.hashCode(123456),
                            intValue2.hashCode());
        Assert.assertEquals(Integer.hashCode(Integer.MIN_VALUE),
                            intValue3.hashCode());
        Assert.assertEquals(Integer.hashCode(Integer.MAX_VALUE),
                            intValue4.hashCode());
    }

    @Test
    public void testToString() {
        IntValue intValue1 = new IntValue();
        IntValue intValue2 = new IntValue(123456);
        IntValue intValue3 = new IntValue(Integer.MIN_VALUE);
        IntValue intValue4 = new IntValue(Integer.MAX_VALUE);

        Assert.assertEquals("0", intValue1.toString());
        Assert.assertEquals("123456", intValue2.toString());
        Assert.assertEquals("-2147483648", intValue3.toString());
        Assert.assertEquals("2147483647", intValue4.toString());
    }

    private static class SubIntValue extends IntValue {

        private static final long serialVersionUID = 279936857611008457L;

        public SubIntValue(int value) {
            super(value);
        }

        @Override
        public String toString() {
            return "SubIntValue:" + super.toString();
        }
    }
}
