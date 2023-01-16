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

import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class BooleanValueTest extends UnitTestBase {

    @Test
    public void testType() {
        BooleanValue value1 = new BooleanValue();
        BooleanValue value2 = new BooleanValue(true);
        BooleanValue value3 = new BooleanValue(false);

        Assert.assertEquals(ValueType.BOOLEAN, value1.valueType());
        Assert.assertEquals(ValueType.BOOLEAN, value2.valueType());
        Assert.assertEquals(ValueType.BOOLEAN, value3.valueType());
    }

    @Test
    public void testValue() {
        BooleanValue value1 = new BooleanValue();
        BooleanValue value2 = new BooleanValue(true);
        BooleanValue value3 = new BooleanValue(false);

        Assert.assertEquals(false, value1.value());
        Assert.assertEquals(true, value2.value());
        Assert.assertEquals(false, value3.value());

        Assert.assertEquals(false, value1.boolValue());
        Assert.assertEquals(true, value2.boolValue());
        Assert.assertEquals(false, value3.boolValue());

        value2.value(false);
        Assert.assertEquals(false, value2.value());
        Assert.assertEquals(value3, value2);

        BooleanValue value5 = new BooleanValue(value2.value());
        Assert.assertEquals(value2, value5);
    }

    @Test
    public void testString() {
        BooleanValue value1 = new BooleanValue();
        BooleanValue value2 = new BooleanValue(true);
        BooleanValue value3 = new BooleanValue(false);

        Assert.assertEquals("false", value1.string());
        Assert.assertEquals("true", value2.string());
        Assert.assertEquals("false", value3.string());
    }

    @Test
    public void testAssign() {
        BooleanValue value1 = new BooleanValue();
        BooleanValue value2 = new BooleanValue(true);
        BooleanValue value3 = new BooleanValue(false);

        Assert.assertEquals(false, value1.value());
        value1.assign(value2);
        Assert.assertEquals(true, value1.value());
        Assert.assertEquals(true, value2.value());

        value2.assign(value3);
        Assert.assertEquals(true, value1.value());
        Assert.assertEquals(false, value2.value());
        Assert.assertEquals(false, value3.value());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Value v = new IntValue();
            value2.assign(v);
        }, e -> {
            Assert.assertContains("Can't assign '0'(IntValue) to BooleanValue",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Value v = new LongValue();
            value2.assign(v);
        }, e -> {
            Assert.assertContains("Can't assign '0'(LongValue) to BooleanValue",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            value2.assign(null);
        }, e -> {
            Assert.assertContains("Can't assign null to BooleanValue",
                                  e.getMessage());
        });
    }

    @Test
    public void testCopy() {
        BooleanValue value1 = new BooleanValue();
        BooleanValue value2 = new BooleanValue(true);
        BooleanValue value3 = new BooleanValue(false);

        BooleanValue copy = value1.copy();
        Assert.assertEquals(false, value1.value());
        Assert.assertEquals(false, copy.value());

        copy.assign(value2);
        Assert.assertEquals(false, value1.value());
        Assert.assertEquals(true, copy.value());

        copy = value2.copy();
        Assert.assertEquals(true, value2.value());
        Assert.assertEquals(true, copy.value());

        copy.assign(value3);
        Assert.assertEquals(true, value2.value());
        Assert.assertEquals(false, copy.value());
    }

    @Test
    public void testReadWrite() throws IOException {
        assertValueEqualAfterWriteAndRead(new BooleanValue());
        assertValueEqualAfterWriteAndRead(new BooleanValue(true));
        assertValueEqualAfterWriteAndRead(new BooleanValue(false));
    }

    @Test
    public void testCompare() {
        BooleanValue value1 = new BooleanValue();
        BooleanValue value2 = new BooleanValue(false);
        BooleanValue value3 = new BooleanValue(true);

        Assert.assertEquals(0, value1.compareTo(value2));
        Assert.assertLt(0, value1.compareTo(value3));
        Assert.assertGt(0, value3.compareTo(value1));

        Assert.assertGt(0, value3.compareTo(NullValue.get()));
        Assert.assertLt(0, value3.compareTo(new IntValue(1)));
        Assert.assertLt(0, value3.compareTo(new StringValue("true")));
    }

    @Test
    public void testEquals() {
        BooleanValue value1 = new BooleanValue();
        Assert.assertTrue(value1.equals(value1));
        Assert.assertTrue(value1.equals(new BooleanValue(false)));
        Assert.assertFalse(value1.equals(new BooleanValue(true)));

        Assert.assertFalse(value1.equals(new IntValue(1)));
        Assert.assertFalse(value1.equals(null));
    }

    @Test
    public void testHashCode() {
        BooleanValue value1 = new BooleanValue();
        BooleanValue value2 = new BooleanValue(true);
        BooleanValue value3 = new BooleanValue(false);

        Assert.assertEquals(Boolean.hashCode(false), value1.hashCode());
        Assert.assertEquals(Boolean.hashCode(true), value2.hashCode());
        Assert.assertEquals(Boolean.hashCode(false), value3.hashCode());
    }

    @Test
    public void testToString() {
        BooleanValue value1 = new BooleanValue();
        BooleanValue value2 = new BooleanValue(true);
        BooleanValue value3 = new BooleanValue(false);

        Assert.assertEquals("false", value1.toString());
        Assert.assertEquals("true", value2.toString());
        Assert.assertEquals("false", value3.toString());
    }
}
