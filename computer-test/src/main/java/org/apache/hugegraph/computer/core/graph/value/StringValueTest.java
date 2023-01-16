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

import static org.apache.hugegraph.computer.suite.unit.UnitTestBase.assertValueEqualAfterWriteAndRead;

import java.io.IOException;

import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.computer.core.graph.id.BytesId;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class StringValueTest {

    @Test
    public void testType() {
        StringValue value1 = new StringValue("t1");
        StringValue value2 = new StringValue("t2");

        Assert.assertEquals(ValueType.STRING, value1.valueType());
        Assert.assertEquals(ValueType.STRING, value2.valueType());
    }

    @Test
    public void testValue() {
        StringValue value1 = new StringValue("t1");
        StringValue value2 = new StringValue("t1");

        Assert.assertEquals("t1", value1.value());
        Assert.assertEquals(value1, value2);
    }

    @Test
    public void testString() {
        StringValue value1 = new StringValue();
        StringValue value2 = new StringValue("t1");
        StringValue value3 = new StringValue("t1");
        StringValue value4 = new StringValue("test");

        Assert.assertEquals("", value1.string());
        Assert.assertEquals("t1", value2.string());
        Assert.assertEquals("t1", value3.string());
        Assert.assertEquals("test", value4.string());
    }

    @Test
    public void testAssign() {
        StringValue value1 = new StringValue();
        StringValue value2 = new StringValue("test");
        value1.assign(value2);

        Assert.assertEquals("test", value1.value());
        Assert.assertEquals("test", value2.value());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Value v = new FloatValue();
            value2.assign(v);
        }, e -> {
            Assert.assertContains("Can't assign '0.0'(FloatValue) to " +
                                  "StringValue", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            value2.assign(null);
        }, e -> {
            Assert.assertContains("Can't assign null to StringValue",
                                  e.getMessage());
        });
    }

    @Test
    public void testCopy() {
        StringValue value1 = new StringValue("test");

        StringValue copy = value1.copy();
        Assert.assertEquals("test", value1.value());
        Assert.assertEquals(value1, copy);

        StringValue value2 = new StringValue("assign test");
        copy.assign(value2);
        Assert.assertEquals("assign test", copy.value());
        Assert.assertEquals(value2, copy);
        Assert.assertNotEquals(value1, copy);
    }

    @Test
    public void testReadWrite() throws IOException {
        assertValueEqualAfterWriteAndRead(new StringValue());
        assertValueEqualAfterWriteAndRead(new StringValue("test"));
    }

    @Test
    public void testCompare() {
        StringValue value1 = new StringValue("123");
        StringValue value2 = new StringValue("abc");
        StringValue value3 = new StringValue("bcd");

        Assert.assertLt(0, value1.compareTo(value2));
        Assert.assertLt(0, value2.compareTo(value3));
        Assert.assertGt(0, value3.compareTo(value1));

        Assert.assertGt(0, value1.compareTo(NullValue.get()));
        Assert.assertGt(0, value1.compareTo(new BooleanValue()));
        Assert.assertGt(0, value1.compareTo(new IntValue(123)));
        Assert.assertGt(0, value1.compareTo(new FloatValue(123)));
        Assert.assertGt(0, value1.compareTo(new DoubleValue(123)));
        Assert.assertLt(0, value1.compareTo(BytesId.of("123")));
    }

    @Test
    public void testEquals() {
        StringValue value1 = new StringValue("test");
        StringValue value2 = new StringValue("test");

        Assert.assertEquals(value1, value2);

        DoubleValue value3 = new DoubleValue(1.1);
        Assert.assertFalse(value1.equals(value3));
    }

    @Test
    public void testHashCode() {
        StringValue value1 = new StringValue();
        StringValue value2 = new StringValue("test");

        Assert.assertEquals(Constants.EMPTY_STR.hashCode(),
                            value1.hashCode());
        Assert.assertEquals("test".hashCode(),
                            value2.hashCode());
    }

    @Test
    public void testToString() {
        StringValue value1 = new StringValue();
        StringValue value2 = new StringValue("test");

        Assert.assertEquals(Constants.EMPTY_STR,
                            value1.toString());
        Assert.assertEquals("test",
                            value2.toString());
    }
}
