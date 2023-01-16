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

public class DoubleValueTest extends UnitTestBase {

    @Test
    public void testType() {
        DoubleValue value1 = new DoubleValue();
        DoubleValue value2 = new DoubleValue(Double.MIN_VALUE);

        Assert.assertEquals(ValueType.DOUBLE, value1.valueType());
        Assert.assertEquals(ValueType.DOUBLE, value2.valueType());
    }

    @Test
    public void testValue() {
        DoubleValue value1 = new DoubleValue();
        DoubleValue value2 = new DoubleValue(1234.56d);
        DoubleValue value3 = new DoubleValue(Double.MIN_VALUE);
        DoubleValue value4 = new DoubleValue(Double.MAX_VALUE);

        Assert.assertEquals(0d, value1.value(), 0d);
        Assert.assertEquals(1234.56d, value2.value(), 0d);
        Assert.assertEquals(Double.MIN_VALUE, value3.value(), 0d);
        Assert.assertEquals(Double.MAX_VALUE, value4.value(), 0d);

        value3.value(Double.MAX_VALUE);
        Assert.assertEquals(Double.MAX_VALUE, value3.value(), 0d);
        Assert.assertEquals(value3, value4);

        DoubleValue value5 = new DoubleValue(value2.value());
        Assert.assertEquals(value2, value5);
    }

    @Test
    public void testNumber() {
        DoubleValue value1 = new DoubleValue();
        DoubleValue value2 = new DoubleValue(1234.56d);
        DoubleValue value3 = new DoubleValue(Double.MIN_VALUE);
        DoubleValue value4 = new DoubleValue(Double.MAX_VALUE);

        Assert.assertEquals(0, value1.intValue());
        Assert.assertEquals(0L, value1.longValue());
        Assert.assertEquals(0.0f, value1.floatValue(), 0.0f);
        Assert.assertEquals(0.0d, value1.doubleValue(), 0.0d);

        Assert.assertEquals(1234, value2.intValue());
        Assert.assertEquals(1234L, value2.longValue());
        Assert.assertEquals(1234.56f, value2.floatValue(), 0.0f);
        Assert.assertEquals(1234.56d, value2.doubleValue(), 0.0d);

        Assert.assertEquals(0, value3.intValue());
        Assert.assertEquals(0L, value3.longValue());
        Assert.assertEquals(0.0f, value3.floatValue(), 0.0f);
        Assert.assertEquals(4.9E-324d, value3.doubleValue(), 0.0d);

        Assert.assertEquals(2147483647, value4.intValue());
        Assert.assertEquals(9223372036854775807L, value4.longValue());
        Assert.assertEquals(Float.POSITIVE_INFINITY, value4.floatValue(), 0.0f);
        Assert.assertEquals(1.7976931348623157E308d, value4.doubleValue(), 0d);
    }

    @Test
    public void testString() {
        DoubleValue value1 = new DoubleValue();
        DoubleValue value2 = new DoubleValue(1234.56d);
        DoubleValue value3 = new DoubleValue(Double.MIN_VALUE);
        DoubleValue value4 = new DoubleValue(Double.MAX_VALUE);

        Assert.assertEquals("0.0", value1.string());
        Assert.assertEquals("1234.56", value2.string());
        Assert.assertEquals("4.9E-324", value3.string());
        Assert.assertEquals("1.7976931348623157E308", value4.string());
    }

    @Test
    public void testAssign() {
        DoubleValue value1 = new DoubleValue();
        DoubleValue value2 = new DoubleValue(1234.56d);
        DoubleValue value3 = new DoubleValue(Double.MIN_VALUE);
        DoubleValue value4 = new DoubleValue(Double.MAX_VALUE);

        Assert.assertEquals(0d, value1.value(), 0d);
        value1.assign(value2);
        Assert.assertEquals(1234.56d, value1.value(), 0d);
        Assert.assertEquals(1234.56d, value2.value(), 0d);

        value2.assign(value3);
        Assert.assertEquals(1234.56d, value1.value(), 0d);
        Assert.assertEquals(Double.MIN_VALUE, value2.value(), 0d);

        value2.assign(value4);
        Assert.assertEquals(1234.56d, value1.value(), 0d);
        Assert.assertEquals(Double.MAX_VALUE, value2.value(), 0d);
        Assert.assertEquals(Double.MIN_VALUE, value3.value(), 0d);
        Assert.assertEquals(Double.MAX_VALUE, value4.value(), 0d);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Value v = new IntValue();
            value2.assign(v);
        }, e -> {
            Assert.assertContains("Can't assign '0'(IntValue) to DoubleValue",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Value v = new FloatValue();
            value2.assign(v);
        }, e -> {
            Assert.assertContains("Can't assign '0.0'(FloatValue) to " +
                                  "DoubleValue", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            value2.assign(null);
        }, e -> {
            Assert.assertContains("Can't assign null to DoubleValue",
                                  e.getMessage());
        });
    }

    @Test
    public void testCopy() {
        DoubleValue value1 = new DoubleValue();
        DoubleValue value2 = new DoubleValue(1234.56d);

        DoubleValue copy = value1.copy();
        Assert.assertEquals(0d, value1.value(), 0d);
        Assert.assertEquals(0d, copy.value(), 0d);

        copy.assign(value2);
        Assert.assertEquals(1234.56d, copy.value(), 0d);
        Assert.assertEquals(0d, value1.value(), 0d);
    }

    @Test
    public void testReadWrite() throws IOException {
        assertValueEqualAfterWriteAndRead(new DoubleValue(0.0d));
        assertValueEqualAfterWriteAndRead(new DoubleValue(-0.0d));
        assertValueEqualAfterWriteAndRead(new DoubleValue(0.1d));
        assertValueEqualAfterWriteAndRead(new DoubleValue(-0.1d));
        assertValueEqualAfterWriteAndRead(new DoubleValue(1.1d));
        assertValueEqualAfterWriteAndRead(new DoubleValue(-1.1d));
        assertValueEqualAfterWriteAndRead(new DoubleValue(1.123456789d));
        assertValueEqualAfterWriteAndRead(new DoubleValue(-1.123456789d));
        assertValueEqualAfterWriteAndRead(new DoubleValue(
                                          987654321.123456789d));
        assertValueEqualAfterWriteAndRead(new DoubleValue(
                                          -987654321.123456789d));
        assertValueEqualAfterWriteAndRead(new DoubleValue(127d));
        assertValueEqualAfterWriteAndRead(new DoubleValue(-127d));
        assertValueEqualAfterWriteAndRead(new DoubleValue(128d));
        assertValueEqualAfterWriteAndRead(new DoubleValue(-128d));
        assertValueEqualAfterWriteAndRead(new DoubleValue(256d));
        assertValueEqualAfterWriteAndRead(new DoubleValue(-256d));
        assertValueEqualAfterWriteAndRead(new DoubleValue(32767d));
        assertValueEqualAfterWriteAndRead(new DoubleValue(-32767d));
        assertValueEqualAfterWriteAndRead(new DoubleValue(32768d));
        assertValueEqualAfterWriteAndRead(new DoubleValue(-32768d));
        assertValueEqualAfterWriteAndRead(new DoubleValue(65536d));
        assertValueEqualAfterWriteAndRead(new DoubleValue(-65535d));
        assertValueEqualAfterWriteAndRead(new DoubleValue(Integer.MIN_VALUE));
        assertValueEqualAfterWriteAndRead(new DoubleValue(Integer.MAX_VALUE));
        assertValueEqualAfterWriteAndRead(new DoubleValue(Long.MIN_VALUE));
        assertValueEqualAfterWriteAndRead(new DoubleValue(Long.MAX_VALUE));
        assertValueEqualAfterWriteAndRead(new DoubleValue(Double.MIN_VALUE));
        assertValueEqualAfterWriteAndRead(new DoubleValue(Double.MAX_VALUE));
        assertValueEqualAfterWriteAndRead(new DoubleValue(Double.MIN_VALUE));
        assertValueEqualAfterWriteAndRead(new DoubleValue(Double.MAX_VALUE));
    }

    @Test
    public void testCompare() {
        DoubleValue value1 = new DoubleValue(123d);
        DoubleValue value2 = new DoubleValue(123d);
        DoubleValue value3 = new DoubleValue(321d);
        Assert.assertEquals(0, value1.compareTo(value2));
        Assert.assertLt(0, value1.compareTo(value3));
        Assert.assertGt(0, value3.compareTo(value1));

        Assert.assertGt(0, value1.compareTo(NullValue.get()));
        Assert.assertGt(0, value1.compareTo(new IntValue(123)));
        Assert.assertGt(0, value1.compareTo(new FloatValue(123)));
        Assert.assertLt(0, value1.compareTo(new StringValue("123")));
    }

    @Test
    public void testEquals() {
        DoubleValue value1 = new DoubleValue();
        Assert.assertTrue(value1.equals(value1));
        Assert.assertTrue(value1.equals(new DoubleValue(0d)));
        Assert.assertFalse(value1.equals(new DoubleValue(1)));
        Assert.assertFalse(value1.equals(new DoubleValue(1.0d)));
        Assert.assertFalse(value1.equals(null));
    }

    @Test
    public void testHashCode() {
        DoubleValue value1 = new DoubleValue();
        DoubleValue value2 = new DoubleValue(1234.56d);
        DoubleValue value3 = new DoubleValue(Double.MIN_VALUE);
        DoubleValue value4 = new DoubleValue(Double.MAX_VALUE);

        Assert.assertEquals(Double.hashCode(0d),
                            value1.hashCode());
        Assert.assertEquals(Double.hashCode(1234.56d),
                            value2.hashCode());
        Assert.assertEquals(Double.hashCode(Double.MIN_VALUE),
                            value3.hashCode());
        Assert.assertEquals(Double.hashCode(Double.MAX_VALUE),
                            value4.hashCode());
    }

    @Test
    public void testToString() {
        DoubleValue value1 = new DoubleValue();
        DoubleValue value2 = new DoubleValue(1234.56d);
        DoubleValue value3 = new DoubleValue(Double.MIN_VALUE);
        DoubleValue value4 = new DoubleValue(Double.MAX_VALUE);
        DoubleValue value5 = new DoubleValue(-Double.MAX_VALUE);

        Assert.assertEquals("0.0", value1.toString());
        Assert.assertEquals("1234.56", value2.toString());
        Assert.assertEquals("4.9E-324", value3.toString());
        Assert.assertEquals("1.7976931348623157E308", value4.toString());
        Assert.assertEquals("-1.7976931348623157E308", value5.toString());
    }
}
