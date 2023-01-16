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

public class FloatValueTest extends UnitTestBase {

    @Test
    public void testType() {
        FloatValue value1 = new FloatValue();
        FloatValue value2 = new FloatValue(Float.MIN_VALUE);

        Assert.assertEquals(ValueType.FLOAT, value1.valueType());
        Assert.assertEquals(ValueType.FLOAT, value2.valueType());
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
    public void testNumber() {
        FloatValue value1 = new FloatValue();
        FloatValue value2 = new FloatValue(1234.56f);
        FloatValue value3 = new FloatValue(Float.MIN_VALUE);
        FloatValue value4 = new FloatValue(Float.MAX_VALUE);

        Assert.assertEquals(0, value1.intValue());
        Assert.assertEquals(0L, value1.longValue());
        Assert.assertEquals(0.0f, value1.floatValue(), 0.0f);
        Assert.assertEquals(0.0d, value1.doubleValue(), 0.0d);

        Assert.assertEquals(1234, value2.intValue());
        Assert.assertEquals(1234L, value2.longValue());
        Assert.assertEquals(1234.56f, value2.floatValue(), 0.0f);
        Assert.assertEquals(1234.56005859375d, value2.doubleValue(), 0.0d);

        Assert.assertEquals(0, value3.intValue());
        Assert.assertEquals(0L, value3.longValue());
        Assert.assertEquals(1.4E-45f, value3.floatValue(), 0.0f);
        Assert.assertEquals(1.401298464324817E-45d, value3.doubleValue(), 0.0d);

        Assert.assertEquals(2147483647, value4.intValue());
        Assert.assertEquals(9223372036854775807L, value4.longValue());
        Assert.assertEquals(3.4028235E38f, value4.floatValue(), 0.0f);
        Assert.assertEquals(3.4028234663852886E38d, value4.doubleValue(), 0.0d);
    }

    @Test
    public void testString() {
        FloatValue value1 = new FloatValue();
        FloatValue value2 = new FloatValue(1234.56f);
        FloatValue value3 = new FloatValue(Float.MIN_VALUE);
        FloatValue value4 = new FloatValue(Float.MAX_VALUE);

        Assert.assertEquals("0.0", value1.string());
        Assert.assertEquals("1234.56", value2.string());
        Assert.assertEquals("1.4E-45", value3.string());
        Assert.assertEquals("3.4028235E38", value4.string());
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
            Value v = new IntValue();
            value2.assign(v);
        }, e -> {
            Assert.assertContains("Can't assign '0'(IntValue) to FloatValue",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Value v = new DoubleValue();
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

        Assert.assertGt(0, value1.compareTo(NullValue.get()));
        Assert.assertGt(0, value1.compareTo(new IntValue(123)));
        Assert.assertGt(0, value1.compareTo(new LongValue(123)));
        Assert.assertLt(0, value1.compareTo(new DoubleValue(123)));
        Assert.assertLt(0, value1.compareTo(new StringValue("123")));
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
