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

public class NullValueTest extends UnitTestBase {

    @Test
    public void testType() {
        NullValue value1 = NullValue.get();
        NullValue value2 = NullValue.get();

        Assert.assertEquals(ValueType.NULL, value1.valueType());
        Assert.assertEquals(ValueType.NULL, value2.valueType());
    }

    @Test
    public void testValue() {
        NullValue value1 = NullValue.get();
        NullValue value2 = NullValue.get();

        Assert.assertEquals(value1, value2);
        Assert.assertEquals(value1.value(), value2.value());
        Assert.assertNull(value1.value());
        Assert.assertNull(value2.value());
    }

    @Test
    public void testString() {
        NullValue value1 = NullValue.get();
        NullValue value2 = NullValue.get();

        Assert.assertEquals("", value1.string());
        Assert.assertEquals("", value2.string());
    }

    @Test
    public void testAssign() {
        NullValue value1 = NullValue.get();
        NullValue value2 = NullValue.get();

        value1.assign(value2);
        Assert.assertEquals(value1, value2);

        value2.assign(value2);
        Assert.assertEquals(value1, value2);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Value v = new IntValue();
            value2.assign(v);
        }, e -> {
            Assert.assertContains("Can't assign '0'(IntValue) to NullValue",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Value v = new LongValue();
            value2.assign(v);
        }, e -> {
            Assert.assertContains("Can't assign '0'(LongValue) to NullValue",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            value2.assign(null);
        }, e -> {
            Assert.assertContains("Can't assign null to NullValue",
                                  e.getMessage());
        });
    }

    @Test
    public void testCopy() {
        NullValue value1 = NullValue.get();
        NullValue value2 = NullValue.get();

        NullValue copy = value1.copy();
        Assert.assertEquals(value1, copy);

        copy.assign(value2);
        Assert.assertEquals(value2, copy);
        Assert.assertEquals(value1, copy);
    }

    @Test
    public void testReadWrite() throws IOException {
        assertValueEqualAfterWriteAndRead(NullValue.get());
    }

    @Test
    public void testCompare() {
        NullValue value1 = NullValue.get();
        NullValue value2 = NullValue.get();
        Assert.assertEquals(0, value1.compareTo(value2));

        Assert.assertLt(0, value1.compareTo(new BooleanValue()));
        Assert.assertLt(0, value1.compareTo(new IntValue()));
        Assert.assertLt(0, value1.compareTo(new StringValue("")));
    }

    @Test
    public void testEquals() {
        NullValue value1 = NullValue.get();

        Assert.assertTrue(value1.equals(value1));
        Assert.assertTrue(value1.equals(NullValue.get()));

        Assert.assertFalse(value1.equals(new IntValue(1)));
        Assert.assertFalse(value1.equals(null));
    }

    @Test
    public void testHashCode() {
        NullValue value1 = NullValue.get();
        NullValue value2 = NullValue.get();

        Assert.assertEquals(0, value1.hashCode());
        Assert.assertEquals(0, value2.hashCode());
    }

    @Test
    public void testToString() {
        NullValue value1 = NullValue.get();
        NullValue value2 = NullValue.get();

        Assert.assertEquals("<null>", value1.toString());
        Assert.assertEquals("<null>", value2.toString());
    }
}
