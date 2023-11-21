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
import java.util.NoSuchElementException;

import org.apache.hugegraph.computer.core.graph.id.BytesId;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class ListValueTest extends UnitTestBase {

    @Test
    public void testType() {
        ListValue<IntValue> value1 = new ListValue<>(ValueType.INT);
        ListValue<FloatValue> value2 = new ListValue<>(ValueType.FLOAT);

        Assert.assertEquals(ValueType.LIST_VALUE, value1.valueType());
        Assert.assertEquals(ValueType.LIST_VALUE, value2.valueType());
    }

    @Test
    public void testElemType() {
        ListValue<IntValue> value1 = new ListValue<>(ValueType.INT);
        ListValue<FloatValue> value2 = new ListValue<>(ValueType.FLOAT);

        Assert.assertEquals(ValueType.INT, value1.elemType());
        Assert.assertEquals(ValueType.FLOAT, value2.elemType());
    }

    @Test
    public void testAdd() {
        ListValue<IntValue> value1 = new ListValue<>(ValueType.INT);
        ListValue<FloatValue> value2 = new ListValue<>(ValueType.FLOAT);

        Assert.assertEquals(0, value1.size());
        value1.add(new IntValue(101));
        value1.add(new IntValue(102));
        value1.add(new IntValue(103));
        Assert.assertEquals(3, value1.size());

        Assert.assertEquals(0, value2.size());
        value2.add(new FloatValue(201f));
        value2.add(new FloatValue(202f));
        Assert.assertEquals(2, value2.size());

        ListValue<Value.Tvalue<?>> value3 = new ListValue<>(ValueType.FLOAT);
        value3.add(new FloatValue(301f));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            value3.add(new IntValue(303));
        }, e -> {
            Assert.assertContains("Invalid value '303' with type int",
                                  e.getMessage());
            Assert.assertContains("expect element with type float",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            value3.add(null);
        }, e -> {
            Assert.assertContains("Can't add null to list",
                                  e.getMessage());
        });

        ListValue<Value.Tvalue<?>> value4 = new ListValue<>(ValueType.UNKNOWN);
        Assert.assertEquals(ValueType.UNKNOWN, value4.elemType());
        value4.add(new IntValue(303));
        Assert.assertEquals(ValueType.INT, value4.elemType());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            value4.add(new FloatValue(303f));
        }, e -> {
            Assert.assertContains("expect element with type int",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            value4.add(null);
        }, e -> {
            Assert.assertContains("Can't add null to list",
                                  e.getMessage());
        });
    }

    @Test
    public void testGet() {
        ListValue<IntValue> value1 = new ListValue<>(ValueType.INT);
        ListValue<FloatValue> value2 = new ListValue<>(ValueType.FLOAT);

        Assert.assertEquals(0, value1.size());
        value1.add(new IntValue(101));
        value1.add(new IntValue(102));
        value1.add(new IntValue(103));
        Assert.assertEquals(3, value1.size());

        Assert.assertEquals(0, value2.size());
        value2.add(new FloatValue(201f));
        value2.add(new FloatValue(202f));
        Assert.assertEquals(2, value2.size());

        Assert.assertEquals(101, value1.get(0).value());
        Assert.assertEquals(102, value1.get(1).value());
        Assert.assertEquals(103, value1.get(2).value());

        Assert.assertEquals(201f, value2.get(0).value(), 0f);
        Assert.assertEquals(202f, value2.get(1).value(), 0f);

        Assert.assertThrows(IndexOutOfBoundsException.class, () -> {
            value1.get(3);
        });

        Assert.assertThrows(IndexOutOfBoundsException.class, () -> {
            value2.get(3);
        });
    }

    @Test
    public void testGetFirst() {
        ListValue<IntValue> value = new ListValue<>(ValueType.INT);

        Assert.assertThrows(NoSuchElementException.class, () -> {
            value.getFirst();
        }, e -> {
            Assert.assertContains("The list is empty", e.getMessage());
        });

        value.add(new IntValue(100));
        Assert.assertEquals(100, value.getFirst().value());

        value.add(new IntValue(200));
        Assert.assertEquals(100, value.getFirst().value());
    }

    @Test
    public void testGetLast() {
        ListValue<IntValue> value1 = new ListValue<>(ValueType.INT);

        Assert.assertThrows(NoSuchElementException.class, () -> {
            value1.getLast();
        }, e -> {
            Assert.assertContains("The list is empty", e.getMessage());
        });

        value1.add(new IntValue(100));
        Assert.assertEquals(100, value1.getLast().value());

        value1.add(new IntValue(200));
        Assert.assertEquals(200, value1.getLast().value());
    }

    @Test
    public void testContains() {
        ListValue<IntValue> value1 = new ListValue<>(ValueType.INT);
        value1.add(new IntValue(100));
        value1.add(new IntValue(200));
        Assert.assertTrue(value1.contains(new IntValue(100)));
        Assert.assertTrue(value1.contains(new IntValue(200)));
        Assert.assertFalse(value1.contains(new IntValue(300)));
    }

    @Test
    public void testSize() {
        ListValue<IntValue> value1 = new ListValue<>(ValueType.INT);
        ListValue<FloatValue> value2 = new ListValue<>(ValueType.FLOAT);

        Assert.assertEquals(0, value1.size());
        value1.add(new IntValue(101));
        value1.add(new IntValue(102));
        value1.add(new IntValue(103));
        Assert.assertEquals(3, value1.size());

        Assert.assertEquals(0, value2.size());
        value2.add(new FloatValue(201f));
        value2.add(new FloatValue(202f));
        Assert.assertEquals(2, value2.size());
    }

    @Test
    public void testClear() {
        ListValue<IntValue> value = new ListValue<>(ValueType.INT);
        value.add(new IntValue(101));
        value.add(new IntValue(102));
        value.add(new IntValue(103));
        Assert.assertEquals(3, value.size());

        value.clear();
        Assert.assertEquals(0, value.size());
    }

    @Test
    public void testValues() {
        ListValue<IntValue> value1 = new ListValue<>(ValueType.INT);
        ListValue<FloatValue> value2 = new ListValue<>(ValueType.FLOAT);

        Assert.assertEquals(ImmutableList.of(), value1.values());
        Assert.assertEquals(ImmutableList.of(), value2.values());

        value1.add(new IntValue(101));
        value1.add(new IntValue(102));
        value1.add(new IntValue(103));

        value2.add(new FloatValue(201f));
        value2.add(new FloatValue(202f));

        Assert.assertEquals(ImmutableList.of(new IntValue(101),
                                             new IntValue(102),
                                             new IntValue(103)),
                            value1.values());

        Assert.assertEquals(ImmutableList.of(new FloatValue(201f),
                                             new FloatValue(202f)),
                            value2.values());
    }

    @Test
    public void testValue() {
        ListValue<IntValue> value1 = new ListValue<>(ValueType.INT);
        ListValue<FloatValue> value2 = new ListValue<>(ValueType.FLOAT);

        value1.add(new IntValue(101));
        value1.add(new IntValue(102));
        value1.add(new IntValue(103));

        value2.add(new FloatValue(201f));
        value2.add(new FloatValue(202f));

        Assert.assertEquals(ImmutableList.of(101, 102, 103), value1.value());
        Assert.assertEquals(ImmutableList.of(201.0f, 202.0f), value2.value());
    }

    @Test
    public void testString() {
        ListValue<IntValue> value1 = new ListValue<>(ValueType.INT);
        ListValue<FloatValue> value2 = new ListValue<>(ValueType.FLOAT);

        value1.add(new IntValue(101));
        value1.add(new IntValue(102));
        value1.add(new IntValue(103));

        value2.add(new FloatValue(201f));
        value2.add(new FloatValue(202f));

        Assert.assertEquals("[101, 102, 103]", value1.string());
        Assert.assertEquals("[201.0, 202.0]", value2.string());
    }

    @Test
    public void testAssign() {
        ListValue<IntValue> value1 = new ListValue<>(ValueType.INT);
        ListValue<FloatValue> value2 = new ListValue<>(ValueType.FLOAT);

        value1.add(new IntValue(101));
        value1.add(new IntValue(102));
        value1.add(new IntValue(103));

        value2.add(new FloatValue(201f));
        value2.add(new FloatValue(202f));

        ListValue<IntValue> value3 = new ListValue<>(ValueType.INT);
        ListValue<FloatValue> value4 = new ListValue<>(ValueType.FLOAT);
        Assert.assertEquals(ImmutableList.of(), value3.values());
        Assert.assertEquals(ImmutableList.of(), value4.values());

        Assert.assertNotEquals(value1, value3);
        value3.assign(value1);
        Assert.assertEquals(value1, value3);

        /*
         * NOTE: updating value1 lead to value3 to change
         * TODO: should keep value1 unmodifiable and let value3 modifiable?
         */
        Assert.assertEquals(3, value3.size());
        value1.add(new IntValue(104));
        Assert.assertEquals(4, value1.size());
        Assert.assertEquals(4, value3.size());

        Assert.assertNotEquals(value2, value4);
        value4.assign(value2);
        Assert.assertEquals(value2, value4);

        Assert.assertThrows(UnsupportedOperationException.class, () -> {
            value4.add(new FloatValue(203f));
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            value3.assign(value2);
        }, e -> {
            Assert.assertContains("Can't assign list<float> to list<int>",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            value4.assign(value1);
        }, e -> {
            Assert.assertContains("Can't assign list<int> to list<float>",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Value v = new IntValue();
            value3.assign(v);
        }, e -> {
            Assert.assertContains("Can't assign '0'(IntValue) to ListValue",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Value v = new LongValue();
            value4.assign(v);
        }, e -> {
            Assert.assertContains("Can't assign '0'(LongValue) to ListValue",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            value2.assign(null);
        }, e -> {
            Assert.assertContains("Can't assign null to ListValue",
                                  e.getMessage());
        });
    }

    @Test
    public void testCopy() {
        ListValue<IntValue> value1 = new ListValue<>(ValueType.INT);
        value1.add(new IntValue(100));
        value1.add(new IntValue(200));

        ListValue<IntValue> value2 = value1.copy();
        Assert.assertEquals(value1, value2);

        value2.add(new IntValue(300));

        Assert.assertEquals(3, value2.size());
        Assert.assertEquals(100, value2.get(0).value());
        Assert.assertEquals(200, value2.get(1).value());
        Assert.assertEquals(300, value2.get(2).value());

        Assert.assertEquals(2, value1.size());
        Assert.assertEquals(100, value1.get(0).value());
        Assert.assertEquals(200, value1.get(1).value());
    }

    @Test
    public void testReadWrite() throws IOException {
        ListValue<IntValue> oldValue = new ListValue<>(ValueType.INT);
        assertValueEqualAfterWriteAndRead(oldValue);

        oldValue.add(new IntValue(100));
        oldValue.add(new IntValue(200));
        assertValueEqualAfterWriteAndRead(oldValue);
    }

    @Test
    public void testCompare() {
        ListValue<IntValue> value1 = new ListValue<>(ValueType.INT);
        ListValue<IntValue> value2 = new ListValue<>(ValueType.INT);
        value1.add(new IntValue(100));
        value2.add(new IntValue(100));
        ListValue<IntValue> value3 = new ListValue<>(ValueType.INT);
        value3.add(new IntValue(100));
        value3.add(new IntValue(200));
        Assert.assertEquals(0, value1.compareTo(value2));
        Assert.assertLt(0, value1.compareTo(value3));
        Assert.assertGt(0, value3.compareTo(value1));

        Assert.assertGt(0, value1.compareTo(NullValue.get()));
        Assert.assertGt(0, value1.compareTo(new BooleanValue()));
        Assert.assertGt(0, value1.compareTo(new IntValue(123)));
        Assert.assertGt(0, value1.compareTo(new FloatValue(123)));
        Assert.assertGt(0, value1.compareTo(new DoubleValue(123)));
        Assert.assertGt(0, value1.compareTo(new StringValue("123")));
        Assert.assertGt(0, value1.compareTo(BytesId.of("123")));
    }

    @Test
    public void testEquals() {
        ListValue<IntValue> value1 = new ListValue<>(ValueType.INT);
        ListValue<FloatValue> value2 = new ListValue<>(ValueType.FLOAT);

        value1.add(new IntValue(101));
        value1.add(new IntValue(102));
        value1.add(new IntValue(103));

        value2.add(new FloatValue(201f));
        value2.add(new FloatValue(202f));

        Assert.assertTrue(value1.equals(value1));
        Assert.assertTrue(value2.equals(value2));

        Assert.assertTrue(value1.equals(value1.copy()));
        Assert.assertTrue(value2.equals(value2.copy()));

        Assert.assertFalse(value1.equals(value2));
        Assert.assertFalse(value2.equals(value1));

        Assert.assertFalse(value1.equals(new IntValue(1)));
        Assert.assertFalse(value1.equals(null));
    }

    @Test
    public void testHashCode() {
        ListValue<IntValue> value1 = new ListValue<>(ValueType.INT);
        ListValue<FloatValue> value2 = new ListValue<>(ValueType.FLOAT);

        Assert.assertEquals(1, value1.hashCode());
        Assert.assertEquals(1, value2.hashCode());

        value1.add(new IntValue(101));
        value1.add(new IntValue(102));
        value1.add(new IntValue(103));

        value2.add(new FloatValue(201f));
        value2.add(new FloatValue(202f));

        Assert.assertEquals(130117, value1.hashCode());
        Assert.assertEquals(1763771329, value2.hashCode());
    }

    @Test
    public void testToString() {
        ListValue<IntValue> value1 = new ListValue<>(ValueType.INT);
        ListValue<FloatValue> value2 = new ListValue<>(ValueType.FLOAT);

        Assert.assertEquals("[]", value1.toString());
        Assert.assertEquals("[]", value2.toString());

        value1.add(new IntValue(101));
        value1.add(new IntValue(102));
        value1.add(new IntValue(103));

        value2.add(new FloatValue(201f));
        value2.add(new FloatValue(202f));

        Assert.assertEquals("[101, 102, 103]", value1.toString());
        Assert.assertEquals("[201.0, 202.0]", value2.toString());
    }
}
