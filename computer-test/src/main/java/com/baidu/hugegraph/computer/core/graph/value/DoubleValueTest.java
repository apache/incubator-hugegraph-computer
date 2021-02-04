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

public class DoubleValueTest extends UnitTestBase {

    @Test
    public void test() {
        DoubleValue doubleValue1 = new DoubleValue();
        DoubleValue doubleValue2 = new DoubleValue(Double.MIN_VALUE);

        Assert.assertEquals(ValueType.DOUBLE, doubleValue1.type());
        Assert.assertEquals(0.0D, doubleValue1.value(), 0.0D);
        Assert.assertEquals(Double.MIN_VALUE, doubleValue2.value(), 0.0D);

        doubleValue2.value(Double.MAX_VALUE);
        Assert.assertEquals(Double.MAX_VALUE, doubleValue2.value(), 0.0D);
        Assert.assertNotEquals(doubleValue1, doubleValue2);
        Assert.assertEquals(doubleValue2,
                            new DoubleValue(doubleValue2.value()));
        Assert.assertEquals(Double.hashCode(Double.MAX_VALUE),
                            doubleValue2.hashCode());
    }

    @Test
    public void testReadWrite() throws IOException {
        assertValueEqualAfterWriteAndRead(new DoubleValue(0.0D));
        assertValueEqualAfterWriteAndRead(new DoubleValue(-0.0D));
        assertValueEqualAfterWriteAndRead(new DoubleValue(0.1D));
        assertValueEqualAfterWriteAndRead(new DoubleValue(-0.1D));
        assertValueEqualAfterWriteAndRead(new DoubleValue(1.1D));
        assertValueEqualAfterWriteAndRead(new DoubleValue(-1.1D));
        assertValueEqualAfterWriteAndRead(new DoubleValue(1.123456789D));
        assertValueEqualAfterWriteAndRead(new DoubleValue(-1.123456789D));
        assertValueEqualAfterWriteAndRead(new DoubleValue(
                                          987654321.123456789D));
        assertValueEqualAfterWriteAndRead(new DoubleValue(
                                          -987654321.123456789D));
        assertValueEqualAfterWriteAndRead(new DoubleValue(127D));
        assertValueEqualAfterWriteAndRead(new DoubleValue(-127D));
        assertValueEqualAfterWriteAndRead(new DoubleValue(128D));
        assertValueEqualAfterWriteAndRead(new DoubleValue(-128D));
        assertValueEqualAfterWriteAndRead(new DoubleValue(256D));
        assertValueEqualAfterWriteAndRead(new DoubleValue(-256D));
        assertValueEqualAfterWriteAndRead(new DoubleValue(32767D));
        assertValueEqualAfterWriteAndRead(new DoubleValue(-32767D));
        assertValueEqualAfterWriteAndRead(new DoubleValue(32768D));
        assertValueEqualAfterWriteAndRead(new DoubleValue(-32768D));
        assertValueEqualAfterWriteAndRead(new DoubleValue(65536D));
        assertValueEqualAfterWriteAndRead(new DoubleValue(-65535D));
        assertValueEqualAfterWriteAndRead(new DoubleValue(Integer.MIN_VALUE));
        assertValueEqualAfterWriteAndRead(new DoubleValue(Integer.MAX_VALUE));
        assertValueEqualAfterWriteAndRead(new DoubleValue(Long.MIN_VALUE));
        assertValueEqualAfterWriteAndRead(new DoubleValue(Long.MAX_VALUE));
        assertValueEqualAfterWriteAndRead(new DoubleValue(Float.MIN_VALUE));
        assertValueEqualAfterWriteAndRead(new DoubleValue(Float.MAX_VALUE));
        assertValueEqualAfterWriteAndRead(new DoubleValue(Double.MIN_VALUE));
        assertValueEqualAfterWriteAndRead(new DoubleValue(Double.MAX_VALUE));
    }

    @Test
    public void testCompare() {
        DoubleValue value1 = new DoubleValue(123.0D);
        DoubleValue value2 = new DoubleValue(123.0D);
        DoubleValue value3 = new DoubleValue(321.0D);
        Assert.assertEquals(0, value1.compareTo(value2));
        Assert.assertLt(0, value1.compareTo(value3));
        Assert.assertGt(0, value3.compareTo(value1));
    }
}
