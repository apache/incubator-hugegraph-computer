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

public class IntValueTest extends UnitTestBase {

    @Test
    public void test() {
        IntValue intValue1 = new IntValue();
        IntValue intValue2 = new IntValue(Integer.MIN_VALUE);

        Assert.assertEquals(ValueType.INT, intValue1.type());
        Assert.assertEquals(0, intValue1.value());
        Assert.assertEquals(Integer.MIN_VALUE, intValue2.value());

        intValue2.value(Integer.MAX_VALUE);
        Assert.assertEquals(Integer.MAX_VALUE, intValue2.value());
        Assert.assertEquals(intValue2, new IntValue(intValue2.value()));
        Assert.assertNotEquals(intValue1, intValue2);
        Assert.assertEquals(Integer.hashCode(Integer.MAX_VALUE),
                            intValue2.hashCode());
    }

    @Test
    public void testReadWrite() throws IOException {
        assertValueEqualAfterWriteAndRead(new IntValue(Integer.MAX_VALUE));
    }

    @Test
    public void testCompare() {
        IntValue value1 = new IntValue(123);
        IntValue value2 = new IntValue(123);
        IntValue value3 = new IntValue(321);
        Assert.assertEquals(0, value1.compareTo(value2));
        Assert.assertLt(0, value1.compareTo(value3));
        Assert.assertGt(0, value3.compareTo(value1));
    }
}
