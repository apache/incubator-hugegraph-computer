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
    public void test() {
        FloatValue floatValue1 = new FloatValue();
        FloatValue floatValue2 = new FloatValue(Float.MIN_VALUE);

        Assert.assertEquals(ValueType.FLOAT, floatValue1.type());
        Assert.assertEquals(0.0F, floatValue1.value(), 0.0F);
        Assert.assertEquals(Float.MIN_VALUE, floatValue2.value(), 0.0D);

        floatValue2.value(Float.MAX_VALUE);
        Assert.assertEquals(Float.MAX_VALUE, floatValue2.value(), 0.0D);
        Assert.assertNotEquals(floatValue1, floatValue2);
        Assert.assertEquals(floatValue2, new FloatValue(floatValue2.value()));
        Assert.assertEquals(Float.hashCode(Float.MAX_VALUE),
                            floatValue2.hashCode());
    }

    @Test
    public void testReadWrite() throws IOException {
        assertValueEqualAfterWriteAndRead(new FloatValue(Float.MAX_VALUE));
    }

    @Test
    public void testCompare() {
        FloatValue value1 = new FloatValue(123.0F);
        FloatValue value2 = new FloatValue(123.0F);
        FloatValue value3 = new FloatValue(321.0F);
        Assert.assertEquals(0, value1.compareTo(value2));
        Assert.assertTrue(value1.compareTo(value3) < 0);
        Assert.assertTrue(value3.compareTo(value1) > 0);
    }
}
