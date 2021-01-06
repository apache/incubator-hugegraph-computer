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

import com.baidu.hugegraph.computer.core.BaseCoreTest;
import com.baidu.hugegraph.testutil.Assert;

public class DoubleValueTest extends BaseCoreTest {

    @Test
    public void test() {
        DoubleValue doubleValue1 = new DoubleValue();
        DoubleValue doubleValue2 = new DoubleValue(Double.MIN_VALUE);

        Assert.assertEquals(ValueType.DOUBLE, doubleValue1.type());
        Assert.assertEquals(0.0D, doubleValue1.value(), 0.0);
        Assert.assertEquals(Double.MIN_VALUE, doubleValue2.value(), 0.0);

        doubleValue2.value(Double.MAX_VALUE);
        Assert.assertEquals(Double.MAX_VALUE, doubleValue2.value(), 0.0);
        Assert.assertNotEquals(doubleValue1, doubleValue2);
        Assert.assertEquals(doubleValue2,
                            new DoubleValue(doubleValue2.value()));
        Assert.assertEquals(Double.hashCode(Double.MAX_VALUE),
                            doubleValue2.hashCode());
    }

    @Test
    public void testReadWrite() throws IOException {
        testReadWrite(new DoubleValue(Double.MAX_VALUE), new DoubleValue());
    }
}
