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

public class BooleanValueTest extends UnitTestBase {

    @Test
    public void test() {
        BooleanValue booleanValue1 = new BooleanValue();
        BooleanValue booleanValue2 = new BooleanValue(true);

        Assert.assertEquals(ValueType.BOOLEAN, booleanValue1.type());
        Assert.assertEquals(false, booleanValue1.value());
        Assert.assertEquals(true, booleanValue2.value());

        booleanValue2.value(false);
        Assert.assertEquals(false, booleanValue2.value());
        Assert.assertEquals(booleanValue2,
                            new BooleanValue(booleanValue2.value()));
        Assert.assertEquals(booleanValue1, booleanValue2);
        Assert.assertEquals(Boolean.hashCode(false),
                            booleanValue2.hashCode());
    }

    @Test
    public void testReadWrite() throws IOException {
        assertValueEqualAfterWriteAndRead(new BooleanValue(true));
    }

    @Test
    public void testCompare() {
        BooleanValue value1 = BooleanValue.FALSE;
        BooleanValue value2 = BooleanValue.FALSE;
        BooleanValue value3 = BooleanValue.TRUE;
        Assert.assertEquals(0, value1.compareTo(value2));
        Assert.assertTrue(value1.compareTo(value3) < 0);
        Assert.assertTrue(value3.compareTo(value1) > 0);
    }
}
