/*
 *
 *  Copyright 2017 HugeGraph Authors
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements. See the NOTICE file distributed with this
 *  work for additional information regarding copyright ownership. The ASF
 *  licenses this file to You under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations
 *  under the License.
 */

package com.baidu.hugegraph.computer.core.graph.value;

import org.junit.Test;

import com.baidu.hugegraph.computer.core.exception.ComputerException;
import com.baidu.hugegraph.testutil.Assert;

public class ValueTypeTest {

    @Test
    public void test() {
        Assert.assertEquals(0, ValueType.NULL.byteSize());
        Assert.assertEquals(8, ValueType.LONG.byteSize());
        Assert.assertEquals(8, ValueType.DOUBLE.byteSize());
        Assert.assertEquals(-1, ValueType.ID_VALUE.byteSize());

        Assert.assertEquals(NullValue.get(),
                            ValueFactory.createValue(ValueType.NULL));
        Assert.assertEquals(new LongValue(),
                            ValueFactory.createValue(ValueType.LONG));
        Assert.assertEquals(new DoubleValue(),
                            ValueFactory.createValue(ValueType.DOUBLE));
        Assert.assertEquals(new IdValue(),
                            ValueFactory.createValue(ValueType.ID_VALUE));

        for (ValueType type : ValueType.values()) {
            Assert.assertEquals(type, ValueType.fromCode(type.code()));
        }
    }

    @Test
    public void testException() {
        Assert.assertThrows(ComputerException.class, () -> {
            ValueType.fromCode((byte) -100);
        });
    }
}
