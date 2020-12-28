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

package com.baidu.hugegraph.computer.core.common;

import static com.baidu.hugegraph.testutil.Assert.assertEquals;

import org.junit.Test;

public class ValueTypeTest {

    @Test
    public void test() {
        assertEquals(0, ValueType.NULL_VALUE.byteSize());
        assertEquals(8, ValueType.LONG_VALUE.byteSize());
        assertEquals(8, ValueType.DOUBLE_VALUE.byteSize());
        assertEquals(8, ValueType.LONG_ID.byteSize());
        assertEquals(-1, ValueType.TEXT_ID.byteSize());

        assertEquals(NullValue.get(),
                     ValueType.createValue(ValueType.NULL_VALUE));
        assertEquals(new LongValue(),
                     ValueType.createValue(ValueType.LONG_VALUE));
        assertEquals(new DoubleValue(),
                     ValueType.createValue(ValueType.DOUBLE_VALUE));
        assertEquals(new LongId(),
                     ValueType.createValue(ValueType.LONG_ID));
        assertEquals(new TextId(),
                     ValueType.createValue(ValueType.TEXT_ID));

        assertEquals(ValueType.NULL_VALUE,
                     ValueType.fromOrdinal(ValueType.NULL_VALUE.ordinal()));
        assertEquals(ValueType.LONG_VALUE,
                     ValueType.fromOrdinal(ValueType.LONG_VALUE.ordinal()));
        assertEquals(ValueType.DOUBLE_VALUE,
                     ValueType.fromOrdinal(ValueType.DOUBLE_VALUE.ordinal()));
        assertEquals(ValueType.LONG_ID,
                     ValueType.fromOrdinal(ValueType.LONG_ID.ordinal()));
        assertEquals(ValueType.TEXT_ID,
                     ValueType.fromOrdinal(ValueType.TEXT_ID.ordinal()));
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void testException() {
        ValueType type = ValueType.fromOrdinal(1024);
    }
}
