/*
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

package org.apache.hugegraph.computer.core.graph.value;

import org.apache.hugegraph.computer.core.common.SerialEnum;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class ValueTypeTest {

    @Test
    public void testCodeAndByteSize() {
        Assert.assertEquals(1, ValueType.NULL.code());
        Assert.assertEquals(2, ValueType.BOOLEAN.code());
        Assert.assertEquals(3, ValueType.INT.code());
        Assert.assertEquals(4, ValueType.LONG.code());
        Assert.assertEquals(5, ValueType.FLOAT.code());
        Assert.assertEquals(6, ValueType.DOUBLE.code());
        Assert.assertEquals(20, ValueType.ID.code());

        Assert.assertEquals(0, ValueType.NULL.byteSize());
        Assert.assertEquals(1, ValueType.BOOLEAN.byteSize());
        Assert.assertEquals(4, ValueType.INT.byteSize());
        Assert.assertEquals(8, ValueType.LONG.byteSize());
        Assert.assertEquals(4, ValueType.FLOAT.byteSize());
        Assert.assertEquals(8, ValueType.DOUBLE.byteSize());
        Assert.assertEquals(-1, ValueType.ID.byteSize());
    }

    @Test
    public void testFromCode() {
        for (ValueType type : ValueType.values()) {
            Assert.assertEquals(type, SerialEnum.fromCode(ValueType.class,
                                                          type.code()));
        }
    }

    @Test
    public void testException() {
        Assert.assertThrows(ComputerException.class, () -> {
            SerialEnum.fromCode(ValueType.class, (byte) -100);
        });
    }
}
