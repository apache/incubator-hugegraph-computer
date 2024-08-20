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

package org.apache.hugegraph.computer.core.graph.id;

import org.apache.hugegraph.computer.core.common.SerialEnum;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class IdTypeTest {

    @Test
    public void testCode() {
        Assert.assertEquals(1, IdType.LONG.code());
        Assert.assertEquals(2, IdType.UTF8.code());
        Assert.assertEquals(3, IdType.UUID.code());
    }

    @Test
    public void testFromCode() {
        for (IdType type : IdType.values()) {
            Assert.assertEquals(type, SerialEnum.fromCode(IdType.class,
                                                          type.code()));
        }
    }

    @Test
    public void testException() {
        Assert.assertThrows(ComputerException.class, () -> {
            SerialEnum.fromCode(IdType.class, (byte) -100);
        });
    }
}
