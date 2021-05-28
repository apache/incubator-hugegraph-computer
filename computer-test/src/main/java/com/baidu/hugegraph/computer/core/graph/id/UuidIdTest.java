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

package com.baidu.hugegraph.computer.core.graph.id;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.junit.Test;

import com.baidu.hugegraph.computer.core.suite.unit.UnitTestBase;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.graph.value.IdValue;
import com.baidu.hugegraph.computer.core.graph.value.ValueType;
import com.baidu.hugegraph.computer.core.util.IdValueUtil;
import com.baidu.hugegraph.testutil.Assert;

public class UuidIdTest extends UnitTestBase {

    @Test
    public void test() {
        UUID uuid1 = UUID.fromString("55b04935-15de-4ed8-a843-c4919f3b7cf0");
        UUID uuid2 = UUID.fromString("55b04935-15de-4ed8-a843-c4919f3b7cf1");
        UuidId uuidId1 = new UuidId(uuid1);
        UuidId uuidId2 = new UuidId(uuid2);
        UuidId uuidId3 = new UuidId(uuid1);

        Assert.assertEquals(IdType.UUID, uuidId1.type());
        IdValue idValue = uuidId1.idValue();
        Assert.assertEquals(ValueType.ID_VALUE, idValue.type());
        Assert.assertEquals(uuidId1, IdValueUtil.toId(idValue));

        Assert.assertEquals(uuid1, uuidId1.asObject());
        Assert.assertThrows(ComputerException.class, () -> {
            uuidId1.asLong();
        }, e -> {
            Assert.assertTrue(e.getMessage().contains(
                              "Can't convert UuidId to long"));
        });
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putLong(uuid1.getMostSignificantBits());
        buffer.putLong(uuid1.getLeastSignificantBits());
        Assert.assertArrayEquals(buffer.array(), uuidId1.asBytes());

        Assert.assertTrue(uuidId1.compareTo(uuidId2) < 0);
        Assert.assertTrue(uuidId2.compareTo(uuidId1) > 0);
        Assert.assertTrue(uuidId1.compareTo(uuidId3) == 0);

        Assert.assertEquals(uuidId1, uuidId3);
        Assert.assertNotEquals(uuidId1, uuidId2);
        Assert.assertEquals(uuid1.hashCode(), uuidId3.hashCode());
        Assert.assertNotEquals(uuid1.hashCode(), uuidId2.hashCode());
    }

    @Test
    public void testReadWrite() throws IOException {
        assertIdEqualAfterWriteAndRead(new UuidId(UUID.randomUUID()));
    }
}
