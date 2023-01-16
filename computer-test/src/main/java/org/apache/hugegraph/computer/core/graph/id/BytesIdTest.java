/*
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

package org.apache.hugegraph.computer.core.graph.id;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.computer.core.graph.value.ValueType;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class BytesIdTest extends UnitTestBase {

    @Test
    public void testLongId() {
        Id longId1 = BytesId.of(123L);
        Id longId2 = BytesId.of(321L);
        Id longId3 = BytesId.of(123L);
        Id longId4 = BytesId.of(322L);

        Id longId5 = BytesId.of(-100L);
        Id longId6 = BytesId.of(0L);
        Id longId7 = BytesId.of(-100L);

        Id longId8 = BytesId.of(0L);

        Assert.assertEquals(IdType.LONG, longId1.idType());
        Assert.assertEquals(ValueType.ID, longId1.valueType());
        Assert.assertEquals(1, longId1.length());

        Assert.assertEquals(IdType.LONG, longId2.idType());
        Assert.assertEquals(ValueType.ID, longId2.valueType());
        Assert.assertEquals(2, longId2.length());

        Assert.assertEquals(new Long(123L), longId1.asObject());
        Assert.assertEquals(123L, longId1.asObject());

        Assert.assertEquals(new Long(-100L), longId5.asObject());
        Assert.assertEquals(-100L, longId5.asObject());

        Assert.assertTrue(longId1.compareTo(longId2) < 0);
        Assert.assertTrue(longId2.compareTo(longId1) > 0);
        Assert.assertEquals(0, longId1.compareTo(longId3));
        Assert.assertTrue(longId2.compareTo(longId4) < 0);

        Assert.assertTrue(longId5.compareTo(longId6) > 0);
        Assert.assertTrue(longId6.compareTo(longId5) < 0);
        Assert.assertEquals(0, longId5.compareTo(longId7));

        Assert.assertEquals(0, longId8.compareTo(longId6));
        Assert.assertTrue(longId8.compareTo(longId1) < 0);
        Assert.assertTrue(longId1.compareTo(longId8) > 0);

        Assert.assertEquals(longId1, longId3);
        Assert.assertNotEquals(longId1, longId2);
        Assert.assertEquals(longId1.hashCode(), longId3.hashCode());
        Assert.assertNotEquals(longId1.hashCode(), longId2.hashCode());
        Assert.assertEquals(longId1, BytesId.of((Long) longId1.asObject()));
    }

    @Test
    public void testUtf8Id() {
        Id utf8Id1 = BytesId.of(Constants.EMPTY_STR);
        Id utf8Id2 = BytesId.of("abc");
        Id utf8Id3 = BytesId.of("abcd");
        Id utf8Id4 = BytesId.of("abd");
        Id utf8Id5 = BytesId.of("abc");
        Id utf8Id6 = BytesId.of("100");
        Id utf8Id7 = new BytesId();

        Assert.assertEquals(IdType.UTF8, utf8Id1.idType());
        Assert.assertEquals(ValueType.ID, utf8Id1.valueType());
        Assert.assertEquals(0, utf8Id1.length());

        Assert.assertEquals(IdType.UTF8, utf8Id2.idType());
        Assert.assertEquals(ValueType.ID, utf8Id2.valueType());

        Assert.assertEquals(IdType.UTF8, utf8Id3.idType());
        Assert.assertEquals(ValueType.ID, utf8Id3.valueType());
        Assert.assertEquals(4, utf8Id3.length());

        Assert.assertEquals(IdType.UTF8, utf8Id7.idType());
        Assert.assertEquals(ValueType.ID, utf8Id7.valueType());
        Assert.assertEquals(0, utf8Id7.length());

        Assert.assertEquals("", utf8Id1.asObject());
        Assert.assertEquals("abc", utf8Id2.asObject());
        Assert.assertEquals("abcd", utf8Id3.asObject());
        Assert.assertEquals("100", utf8Id6.asObject());
        Assert.assertEquals("", utf8Id7.asObject());

        Assert.assertTrue(utf8Id3.compareTo(utf8Id2) > 0);
        Assert.assertTrue(utf8Id2.compareTo(utf8Id3) < 0);
        Assert.assertEquals(0, utf8Id2.compareTo(utf8Id2));
        Assert.assertTrue(utf8Id2.compareTo(utf8Id4) < 0);
        Assert.assertTrue(utf8Id4.compareTo(utf8Id2) > 0);

        Assert.assertEquals(utf8Id2, utf8Id5);
        Assert.assertNotEquals(utf8Id2, utf8Id4);
        Assert.assertEquals(utf8Id1, utf8Id7);

        Assert.assertEquals(utf8Id2.hashCode(), utf8Id5.hashCode());
        Assert.assertNotEquals(utf8Id2.hashCode(), utf8Id3.hashCode());
    }

    @Test
    public void testUuidId() {
        UUID uuid1 = UUID.fromString("55b04935-15de-4ed8-a843-c4919f3b7cf0");
        UUID uuid2 = UUID.fromString("55b04935-15de-4ed8-a843-c4919f3b7cf1");
        Id uuidId1 = BytesId.of(uuid1);
        Id uuidId2 = BytesId.of(uuid2);
        Id uuidId3 = BytesId.of(uuid1);

        Assert.assertEquals(IdType.UUID, uuidId1.idType());
        Assert.assertEquals(ValueType.ID, uuidId1.valueType());
        Assert.assertEquals(19, uuidId1.length());

        Assert.assertEquals(IdType.UUID, uuidId2.idType());
        Assert.assertEquals(ValueType.ID, uuidId2.valueType());
        Assert.assertEquals(19, uuidId2.length());

        Assert.assertEquals(uuid1, uuidId1.asObject());
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putLong(uuid1.getMostSignificantBits());
        buffer.putLong(uuid1.getLeastSignificantBits());

        Assert.assertTrue(uuidId1.compareTo(uuidId2) < 0);
        Assert.assertTrue(uuidId2.compareTo(uuidId1) > 0);
        Assert.assertEquals(0, uuidId1.compareTo(uuidId3));

        Assert.assertEquals(uuidId1, uuidId3);
        Assert.assertNotEquals(uuidId1, uuidId2);
        Assert.assertEquals(uuidId1.hashCode(), uuidId3.hashCode());
        Assert.assertNotEquals(uuidId1.hashCode(), uuidId2.hashCode());
    }

    @Test
    public void testBytesId() {
        Id longId = BytesId.of(1L);
        Id utf8Id = BytesId.of("1");
        UUID uuid = UUID.fromString("55b04935-15de-4ed8-a843-c4919f3b7cf0");
        Id uuidId = BytesId.of(uuid);

        Assert.assertTrue(longId.compareTo(utf8Id) < 0);
        Assert.assertTrue(utf8Id.compareTo(uuidId) < 0);
        Assert.assertTrue(longId.compareTo(uuidId) < 0);

        Assert.assertNotEquals(longId, utf8Id);
        Assert.assertNotEquals(utf8Id, uuidId);
        Assert.assertNotEquals(longId, uuidId);
    }

    @Test
    public void testReadWrite() throws IOException {
        assertIdEqualAfterWriteAndRead(BytesId.of(100L));
        assertIdEqualAfterWriteAndRead(BytesId.of("abc"));
        assertIdEqualAfterWriteAndRead(BytesId.of(UUID.randomUUID()));
    }
}
