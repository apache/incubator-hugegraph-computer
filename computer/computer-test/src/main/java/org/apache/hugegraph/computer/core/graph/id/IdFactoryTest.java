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

import java.util.UUID;

import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class IdFactoryTest {

    @Test
    public void testCreateIdFromCode() {
        Assert.assertEquals(IdType.LONG,
                            IdFactory.createId(IdType.LONG.code()).idType());
        Assert.assertEquals(IdType.UTF8,
                            IdFactory.createId(IdType.UTF8.code()).idType());
        Assert.assertEquals(IdType.UUID,
                            IdFactory.createId(IdType.UUID.code()).idType());
    }

    @Test
    public void testCreateIdFromType() {
        Assert.assertEquals(IdType.LONG,
                            IdFactory.createId(IdType.LONG).idType());
        Assert.assertEquals(IdType.UTF8,
                            IdFactory.createId(IdType.UTF8).idType());
        Assert.assertEquals(IdType.UUID,
                            IdFactory.createId(IdType.UUID).idType());

        Assert.assertEquals(BytesId.of(0L), IdFactory.createId(IdType.LONG));
        Assert.assertEquals(BytesId.of(Constants.EMPTY_STR),
                            IdFactory.createId(IdType.UTF8));
        Assert.assertEquals(BytesId.of(new UUID(0L, 0L)),
                            IdFactory.createId(IdType.UUID));
    }

    @Test
    public void testParseId() {
        UUID uuid = UUID.fromString("3b676b77-c484-4ba6-b627-8c040bc42863");
        Assert.assertEquals(IdType.LONG, IdFactory.parseId(IdType.LONG, 222).idType());
        Assert.assertEquals(IdType.UTF8, IdFactory.parseId(IdType.UTF8, "aaa222").idType());
        Assert.assertEquals(IdType.UUID, IdFactory.parseId(IdType.UUID, uuid).idType());
    }
}
