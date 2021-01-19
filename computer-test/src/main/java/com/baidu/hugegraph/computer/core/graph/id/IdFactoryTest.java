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

package com.baidu.hugegraph.computer.core.graph.id;

import org.junit.Test;

import com.baidu.hugegraph.testutil.Assert;

public class IdFactoryTest {

    @Test
    public void testCreateIdFromCode() {
        Assert.assertEquals(IdType.LONG,
                            IdFactory.createID(IdType.LONG.code()).type());
        Assert.assertEquals(IdType.UTF8,
                            IdFactory.createID(IdType.UTF8.code()).type());
        Assert.assertEquals(IdType.UUID,
                            IdFactory.createID(IdType.UUID.code()).type());
    }

    @Test
    public void testCreateIdFromType() {
        Assert.assertEquals(IdType.LONG,
                            IdFactory.createID(IdType.LONG).type());
        Assert.assertEquals(IdType.UTF8,
                            IdFactory.createID(IdType.UTF8).type());
        Assert.assertEquals(IdType.UUID,
                            IdFactory.createID(IdType.UUID).type());

        Assert.assertEquals(new LongId(), IdFactory.createID(IdType.LONG));
        Assert.assertEquals(new Utf8Id(), IdFactory.createID(IdType.UTF8));
        Assert.assertEquals(new UuidId(), IdFactory.createID(IdType.UUID));
    }
}
