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

package com.baidu.hugegraph.computer.core.graph;

import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import com.baidu.hugegraph.computer.core.suite.unit.UnitTestBase;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.id.IdType;
import com.baidu.hugegraph.computer.core.graph.id.LongId;
import com.baidu.hugegraph.computer.core.graph.id.Utf8Id;
import com.baidu.hugegraph.computer.core.graph.id.UuidId;

public class BuiltinGraphFactoryTest extends UnitTestBase {

    @Test
    public void testCreateLongId() {
        long value = 1L;
        GraphFactory graphFactory = graphFactory();
        Id id = graphFactory.createId(value);
        Assert.assertEquals(IdType.LONG, id.type());
        Assert.assertEquals(new LongId(value), id);
    }

    @Test
    public void testCreateUtf8Id() {
        String value = "John";
        GraphFactory graphFactory = graphFactory();
        Id id = graphFactory.createId(value);
        Assert.assertEquals(IdType.UTF8, id.type());
        Assert.assertEquals(new Utf8Id(value), id);
    }

    @Test
    public void testCreateUuidId() {
        UUID uuid = UUID.randomUUID();
        GraphFactory graphFactory = graphFactory();
        Id id = graphFactory.createId(uuid);
        Assert.assertEquals(IdType.UUID, id.type());
        Assert.assertEquals(new UuidId(uuid), id);
    }
}
