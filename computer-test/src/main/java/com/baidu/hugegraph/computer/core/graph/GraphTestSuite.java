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

package com.baidu.hugegraph.computer.core.graph;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.baidu.hugegraph.computer.core.common.ContainerInfoTest;
import com.baidu.hugegraph.computer.core.graph.id.IdFactoryTest;
import com.baidu.hugegraph.computer.core.graph.id.IdTypeTest;
import com.baidu.hugegraph.computer.core.graph.id.LongIdTest;
import com.baidu.hugegraph.computer.core.graph.id.Utf8IdTest;
import com.baidu.hugegraph.computer.core.graph.id.UuidIdTest;
import com.baidu.hugegraph.computer.core.graph.partition.HashPartitionerTest;
import com.baidu.hugegraph.computer.core.graph.partition.PartitionStatTest;
import com.baidu.hugegraph.computer.core.graph.value.BooleanValueTest;
import com.baidu.hugegraph.computer.core.graph.value.DoubleValueTest;
import com.baidu.hugegraph.computer.core.graph.value.FloatValueTest;
import com.baidu.hugegraph.computer.core.graph.value.IdValueListListTest;
import com.baidu.hugegraph.computer.core.graph.value.IdValueListTest;
import com.baidu.hugegraph.computer.core.graph.value.IdValueTest;
import com.baidu.hugegraph.computer.core.graph.value.IntValueTest;
import com.baidu.hugegraph.computer.core.graph.value.ListValueTest;
import com.baidu.hugegraph.computer.core.graph.value.LongValueTest;
import com.baidu.hugegraph.computer.core.graph.value.NullValueTest;
import com.baidu.hugegraph.computer.core.graph.value.ValueFactoryTest;
import com.baidu.hugegraph.computer.core.graph.value.ValueTypeTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    LongIdTest.class,
    Utf8IdTest.class,
    UuidIdTest.class,
    IdTypeTest.class,
    IdFactoryTest.class,
    NullValueTest.class,
    BooleanValueTest.class,
    IdValueTest.class,
    IntValueTest.class,
    LongValueTest.class,
    FloatValueTest.class,
    DoubleValueTest.class,
    IdValueListTest.class,
    IdValueListListTest.class,
    ListValueTest.class,
    ValueTypeTest.class,
    ValueFactoryTest.class,
    ContainerInfoTest.class,
    PartitionStatTest.class,
    HashPartitionerTest.class,
    SuperstepStatTest.class
})
public class GraphTestSuite {
}
