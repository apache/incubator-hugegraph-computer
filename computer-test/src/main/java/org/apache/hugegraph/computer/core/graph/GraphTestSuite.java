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

package org.apache.hugegraph.computer.core.graph;

import org.apache.hugegraph.computer.core.common.ContainerInfoTest;
import org.apache.hugegraph.computer.core.graph.id.BytesIdTest;
import org.apache.hugegraph.computer.core.graph.id.IdFactoryTest;
import org.apache.hugegraph.computer.core.graph.id.IdTypeTest;
import org.apache.hugegraph.computer.core.graph.partition.HashPartitionerTest;
import org.apache.hugegraph.computer.core.graph.partition.PartitionStatTest;
import org.apache.hugegraph.computer.core.graph.value.BooleanValueTest;
import org.apache.hugegraph.computer.core.graph.value.DoubleValueTest;
import org.apache.hugegraph.computer.core.graph.value.FloatValueTest;
import org.apache.hugegraph.computer.core.graph.value.IdListListTest;
import org.apache.hugegraph.computer.core.graph.value.IdValueListTest;
import org.apache.hugegraph.computer.core.graph.value.IdValueTest;
import org.apache.hugegraph.computer.core.graph.value.IntValueTest;
import org.apache.hugegraph.computer.core.graph.value.ListValueTest;
import org.apache.hugegraph.computer.core.graph.value.LongValueTest;
import org.apache.hugegraph.computer.core.graph.value.NullValueTest;
import org.apache.hugegraph.computer.core.graph.value.StringValueTest;
import org.apache.hugegraph.computer.core.graph.value.ValueTypeTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    BytesIdTest.class,
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
    IdListListTest.class,
    ListValueTest.class,
    ValueTypeTest.class,
    StringValueTest.class,
    BuiltinGraphFactoryTest.class,
    ContainerInfoTest.class,
    PartitionStatTest.class,
    HashPartitionerTest.class,
    SuperstepStatTest.class,
    DefaultEdgeTest.class,
    DefaultPropertiesTest.class
})
public class GraphTestSuite {
}
