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

package org.apache.hugegraph.computer.core.combiner;

import java.io.IOException;
import java.util.Map;

import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.graph.GraphFactory;
import org.apache.hugegraph.computer.core.graph.properties.Properties;
import org.apache.hugegraph.computer.core.graph.value.DoubleValue;
import org.apache.hugegraph.computer.core.graph.value.LongValue;
import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.computer.core.io.BytesInput;
import org.apache.hugegraph.computer.core.io.BytesOutput;
import org.apache.hugegraph.computer.core.io.IOFactory;
import org.apache.hugegraph.computer.core.sort.SorterTestUtil;
import org.apache.hugegraph.computer.core.store.entry.InlinePointer;
import org.apache.hugegraph.computer.core.store.entry.Pointer;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class PointerCombinerTest extends UnitTestBase {

    @Test
    public void testMessageCombiner() throws IOException {
        Config config = UnitTestBase.updateWithRequiredOptions(
            ComputerOptions.WORKER_COMBINER_CLASS,
            DoubleValueSumCombiner.class.getName()
        );
        Combiner<DoubleValue> valueCombiner = config.createObject(
                              ComputerOptions.WORKER_COMBINER_CLASS);

        PointerCombiner combiner = SorterTestUtil.createPointerCombiner(
                                                  DoubleValue::new,
                                                  new DoubleValueSumCombiner());

        try (BytesOutput bytesOutput1 = IOFactory.createBytesOutput(
                                        Constants.SMALL_BUF_SIZE);
             BytesOutput bytesOutput2 = IOFactory.createBytesOutput(
                                        Constants.SMALL_BUF_SIZE);) {
            DoubleValue value1 = new DoubleValue(1.0D);
            DoubleValue value2 = new DoubleValue(2.0D);
            value1.write(bytesOutput1);
            value2.write(bytesOutput2);

            Pointer pointer1 = new InlinePointer(bytesOutput1.buffer(),
                                                 bytesOutput1.position());

            Pointer pointer2 = new InlinePointer(bytesOutput2.buffer(),
                                                 bytesOutput2.position());

            Pointer pointer = combiner.combine(pointer1, pointer2);

            BytesInput input = IOFactory.createBytesInput(pointer.bytes());

            DoubleValue combinedValue = new DoubleValue();
            combinedValue.read(input);
            Assert.assertEquals(new DoubleValue(3.0D), combinedValue);
        }
    }

    @Test
    public void testVertexPropertiesCombiner() throws IOException {
        Config config = UnitTestBase.updateWithRequiredOptions(
            ComputerOptions.WORKER_COMBINER_CLASS,
            DoubleValueSumCombiner.class.getName(),
            ComputerOptions.WORKER_VERTEX_PROPERTIES_COMBINER_CLASS,
            MergeOldPropertiesCombiner.class.getName()
        );
        Combiner<Properties> valueCombiner = config.createObject(
        ComputerOptions.WORKER_VERTEX_PROPERTIES_COMBINER_CLASS);

        GraphFactory graphFactory = graphFactory();
        PointerCombiner combiner =
                        SorterTestUtil.createPointerCombiner(
                                       graphFactory::createProperties,
                                       valueCombiner);

        try (BytesOutput bytesOutput1 = IOFactory.createBytesOutput(
                                        Constants.SMALL_BUF_SIZE);
             BytesOutput bytesOutput2 = IOFactory.createBytesOutput(
                                        Constants.SMALL_BUF_SIZE)) {
            Properties value1 = graphFactory.createProperties();
            value1.put("p1", new LongValue(1L));
            Properties value2 = graphFactory.createProperties();
            value2.put("p2", new LongValue(2L));
            value1.write(bytesOutput1);
            value2.write(bytesOutput2);

            Pointer pointer1 = new InlinePointer(bytesOutput1.buffer(),
                                                 bytesOutput1.position());

            Pointer pointer2 = new InlinePointer(bytesOutput2.buffer(),
                                                 bytesOutput2.position());

            Pointer pointer = combiner.combine(pointer1, pointer2);

            BytesInput input = IOFactory.createBytesInput(pointer.bytes());

            Properties combinedValue = graphFactory.createProperties();
            combinedValue.read(input);
            Map<String, Value> map = combinedValue.get();
            Assert.assertEquals(2, map.size());
            Assert.assertEquals(new LongValue(1L), map.get("p1"));
            Assert.assertEquals(new LongValue(2L), map.get("p2"));
        }
    }

    @Test
    public void testCombineEdgePropertiesFail() throws IOException {
        Config config = UnitTestBase.updateWithRequiredOptions(
            ComputerOptions.WORKER_COMBINER_CLASS,
            DoubleValueSumCombiner.class.getName(),
            ComputerOptions.WORKER_EDGE_PROPERTIES_COMBINER_CLASS,
            MergeOldPropertiesCombiner.class.getName()
        );
        Combiner<Properties> valueCombiner = config.createObject(
        ComputerOptions.WORKER_EDGE_PROPERTIES_COMBINER_CLASS);

        GraphFactory graphFactory = graphFactory();

        PointerCombiner combiner =
                        SorterTestUtil.createPointerCombiner(
                                       graphFactory::createProperties,
                                       valueCombiner);

        try (BytesOutput bytesOutput1 = IOFactory.createBytesOutput(
                                        Constants.SMALL_BUF_SIZE);
             BytesOutput bytesOutput2 = IOFactory.createBytesOutput(
                                        Constants.SMALL_BUF_SIZE)) {
            Properties value1 = graphFactory.createProperties();
            value1.put("p1", new LongValue(1L));
            Properties value2 = graphFactory.createProperties();
            value2.put("p2", new LongValue(2L));
            // Only write count.
            bytesOutput1.writeInt(1);
            value2.write(bytesOutput2);

            Pointer pointer1 = new InlinePointer(bytesOutput1.buffer(),
                                                 bytesOutput1.position());

            Pointer pointer2 = new InlinePointer(bytesOutput2.buffer(),
                                                 bytesOutput2.position());

            Assert.assertThrows(ComputerException.class, () -> {
                combiner.combine(pointer1, pointer2);
            }, e -> {
                Assert.assertContains("Failed to combine pointer",
                                      e.getMessage());
            });
        }
    }
}
