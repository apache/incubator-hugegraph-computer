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

package org.apache.hugegraph.computer.core.sort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.hugegraph.computer.core.combiner.AbstractPointerCombiner;
import org.apache.hugegraph.computer.core.combiner.Combiner;
import org.apache.hugegraph.computer.core.combiner.PointerCombiner;
import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.io.BytesInput;
import org.apache.hugegraph.computer.core.io.BytesOutput;
import org.apache.hugegraph.computer.core.io.IOFactory;
import org.apache.hugegraph.computer.core.io.Writable;
import org.apache.hugegraph.computer.core.store.StoreTestUtil;
import org.apache.hugegraph.computer.core.store.entry.EntriesUtil;
import org.apache.hugegraph.computer.core.store.entry.KvEntry;
import org.apache.hugegraph.testutil.Assert;

public class SorterTestUtil {

    public static void writeData(BytesOutput output, Integer value)
                                 throws IOException {
        // Write data length placeholder
        output.writeFixedInt(0);
        long position = output.position();
        // Write data
        output.writeInt(value);
        // Fill data length placeholder
        int dataLength = (int) (output.position() - position);
        output.writeFixedInt(position - Integer.BYTES, dataLength);
    }

    public static BytesOutput writeKvMapToOutput(List<Integer> map)
                                                 throws IOException {
        BytesOutput output = IOFactory.createBytesOutput(
                Constants.SMALL_BUF_SIZE);

        for (int i = 0; i < map.size(); ) {
            writeData(output, map.get(i++));
            writeData(output, map.get(i++));
        }
        return output;
    }

    public static BytesOutput writeSubKvMapToOutput(List<List<Integer>> data)
                                                    throws IOException {
        BytesOutput output = IOFactory.createBytesOutput(
                             Constants.SMALL_BUF_SIZE);
        for (List<Integer> entry : data) {
            Integer key = entry.get(0);
            // Write key
            writeData(output, key);
            // Write subKv
            long position = output.position();
            int count = 0;
            // Write value length placeholder
            output.writeFixedInt(0);
            // Write subKv count placeholder
            output.writeFixedInt(0);
            for (int i = 1; i < entry.size(); ) {
                Integer subKey = entry.get(i++);
                Integer subValue = entry.get(i++);
                writeData(output, subKey);
                writeData(output, subValue);
                count++;
            }
            long currentPosition = output.position();
            output.seek(position);
            output.writeFixedInt((int) (currentPosition - position - 4));
            output.writeFixedInt(count);
            output.seek(currentPosition);
        }
        return output;
    }

    public static BytesInput inputFromKvMap(List<Integer> map)
                                            throws IOException {
        return EntriesUtil.inputFromOutput(writeKvMapToOutput(map));
    }

    public static BytesInput inputFromSubKvMap(List<List<Integer>> map)
                                               throws IOException {
        return EntriesUtil.inputFromOutput(writeSubKvMapToOutput(map));
    }

    public static void assertOutputEqualsMap(BytesOutput output,
                                             Map<Integer, List<Integer>> map)
                                             throws IOException {
        byte[] buffer = output.buffer();
        BytesInput input = IOFactory.createBytesInput(buffer);
        for (Map.Entry<Integer, List<Integer>> entry : map.entrySet()) {
            input.readInt();
            int key = input.readInt();
            Assert.assertEquals(entry.getKey().intValue(), key);
            int valueLength = input.readFixedInt();
            int valueCount = valueLength / Integer.BYTES;
            List<Integer> values = new ArrayList<>();
            for (int i = 0; i < valueCount; i++) {
                values.add(input.readInt());
            }
            Assert.assertEquals(entry.getValue(), values);
        }
    }

    public static void assertKvEntry(KvEntry entry, Id expectKey,
                                     Id expectValue) throws IOException {
        Assert.assertEquals(expectKey,
                            StoreTestUtil.idFromPointer(entry.key()));
        Assert.assertEquals(expectValue,
                            StoreTestUtil.idFromPointer(entry.value()));
    }

    public static void assertKvEntry(KvEntry entry, Integer expectKey,
                                     Integer expectValue)
                                     throws IOException {
        Assert.assertEquals(expectKey,
                            StoreTestUtil.dataFromPointer(entry.key()));
        Assert.assertEquals(expectValue,
                            StoreTestUtil.dataFromPointer(entry.value()));
    }

    public static void assertSubKvByKv(KvEntry entry, int... expect)
                                       throws IOException {
        int index = 0;
        Assert.assertEquals(expect[index++],
                            StoreTestUtil.dataFromPointer(entry.key()));

        Iterator<KvEntry> subKvs = EntriesUtil.subKvIterFromEntry(entry);
        while (subKvs.hasNext()) {
            KvEntry subKv = subKvs.next();
            Assert.assertEquals(expect[index++],
                                StoreTestUtil.dataFromPointer(subKv.key()));
            Assert.assertEquals(expect[index++],
                                StoreTestUtil.dataFromPointer(subKv.value()));
        }
    }

    public static <T extends Readable & Writable> PointerCombiner
                  createPointerCombiner(Supplier<T> supplier,
                                        Combiner<T> combiner) {
        return new AbstractPointerCombiner<T>(supplier, combiner) { };
    }

    public static Sorter createSorter(Config config) {
        if (config.get(ComputerOptions.TRANSPORT_RECV_FILE_MODE)) {
            return new BufferFileSorter(config);
        } else {
            return new HgkvFileSorter(config);
        }
    }
}
