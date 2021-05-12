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

package com.baidu.hugegraph.computer.core.sort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.UnsafeBytesInput;
import com.baidu.hugegraph.computer.core.io.UnsafeBytesOutput;
import com.baidu.hugegraph.computer.core.store.StoreTestUtil;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.testutil.Assert;

public class SorterTestUtil {

    public static UnsafeBytesOutput writeMapToOutput(List<Integer> map)
                                                         throws IOException {
        UnsafeBytesOutput output = new UnsafeBytesOutput();

        for (int i = 0; i < map.size(); ) {
            output.writeInt(Integer.BYTES);
            output.writeInt(map.get(i++));
            output.writeInt(Integer.BYTES);
            output.writeInt(map.get(i++));
        }

        return output;
    }

    public static UnsafeBytesInput inputFromMap(List<Integer> map)
                                                    throws IOException {
        UnsafeBytesOutput output = writeMapToOutput(map);
        return new UnsafeBytesInput(output.buffer(), output.position());
    }

    public static void assertOutputEqualsMap(UnsafeBytesOutput output,
                                             Map<Integer, List<Integer>> map)
                                             throws IOException {
        byte[] buffer = output.buffer();
        RandomAccessInput input = new UnsafeBytesInput(buffer);
        for (Map.Entry<Integer, List<Integer>> entry : map.entrySet()) {
            input.readInt();
            int key = input.readInt();
            Assert.assertEquals(entry.getKey().intValue(), key);
            int valueLength = input.readInt();
            int valueCount = valueLength / Integer.BYTES;
            List<Integer> values = new ArrayList<>();
            for (int i = 0; i < valueCount; i++) {
                values.add(input.readInt());
            }
            Assert.assertEquals(entry.getValue(), values);
        }
    }

    public static void assertKvEntry(KvEntry entry, Integer expectKey,
                                            Integer expectValues)
            throws IOException {
        Assert.assertEquals(expectKey,
                            StoreTestUtil.dataFromPointer(entry.key()));
        Assert.assertEquals(expectValues,
                            StoreTestUtil.dataFromPointer(entry.value()));
    }
}
