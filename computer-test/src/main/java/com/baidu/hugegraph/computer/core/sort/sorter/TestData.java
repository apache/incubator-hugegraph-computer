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

package com.baidu.hugegraph.computer.core.sort.sorter;

import java.io.IOException;
import java.util.List;

import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.UnsafeByteArrayInput;
import com.baidu.hugegraph.computer.core.io.UnsafeByteArrayOutput;
import com.baidu.hugegraph.computer.core.store.entry.Pointer;

public class TestData {

    public static UnsafeByteArrayOutput writeMapToOutput(List<Integer> map)
                                                         throws IOException {
        UnsafeByteArrayOutput output = new UnsafeByteArrayOutput();

        for (int i = 0; i < map.size(); ) {
            output.writeInt(Integer.BYTES);
            output.writeInt(map.get(i++));
            output.writeInt(Integer.BYTES);
            output.writeInt(map.get(i++));
        }

        return output;
    }

    public static UnsafeByteArrayInput inputFromMap(List<Integer> map)
                                                    throws IOException {
        UnsafeByteArrayOutput output = writeMapToOutput(map);
        return new UnsafeByteArrayInput(output.buffer(), output.position());
    }

    public static Integer dataFromPointer(Pointer pointer) throws IOException {
        RandomAccessInput input = pointer.input();
        long position = input.position();
        input.seek(pointer.offset());
        int result = input.readInt();
        input.seek(position);
        return result;
    }
}
