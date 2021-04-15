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

package com.baidu.hugegraph.computer.core.sort.util;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.store.EntriesKeyInput;
import com.baidu.hugegraph.computer.core.store.base.DefaultPointer;
import com.baidu.hugegraph.computer.core.store.base.Pointer;

public class EntriesUtil {

    private static final int DEFAULT_CAPACITY = 100000;

    public static List<Pointer> splitInput(RandomAccessInput input) {
        List<Pointer> pointers = new ArrayList<>(DEFAULT_CAPACITY);
        EntriesKeyInput entriesKeyInput = new EntriesKeyInput(input);
        while (entriesKeyInput.hasNext()) {
            pointers.add(entriesKeyInput.next());
        }
        return pointers;
    }

    public static Pointer valuePointerByKeyPointer(Pointer keyPointer)
                                                   throws IOException {
        RandomAccessInput input = keyPointer.input();
        long positionTemp = input.position();

        input.seek(keyPointer.offset());
        input.skip(keyPointer.length());

        int valueLength = input.readInt();
        Pointer value = new DefaultPointer(input, input.position(),
                                           valueLength);
        input.seek(positionTemp);
        return value;
    }
}
