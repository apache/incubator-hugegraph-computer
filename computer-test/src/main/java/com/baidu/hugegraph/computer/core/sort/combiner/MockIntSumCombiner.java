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

package com.baidu.hugegraph.computer.core.sort.combiner;

import java.io.IOException;

import com.baidu.hugegraph.computer.core.combiner.Combiner;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.store.StoreTestUtil;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.InlinePointer;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.Pointer;

public class MockIntSumCombiner implements Combiner<Pointer> {

    @Override
    public Pointer combine(Pointer v1, Pointer v2) {
        try {
            Integer d1 = StoreTestUtil.dataFromPointer(v1);
            Integer d2 = StoreTestUtil.dataFromPointer(v2);

            return new InlinePointer(StoreTestUtil.intToByteArray(d1 + d2));
        } catch (IOException e) {
            throw new ComputerException(e.getMessage(), e);
        }
    }
}
