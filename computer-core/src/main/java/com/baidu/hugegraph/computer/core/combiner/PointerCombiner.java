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

package com.baidu.hugegraph.computer.core.combiner;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.io.OptimizedUnsafeBytesOutput;
import com.baidu.hugegraph.computer.core.io.Readable;
import com.baidu.hugegraph.computer.core.io.Writable;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.InlinePointer;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.Pointer;

public class PointerCombiner<V extends Readable & Writable>
       implements Combiner<Pointer> {

    private final V v1;
    private final V v2;
    private final Combiner<V> combiner;
    private final OptimizedUnsafeBytesOutput bytesOutput;

    public PointerCombiner(V v1, V v2, Combiner<V> combiner) {
        this.v1 = v1;
        this.v2 = v2;
        this.combiner = combiner;
        this.bytesOutput = new OptimizedUnsafeBytesOutput();
    }

    @Override
    public Pointer combine(Pointer v1, Pointer v2) {
        try {
            this.v1.read(v1.input());
            this.v2.read(v2.input());
            V combinedValue = this.combiner.combine(this.v1, this.v2);
            this.bytesOutput.seek(0L);
            combinedValue.write(this.bytesOutput);
            return new InlinePointer(this.bytesOutput.buffer(),
                                     this.bytesOutput.position());
        } catch (Exception e) {
            throw new ComputerException(
                      "Failed to combine pointer1(offset=%s, length=%s) and" +
                      " pointer2(offset=%s, length=%s)'",
                      e, v1.offset(), v1.length(), v2.offset(), v2.length());
        }
    }
}
