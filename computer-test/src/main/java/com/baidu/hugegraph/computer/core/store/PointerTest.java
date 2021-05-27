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

package com.baidu.hugegraph.computer.core.store;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import com.baidu.hugegraph.computer.core.io.UnsafeBytesInput;
import com.baidu.hugegraph.computer.core.io.UnsafeBytesOutput;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.EntriesUtil;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.Pointer;
import com.baidu.hugegraph.computer.core.util.BytesUtil;

public class PointerTest {

    @Test
    public void test() throws IOException {
        byte[] bytes = new byte[]{100, 0, 0, 0};
        UnsafeBytesOutput output = new UnsafeBytesOutput();
        output.writeInt(Integer.BYTES);
        output.write(bytes);
        output.writeInt(Integer.BYTES);
        output.write(bytes);

        UnsafeBytesInput input = EntriesUtil.inputFromOutput(output);
        KvEntry inlineKvEntry = EntriesUtil.entryFromInput(input, true, false);
        Pointer inlineKey = inlineKvEntry.key();
        Pointer inlineValue = inlineKvEntry.value();
        Assert.assertEquals(0L, inlineKey.offset());
        Assert.assertEquals(4, inlineKey.length());
        Assert.assertEquals(0, BytesUtil.compare(bytes,
                                                          inlineKey.bytes()));
        Assert.assertEquals(0L, inlineValue.offset());
        Assert.assertEquals(4, inlineValue.length());
        Assert.assertEquals(0, BytesUtil.compare(bytes,
                                                          inlineValue.bytes()));

        input.seek(0);

        KvEntry cachedKvEntry = EntriesUtil.entryFromInput(input, false, false);
        Pointer cachedKey = cachedKvEntry.key();
        Pointer cachedValue = cachedKvEntry.value();
        Assert.assertEquals(4, cachedKey.offset());
        Assert.assertEquals(4, cachedKey.length());
        Assert.assertEquals(0, BytesUtil.compare(bytes,
                                                          cachedKey.bytes()));
        Assert.assertEquals(12, cachedValue.offset());
        Assert.assertEquals(4, cachedValue.length());
        Assert.assertEquals(0, BytesUtil.compare(bytes,
                                                          cachedValue.bytes()));
    }
}
