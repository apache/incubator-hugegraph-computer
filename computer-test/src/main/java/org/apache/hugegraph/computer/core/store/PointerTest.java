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

package org.apache.hugegraph.computer.core.store;

import java.io.IOException;

import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.computer.core.io.BytesInput;
import org.apache.hugegraph.computer.core.io.BytesOutput;
import org.apache.hugegraph.computer.core.io.IOFactory;
import org.apache.hugegraph.computer.core.store.entry.EntriesUtil;
import org.apache.hugegraph.computer.core.store.entry.KvEntry;
import org.apache.hugegraph.computer.core.store.entry.Pointer;
import org.apache.hugegraph.computer.core.util.BytesUtil;
import org.junit.Assert;
import org.junit.Test;

public class PointerTest {

    @Test
    public void test() throws IOException {
        byte[] data = new byte[]{100, 0, 0, 0};
        byte[] expectedWriteResult = {4, 0, 0, 0, 100, 0, 0, 0,
                                      4, 0, 0, 0, 100, 0, 0, 0};
        BytesOutput output = IOFactory.createBytesOutput(
                Constants.SMALL_BUF_SIZE);
        output.writeFixedInt(data.length);
        output.write(data);
        output.writeFixedInt(data.length);
        output.write(data);

        BytesInput input = EntriesUtil.inputFromOutput(output);
        KvEntry inlineKvEntry = EntriesUtil.kvEntryFromInput(input, true,
                                                             false);
        Pointer inlineKey = inlineKvEntry.key();
        Pointer inlineValue = inlineKvEntry.value();
        Assert.assertEquals(0L, inlineKey.offset());
        Assert.assertEquals(4L, inlineKey.length());
        Assert.assertEquals(0, BytesUtil.compare(data, inlineKey.bytes()));
        Assert.assertEquals(0L, inlineValue.offset());
        Assert.assertEquals(4L, inlineValue.length());
        Assert.assertEquals(0, BytesUtil.compare(data, inlineValue.bytes()));

        BytesOutput writeOutput = IOFactory.createBytesOutput(
                                  Constants.SMALL_BUF_SIZE);
        inlineKey.write(writeOutput);
        inlineValue.write(writeOutput);
        int result = BytesUtil.compare(expectedWriteResult,
                                       expectedWriteResult.length,
                                       writeOutput.buffer(),
                                       (int) writeOutput.position());
        Assert.assertEquals(0, result);

        input.seek(0);

        KvEntry cachedKvEntry = EntriesUtil.kvEntryFromInput(input, false,
                                                             false);
        Pointer cachedKey = cachedKvEntry.key();
        Pointer cachedValue = cachedKvEntry.value();
        Assert.assertEquals(4L, cachedKey.offset());
        Assert.assertEquals(4L, cachedKey.length());
        Assert.assertEquals(0, BytesUtil.compare(data, cachedKey.bytes()));
        Assert.assertEquals(12L, cachedValue.offset());
        Assert.assertEquals(4L, cachedValue.length());
        Assert.assertEquals(0, BytesUtil.compare(data, cachedValue.bytes()));

        writeOutput = IOFactory.createBytesOutput(Constants.SMALL_BUF_SIZE);
        cachedKey.write(writeOutput);
        cachedValue.write(writeOutput);
        result = BytesUtil.compare(expectedWriteResult,
                                   expectedWriteResult.length,
                                   writeOutput.buffer(),
                                   (int) writeOutput.position());
        Assert.assertEquals(0, result);
    }
}
