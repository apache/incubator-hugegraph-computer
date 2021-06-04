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

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.io.BytesInput;
import com.baidu.hugegraph.computer.core.io.BytesOutput;
import com.baidu.hugegraph.computer.core.io.IOFactory;
import com.baidu.hugegraph.computer.core.sort.SorterTestUtil;
import com.baidu.hugegraph.computer.core.store.hgkvfile.buffer.KvEntriesInput;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.Pointer;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.HgkvDirImpl;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.builder.HgkvDirBuilder;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.builder.HgkvDirBuilderImpl;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.builder.HgkvFileBuilder;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.builder.HgkvFileBuilderImpl;
import com.baidu.hugegraph.testutil.Assert;

public class StoreTestUtil {

    public static final String FILE_DIR = "hgkv";

    private static void writeData(BytesOutput output, Integer value)
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

    public static List<KvEntry> kvEntriesFromMap(List<Integer> map)
                                                 throws IOException {
        BytesOutput data = IOFactory.createBytesOutput(
                           Constants.DEFAULT_SIZE);
        Iterator<Integer> iterator = map.iterator();
        while (iterator.hasNext()) {
            // Write key length
            writeData(data, iterator.next());
            // Write value length
            writeData(data, iterator.next());
        }

        BytesInput input = IOFactory.createBytesInput(data.buffer(),
                                                      (int) data.position());
        Iterator<KvEntry> entriesIter = new KvEntriesInput(input);
        List<KvEntry> entries = new ArrayList<>();
        while (entriesIter.hasNext()) {
            entries.add(entriesIter.next());
        }

        return entries;
    }

    public static void hgkvDirFromKvMap(Config config, List<Integer> map,
                                        String path) throws IOException {
        File file = new File(path);
        try (HgkvDirBuilder builder = new HgkvDirBuilderImpl(config, path)) {
            List<KvEntry> entries = StoreTestUtil.kvEntriesFromMap(map);
            for (KvEntry entry : entries) {
                builder.write(entry);
            }
            builder.finish();
        } catch (Exception e) {
            FileUtils.deleteQuietly(file);
            throw e;
        }
    }

    public static void hgkvDirFromSubKvMap(Config config,
                                           List<List<Integer>> map,
                                           String path) throws IOException {
        BytesInput input = SorterTestUtil.inputFromSubKvMap(map);
        Iterator<KvEntry> iter = new KvEntriesInput(input, true);
        try (HgkvDirBuilder builder = new HgkvDirBuilderImpl(config, path)) {
            while (iter.hasNext()) {
                builder.write(iter.next());
            }
        }
    }

    public static File mapToHgkvFile(Config config, List<Integer> map,
                                     String path) throws IOException {
        File file = new File(path);

        try (HgkvFileBuilder builder = new HgkvFileBuilderImpl(config, path)) {
            List<KvEntry> entries = StoreTestUtil.kvEntriesFromMap(map);
            for (KvEntry entry : entries) {
                builder.add(entry);
            }
            builder.finish();
            /*
             * Some fields are written in a variable-length way,
             * so it's not recommended to assert length value.
             */
            Assert.assertEquals(19, builder.headerLength());
        } catch (Exception e) {
            FileUtils.deleteQuietly(file);
            throw e;
        }
        return file;
    }

    public static Integer dataFromPointer(Pointer pointer) throws IOException {
        return byteArrayToInt(pointer.bytes());
    }

    public static String availablePathById(String id) {
        String fileName = StringUtils.join(HgkvDirImpl.FILE_NAME_PREFIX, id,
                                           HgkvDirImpl.FILE_EXTEND_NAME);
        return Paths.get(FILE_DIR, fileName).toString();
    }

    public static String availablePathById(int id) {
        return availablePathById(String.valueOf(id));
    }

    public static int byteArrayToInt(byte[] bytes) throws IOException {
        return IOFactory.createBytesInput(bytes).readInt();
    }

    public static byte[] intToByteArray(int data) throws IOException {
        BytesOutput output = IOFactory.createBytesOutput(
                             Constants.DEFAULT_SIZE);
        output.writeInt(data);
        return output.toByteArray();
    }
}
