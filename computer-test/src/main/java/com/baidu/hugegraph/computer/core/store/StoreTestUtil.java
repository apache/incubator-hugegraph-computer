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

import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.UnsafeBytesInput;
import com.baidu.hugegraph.computer.core.io.UnsafeBytesOutput;
import com.baidu.hugegraph.computer.core.sort.SorterTestUtil;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.Pointer;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.HgkvDirImpl;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.builder.HgkvDirBuilder;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.builder.HgkvDirBuilderImpl;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.builder.HgkvFileBuilder;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.builder.HgkvFileBuilderImpl;
import com.baidu.hugegraph.computer.core.store.hgkvfile.buffer.EntriesInput;
import com.baidu.hugegraph.testutil.Assert;

public class StoreTestUtil {

    public static final String FILE_DIR = "hgkv";

    public static List<KvEntry> kvEntriesFromMap(List<Integer> map)
                                                 throws IOException {
        UnsafeBytesOutput data = new UnsafeBytesOutput();
        Iterator<Integer> iterator = map.iterator();
        while (iterator.hasNext()) {
            data.writeInt(Integer.BYTES);
            data.writeInt(iterator.next());
            data.writeInt(Integer.BYTES);
            data.writeInt(iterator.next());
        }

        RandomAccessInput input = new UnsafeBytesInput(data.buffer(),
                                                       data.position());
        Iterator<KvEntry> entriesIter = new EntriesInput(input);
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
        UnsafeBytesInput input = SorterTestUtil.inputFromSubKvMap(map);
        Iterator<KvEntry> iter = new EntriesInput(input);
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
            Assert.assertEquals(56, builder.headerLength());
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

    public static int byteArrayToInt(byte[] bytes) {
        return bytes[0] & 0xFF |
               (bytes[1] & 0xFF) << 8 |
               (bytes[2] & 0xFF) << 16 |
               (bytes[3] & 0xFF) << 24;
    }

    public static byte[] intToByteArray(int data) {
        return new byte[] {
                (byte) (data & 0xFF),
                (byte) ((data >> 8) & 0xFF),
                (byte) ((data >> 16) & 0xFF),
                (byte) ((data >> 24) & 0xFF)
        };
    }
}
