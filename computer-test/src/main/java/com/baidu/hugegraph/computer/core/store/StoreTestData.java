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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;

import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.UnsafeByteArrayInput;
import com.baidu.hugegraph.computer.core.io.UnsafeByteArrayOutput;
import com.baidu.hugegraph.computer.core.sort.util.EntriesUtil;
import com.baidu.hugegraph.computer.core.store.base.DefaultKvEntry;
import com.baidu.hugegraph.computer.core.store.base.KvEntry;
import com.baidu.hugegraph.computer.core.store.base.Pointer;
import com.baidu.hugegraph.computer.core.store.file.HgkvFileImpl;
import com.baidu.hugegraph.computer.core.store.file.builder.HgkvDirBuilder;
import com.baidu.hugegraph.computer.core.store.file.builder.HgkvDirBuilderImpl;
import com.baidu.hugegraph.computer.core.store.file.builder.HgkvFileBuilder;
import com.baidu.hugegraph.computer.core.store.file.builder.HgkvFileBuilderImpl;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class StoreTestData {

    public static List<Pointer> keysFromMap(List<Integer> map)
                                            throws IOException {
        UnsafeByteArrayOutput data = new UnsafeByteArrayOutput();
        Iterator<Integer> iterator = map.iterator();
        while (iterator.hasNext()) {
            data.writeInt(Integer.BYTES);
            data.writeInt(iterator.next());
            data.writeInt(Integer.BYTES);
            data.writeInt(iterator.next());
        }

        RandomAccessInput input = new UnsafeByteArrayInput(data.buffer(),
                                                           data.position());
        Iterator<Pointer> inputItr = new EntriesKeyInput(input);
        List<Pointer> keys = new ArrayList<>();
        while (inputItr.hasNext()) {
            keys.add(inputItr.next());
        }

        return keys;
    }

    public static List<KvEntry> kvEntriesFromMap(List<Integer> map)
                                                 throws IOException {
        List<Pointer> keys = keysFromMap(map);
        if (CollectionUtils.isEmpty(keys)) {
            return Lists.newArrayList();
        }

        // Group by key
        Iterator<Pointer> keysItr = keys.iterator();
        Pointer last = keysItr.next();
        List<Pointer> sameKeys = new ArrayList<>();
        sameKeys.add(last);

        List<KvEntry> entries = new ArrayList<>();
        while (true) {
            Pointer current = null;
            if (keysItr.hasNext()) {
                current = keysItr.next();
                if (last.compareTo(current) == 0) {
                    sameKeys.add(current);
                    continue;
                }
            }

            List<Pointer> values = new ArrayList<>();
            for (Pointer key : sameKeys) {
                values.add(EntriesUtil.valuePointerByKeyPointer(key));
            }
            entries.add(new DefaultKvEntry(last, ImmutableList.copyOf(values)));

            if (current == null) {
                break;
            }
            sameKeys.clear();
            sameKeys.add(current);
            last = current;
        }

        return entries;
    }

    public static Map<Pointer, Pointer> kvPointerMapFromMap(List<Integer> map)
                                                            throws IOException {
        List<Pointer> keys = keysFromMap(map);
        Map<Pointer, Pointer> result = new LinkedHashMap<>();
        for (Pointer key : keys) {
            result.put(key, EntriesUtil.valuePointerByKeyPointer(key));
        }
        return result;
    }

    public static File hgkvDirFromMap(List<Integer> map, String path)
                                      throws IOException {
        File file = new File(path);
        try (HgkvDirBuilder builder = new HgkvDirBuilderImpl(path)) {
            List<KvEntry> entries = StoreTestData.kvEntriesFromMap(map);
            for (KvEntry entry : entries) {
                builder.write(entry);
            }
            builder.finish();
        } catch (Exception e) {
            FileUtils.deleteQuietly(file);
            throw e;
        }
        return file;
    }

    public static File hgkvFileFromMap(List<Integer> map, String path)
                                       throws IOException {
        File file = new File(path);

        try (HgkvFileBuilder builder = new HgkvFileBuilderImpl(path)) {
            Map<Pointer, Pointer> kvMap = StoreTestData.kvPointerMapFromMap(map);
            for (Map.Entry<Pointer, Pointer> entry : kvMap.entrySet()) {
                builder.add(entry.getKey(), entry.getValue());
            }
            builder.finish();
        } catch (Exception e) {
            FileUtils.deleteQuietly(file);
            throw e;
        }
        return file;
    }
}
