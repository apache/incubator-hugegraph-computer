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

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.graph.id.BytesId;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.io.BytesInput;
import org.apache.hugegraph.computer.core.io.BytesOutput;
import org.apache.hugegraph.computer.core.io.IOFactory;
import org.apache.hugegraph.computer.core.io.RandomAccessOutput;
import org.apache.hugegraph.computer.core.sort.SorterTestUtil;
import org.apache.hugegraph.computer.core.store.buffer.KvEntriesInput;
import org.apache.hugegraph.computer.core.store.entry.KvEntry;
import org.apache.hugegraph.computer.core.store.entry.Pointer;
import org.apache.hugegraph.computer.core.store.file.hgkvfile.HgkvDirImpl;
import org.apache.hugegraph.computer.core.store.file.hgkvfile.builder.HgkvDirBuilderImpl;
import org.apache.hugegraph.computer.core.store.file.hgkvfile.builder.HgkvFileBuilder;
import org.apache.hugegraph.computer.core.store.file.hgkvfile.builder.HgkvFileBuilderImpl;
import org.apache.hugegraph.testutil.Assert;

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
                           Constants.SMALL_BUF_SIZE);
        Iterator<Integer> iterator = map.iterator();
        while (iterator.hasNext()) {
            // Write key length
            writeData(data, iterator.next());
            // Write value length
            writeData(data, iterator.next());
        }

        BytesInput input = IOFactory.createBytesInput(data.buffer(),
                                                      (int) data.position());
        KvEntriesInput iter = new KvEntriesInput(input);
        List<KvEntry> entries = new ArrayList<>();
        while (iter.hasNext()) {
            entries.add(iter.next());
        }
        iter.close();

        return entries;
    }

    public static void hgkvDirFromKvMap(Config config, List<Integer> map,
                                        String path) throws IOException {
        File file = new File(path);
        try (KvEntryFileWriter builder = new HgkvDirBuilderImpl(config, path)) {
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
        KvEntriesInput iter = new KvEntriesInput(input, true);
        try (KvEntryFileWriter builder = new HgkvDirBuilderImpl(config, path)) {
            while (iter.hasNext()) {
                builder.write(iter.next());
            }
        }
        iter.close();
    }

    public static void bufferFileFromKvMap(List<Integer> map, String path)
                                           throws IOException {
        RandomAccessOutput output = IOFactory.createFileOutput(new File(path));
        BytesInput buffer = SorterTestUtil.inputFromKvMap(map);
        output.write(buffer.readBytes((int) buffer.available()));
        output.close();
    }

    public static void bufferFileFromSubKvMap(List<List<Integer>> map,
                                              String path) throws IOException {
        RandomAccessOutput output = IOFactory.createFileOutput(new File(path));
        BytesInput buffer = SorterTestUtil.inputFromSubKvMap(map);
        output.write(buffer.readBytes((int) buffer.available()));
        output.close();
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

    public static Id idFromPointer(Pointer pointer) throws IOException {
        BytesInput input = IOFactory.createBytesInput(pointer.bytes());
        Id id = new BytesId();
        id.read(input);
        return id;
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
                             Constants.SMALL_BUF_SIZE);
        output.writeInt(data);
        return output.toByteArray();
    }
}
