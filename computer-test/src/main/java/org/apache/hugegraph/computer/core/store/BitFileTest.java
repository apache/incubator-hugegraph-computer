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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.computer.core.common.ComputerContext;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.store.file.seqfile.BitsFileReader;
import org.apache.hugegraph.computer.core.store.file.seqfile.BitsFileReaderImpl;
import org.apache.hugegraph.computer.core.store.file.seqfile.BitsFileWriter;
import org.apache.hugegraph.computer.core.store.file.seqfile.BitsFileWriterImpl;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class BitFileTest {

    private static final Config CONFIG = ComputerContext.instance().config();

    @Test
    public void test() throws IOException {
        File dir = createTempDir();

        try {
            try (BitsFileWriter writer = new BitsFileWriterImpl(CONFIG, dir)) {
                for (int i = 0; i < 1000000; i++) {
                    writer.writeBoolean(i % 2 == 0);
                }
            }

            try (BitsFileReader reader = new BitsFileReaderImpl(CONFIG, dir)) {
                for (int i = 0; i < 1000000; i++) {
                    Assert.assertEquals(i % 2 == 0, reader.readBoolean());
                }
            }
        } finally {
            FileUtils.deleteQuietly(dir);
        }
    }

    @Test
    public void testRandomValue() throws IOException {
        File dir = createTempDir();

        List<Boolean> data = new ArrayList<>();
        Random random = new Random();
        try {
            try (BitsFileWriter writer = new BitsFileWriterImpl(CONFIG, dir)) {
                for (int i = 0; i < 1000000; i++) {
                    boolean item = random.nextInt(2) == 0;
                    writer.writeBoolean(item);
                    data.add(item);
                }
            }

            try (BitsFileReader reader = new BitsFileReaderImpl(CONFIG, dir)) {
                for (int i = 0; i < data.size(); i++) {
                    Assert.assertEquals(data.get(i), reader.readBoolean());
                }
            }
        } finally {
            FileUtils.deleteQuietly(dir);
        }
    }

    private static File createTempDir() {
        File dir = new File(UUID.randomUUID().toString());
        dir.mkdirs();
        return dir;
    }
}
