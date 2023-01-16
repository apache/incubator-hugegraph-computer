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

package org.apache.hugegraph.computer.core.store.file.hgkvfile;

import java.io.File;
import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.io.IOFactory;
import org.apache.hugegraph.computer.core.io.RandomAccessInput;
import org.apache.hugegraph.computer.core.io.RandomAccessOutput;
import org.apache.hugegraph.util.E;

public class HgkvFileImpl extends AbstractHgkvFile {

    public HgkvFileImpl(String path) {
        super(path);
    }

    public static HgkvFile create(String path) throws IOException {
        File file = new File(path);
        E.checkArgument(!file.exists(),
                        "Can't create HgkvFile because the " +
                        "file already exists: '%s'", file.getPath());
        if (!file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }
        file.createNewFile();

        return new HgkvFileImpl(path);
    }

    public static HgkvFile open(String path) throws IOException {
        E.checkArgumentNotNull(path, "Parameter path can't be null");
        return open(new File(path));
    }

    public static HgkvFile open(File file) throws IOException {
        E.checkArgumentNotNull(file, "Parameter file can't be null");
        E.checkArgument(file.exists(),
                        "Failed to open path because the " +
                        "file does not exists: '%s'", file.getPath());
        E.checkArgument(file.isFile(),
                        "Failed to open path because it's not a file: '%s'",
                        file.getPath());

        HgkvFileImpl hgkvFile = new HgkvFileImpl(file.getPath());
        hgkvFile.readFooter();
        return hgkvFile;
    }

    private static String version(short majorVersion, short minorVersion) {
        return StringUtils.join(majorVersion, ".", minorVersion);
    }

    @Override
    public RandomAccessOutput output() throws IOException {
        return IOFactory.createFileOutput(new File(this.path));
    }

    @Override
    public void close() throws IOException {
        // pass
    }

    private void readFooter() throws IOException {
        File file = new File(this.path);
        // The footerLength occupied 4 bytes, versionLength 2 * 2 bytes
        long versionOffset = file.length() - Short.BYTES * 2 - Integer.BYTES;

        try (RandomAccessInput input = IOFactory.createFileInput(file)) {
            input.seek(versionOffset);
            // Read version
            short majorVersion = input.readShort();
            short minorVersion = input.readShort();
            String version = version(majorVersion, minorVersion);
            // Read footerLength
            int footerLength = input.readFixedInt();
            switch (version) {
                case "1.0":
                    this.readFooterV1d0(input, file.length() - footerLength);
                    break;
                default:
                    throw new ComputerException("Illegal HgkvFile version '%s'",
                                                version);
            }
        }
    }

    private void readFooterV1d0(RandomAccessInput input, long footerBegin)
                                throws IOException {
        input.seek(footerBegin);

        // Read magic
        String magic = new String(input.readBytes(MAGIC.length()));
        E.checkArgument(MAGIC.equals(magic),
                        "Failed to read footer, illegal hgvk-file magic in " +
                        "file: '%s'", this.path);
        this.magic = magic;
        // Read numEntries
        this.numEntries = input.readLong();
        // Read numSubEntries
        this.numSubEntries = input.readLong();
        // Read dataBlock length
        this.dataBlockSize = input.readLong();
        // Read indexBlock length
        this.indexBlockSize = input.readLong();
        // Read max key and min key
        long maxKeyOffset = input.readLong();
        long minKeyOffset = input.readLong();
        // Read version
        short majorVersion = input.readShort();
        short minorVersion = input.readShort();
        this.version = version(majorVersion, minorVersion);

        if (this.numEntries > 0) {
            // Read max key
            input.seek(maxKeyOffset);
            int maxKeyLength = input.readFixedInt();
            this.max = input.readBytes(maxKeyLength);
            // Read min Key
            input.seek(minKeyOffset);
            int minKeyLength = input.readFixedInt();
            this.min = input.readBytes(minKeyLength);
        }
    }
}
