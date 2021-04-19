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

package com.baidu.hugegraph.computer.core.store.file;

import com.baidu.hugegraph.computer.core.store.entry.Pointer;

public abstract class AbstractHgkvFile implements HgkvFile {

    public static final byte PRIMARY_VERSION;
    public static final byte MINOR_VERSION;
    public static final String MAGIC;

    static {
        PRIMARY_VERSION = 1;
        MINOR_VERSION = 0;
        MAGIC = "hgkv";
    }

    protected final String path;
    protected String magic;
    protected long entriesSize;
    protected long dataBlockSize;
    protected long indexBlockSize;
    protected Pointer max;
    protected Pointer min;
    protected String version;

    public AbstractHgkvFile(String path) {
        this.path = path;
    }

    @Override
    public String path() {
        return this.path;
    }

    @Override
    public long entriesSize() {
        return this.entriesSize;
    }

    @Override
    public String version() {
        return this.version;
    }

    @Override
    public Pointer max() {
        return this.max;
    }

    @Override
    public Pointer min() {
        return this.min;
    }

    @Override
    public String magic() {
        return this.magic;
    }
}
