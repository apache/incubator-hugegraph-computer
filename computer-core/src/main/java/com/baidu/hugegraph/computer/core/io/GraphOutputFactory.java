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

package com.baidu.hugegraph.computer.core.io;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.EntryOutput;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.EntryOutputImpl;

public class GraphOutputFactory {

    public static GraphOutput create(ComputerContext context,
                                     OutputFormat format,
                                     RandomAccessOutput out) {
        switch (format) {
            case BIN:
                EntryOutput entryOutput = new EntryOutputImpl(out);
                return new StreamGraphOutput(context, entryOutput);
            case CSV:
                StructRandomAccessOutput srao;
                srao = new StructRandomAccessOutput(out);
                return new CsvStructGraphOutput(context, srao);
            case JSON:
                srao = new StructRandomAccessOutput(out);
                return new JsonStructGraphOutput(context, srao);
            default:
                throw new ComputerException("Can't create GraphOutput for %s",
                                            format);
        }
    }
}
