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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;

public class GraphOutputFactory {

    public static GraphOutput create(OutputFormat format,
                                     ByteArrayOutputStream out) {
        return create(format, new DataOutputStream(out));
    }

    public static GraphOutput create(OutputFormat format,
                                     DataOutputStream out) {
        switch (format) {
            case BIN:
                return new OptimizedStreamGraphOutput(out);
            case CSV:
                return new CsvStructGraphOutput(out);
            case JSON:
                return new JsonStructGraphOutput(out);
            default:
                throw new ComputerException("Can't create GraphOutput for %s",
                                            format);
        }
    }
}
