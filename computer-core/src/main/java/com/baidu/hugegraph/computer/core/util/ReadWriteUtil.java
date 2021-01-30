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

package com.baidu.hugegraph.computer.core.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.commons.io.output.ByteArrayOutputStream;

import com.baidu.hugegraph.computer.core.common.exception.ComputeException;
import com.baidu.hugegraph.computer.core.io.OptimizedStreamGraphInput;
import com.baidu.hugegraph.computer.core.io.OptimizedStreamGraphOutput;
import com.baidu.hugegraph.computer.core.io.Readable;
import com.baidu.hugegraph.computer.core.io.StreamGraphInput;
import com.baidu.hugegraph.computer.core.io.StreamGraphOutput;
import com.baidu.hugegraph.computer.core.io.Writable;

public class ReadWriteUtil {

    public static byte[] toBytes(Writable obj) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            StreamGraphOutput output = new OptimizedStreamGraphOutput(baos);
            obj.write(output);
            return baos.toByteArray();
        } catch (IOException e) {
            throw new ComputeException(
                      "Failed to create byte array with writable '%s'", e, obj);
        }
    }

    public static void fromBytes(byte[] bytes, Readable obj) {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes)) {
            StreamGraphInput input = new OptimizedStreamGraphInput(bais);
            obj.read(input);
        } catch (IOException e) {
            throw new ComputeException("Failed to read from byte array", e);
        }
    }
}
