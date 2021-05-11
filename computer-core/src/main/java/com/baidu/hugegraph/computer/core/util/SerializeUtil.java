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

import java.io.IOException;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.common.exception.ComputeException;
import com.baidu.hugegraph.computer.core.io.Readable;
import com.baidu.hugegraph.computer.core.io.UnsafeBytesInput;
import com.baidu.hugegraph.computer.core.io.UnsafeBytesOutput;
import com.baidu.hugegraph.computer.core.io.Writable;

public final class SerializeUtil {

    // TODO: try to reduce call ComputerContext.instance() directly.
    private static final ComputerContext CONTEXT = ComputerContext.instance();

    public static byte[] toBytes(Writable obj) {
        try (UnsafeBytesOutput bao = new UnsafeBytesOutput()) {
            obj.write(bao);
            return bao.toByteArray();
        } catch (IOException e) {
            throw new ComputeException(
                      "Failed to create byte array with writable '%s'", e, obj);
        }
    }

    public static void fromBytes(byte[] bytes, Readable obj) {
        try (UnsafeBytesInput bai = new UnsafeBytesInput(bytes)) {
            obj.read(bai);
        } catch (IOException e) {
            throw new ComputeException("Failed to read from byte array", e);
        }
    }
}
