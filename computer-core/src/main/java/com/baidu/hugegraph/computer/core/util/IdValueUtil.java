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
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.value.IdValue;
import com.baidu.hugegraph.computer.core.io.OptimizedStreamGraphInput;
import com.baidu.hugegraph.computer.core.io.OptimizedStreamGraphOutput;
import com.baidu.hugegraph.computer.core.io.StreamGraphInput;
import com.baidu.hugegraph.computer.core.io.StreamGraphOutput;
import com.baidu.hugegraph.computer.core.io.UnsafeByteArrayInput;
import com.baidu.hugegraph.computer.core.io.UnsafeByteArrayOutput;

public class IdValueUtil {

    // TODO: try to reduce call ComputerContext.instance() directly.
    private static final ComputerContext CONTEXT = ComputerContext.instance();

    public static Id toId(IdValue idValue) {
        byte[] bytes = idValue.bytes();
        try (UnsafeByteArrayInput bai = new UnsafeByteArrayInput(bytes);
             StreamGraphInput input = new OptimizedStreamGraphInput(CONTEXT,
                                                                    bai)) {
            return input.readId();
        } catch (IOException e) {
            throw new ComputerException("Failed to get id from idValue '%s'",
                                        e, idValue);
        }
    }

    public static IdValue toIdValue(Id id, int len) {
        try (UnsafeByteArrayOutput bao = new UnsafeByteArrayOutput(len);
             StreamGraphOutput output = new OptimizedStreamGraphOutput(
                                            CONTEXT, bao)) {
            output.writeId(id);
            return new IdValue(bao.toByteArray());
        } catch (IOException e) {
            throw new ComputerException("Failed to get idValue from id '%s'",
                                        e, id);
        }
    }
}
