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
import com.baidu.hugegraph.computer.core.graph.id.IdFactory;
import com.baidu.hugegraph.computer.core.graph.value.IdValue;
import com.baidu.hugegraph.computer.core.io.BytesInput;
import com.baidu.hugegraph.computer.core.io.BytesOutput;
import com.baidu.hugegraph.computer.core.io.IOFactory;

public class IdValueUtil {

    // TODO: try to reduce call ComputerContext.instance() directly.
    private static final ComputerContext CONTEXT = ComputerContext.instance();

    public static Id toId(IdValue idValue) {
        byte[] bytes = idValue.bytes();
        /*
         * NOTE: must use OptimizedUnsafeBytesInput, it make sure to
         * write bytes in big-end-aligned way
         */
        try (BytesInput bai = IOFactory.createBytesInput(bytes)) {
            byte type = bai.readByte();
            Id id = IdFactory.createId(type);
            id.read(bai);
            return id;
        } catch (IOException e) {
            throw new ComputerException("Failed to get id from idValue '%s'",
                                        e, idValue);
        }
    }

    public static IdValue toIdValue(Id id, int len) {
        try (BytesOutput bao = IOFactory.createBytesOutput(len)) {
            bao.writeByte(id.type().code());
            id.write(bao);
            return new IdValue(bao.buffer(), (int) bao.position());
        } catch (IOException e) {
            throw new ComputerException("Failed to get idValue from id '%s'",
                                        e, id);
        }
    }
}
