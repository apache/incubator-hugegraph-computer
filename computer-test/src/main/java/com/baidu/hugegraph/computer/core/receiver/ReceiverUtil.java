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

package com.baidu.hugegraph.computer.core.receiver;

import java.io.IOException;
import java.util.function.Consumer;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.io.GraphInput;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.Readable;
import com.baidu.hugegraph.computer.core.io.StreamGraphInput;
import com.baidu.hugegraph.computer.core.network.buffer.ManagedBuffer;
import com.baidu.hugegraph.computer.core.network.buffer.NettyManagedBuffer;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.Pointer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class ReceiverUtil {

    public static void comsumeBuffer(byte[] bytes,
                                     Consumer<ManagedBuffer> consumer) {
        ByteBuf buf = Unpooled.directBuffer(bytes.length);
        try {
            buf = buf.writeBytes(bytes);
            NettyManagedBuffer buff = new NettyManagedBuffer(buf);
            consumer.accept(buff);
        } finally {
            buf.release();
        }
    }

    public static Id readId(ComputerContext context, Pointer pointer)
                            throws IOException {
        RandomAccessInput input = pointer.input();
        long position = input.position();
        input.seek(pointer.offset());
        GraphInput graphInput = new StreamGraphInput(context, pointer.input());
        Id id =  graphInput.readId();
        input.seek(position);
        return id;
    }

    public static void readValue(Pointer pointer, Readable value)
                                 throws IOException {
        RandomAccessInput input = pointer.input();
        long position = input.position();
        input.seek(pointer.offset());
        value.read(input);
        input.seek(position);
    }
}
