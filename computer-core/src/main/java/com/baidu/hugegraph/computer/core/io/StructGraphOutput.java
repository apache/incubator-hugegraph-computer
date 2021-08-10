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

import java.io.IOException;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.value.ListValue;
import com.baidu.hugegraph.computer.core.graph.value.Value;

public abstract class StructGraphOutput implements GraphWritebackOutput {

    protected final Config config;
    protected final StructRandomAccessOutput out;

    public StructGraphOutput(ComputerContext context,
                             StructRandomAccessOutput out) {
        this.config = context.config();
        this.out = out;
    }

    public abstract void writeObjectStart() throws IOException;

    public abstract void writeObjectEnd() throws IOException;

    public abstract void writeArrayStart() throws IOException;

    public abstract void writeArrayEnd() throws IOException;

    public abstract void writeKey(String key) throws IOException;

    public abstract void writeJoiner() throws IOException;

    public abstract void writeSplitter() throws IOException;

    public void writeLineStart() throws IOException {
        // pass
    }

    public void writeLineEnd() throws IOException {
        this.out.writeRawString(System.lineSeparator());
    }

    public void writeId(Id id) throws IOException {
        Object rawId = id.asObject();
        if (rawId instanceof Number) {
            this.out.writeNumber((Number) rawId);
        } else {
            this.out.writeString(rawId.toString());
        }
    }

    public void writeValue(Value<?> value) throws IOException {
        switch (value.type()) {
            case ID_VALUE:
                this.writeIdValue((Id) value);
                break;
            case ID_VALUE_LIST:
            case ID_VALUE_LIST_LIST:
            case LIST_VALUE:
                this.writeListValue((ListValue<?>) value);
                break;
            case NULL:
            case BOOLEAN:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
                value.write(this.out);
                break;
            default:
                throw new ComputerException("Unexpected value type %s",
                                            value.type());
        }
    }

    private void writeIdValue(Id idValue) throws IOException {
        /*
         * The idValue is shown as bytes in computation,
         * but it's as id when output
         */
        this.writeId(idValue);
    }

    private void writeListValue(ListValue<?> listValue) throws IOException {
        this.writeArrayStart();
        int size = listValue.size();
        int i = 0;
        for (Value<?> value : listValue.values()) {
            this.writeValue(value);
            if (++i < size) {
                this.writeSplitter();
            }
        }
        this.writeArrayEnd();
    }
}
