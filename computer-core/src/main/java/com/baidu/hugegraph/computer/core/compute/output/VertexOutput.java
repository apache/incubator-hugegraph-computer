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

package com.baidu.hugegraph.computer.core.compute.output;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.graph.properties.Properties;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.io.BufferedFileOutput;
import com.baidu.hugegraph.computer.core.io.RandomAccessOutput;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import com.baidu.hugegraph.computer.core.util.CoderUtil;
import java.nio.ByteBuffer;

public class VertexOutput {

    private RandomAccessOutput output;
    private final Vertex vertex;
    private final Properties properties;
    private final File vertexFile;
    private boolean useFixLength;
    private int idBytes;

    public VertexOutput(ComputerContext context,
                       File vertexFile) {
        this.vertexFile = vertexFile;
        this.vertex = context.graphFactory().createVertex();
        this.properties = context.graphFactory().createProperties();
        this.useFixLength = false;
        this.idBytes = context.config().
                            get(ComputerOptions.ID_FIXLENGTH_BYTES);
    }

    public void init() throws IOException {
        this.output = new BufferedFileOutput(this.vertexFile);
    }

    public void close() throws IOException {
        this.output.close();
    }

    public void switchToFixLength() {
       this.useFixLength = true;
    }

    public void writeIdBytes() {
       try {
           this.output.writeFixedInt(this.idBytes);
       }  catch (IOException e) {
            throw new ComputerException("Can't read vertex from input '%s'",
                                        e, this.vertexFile.getAbsolutePath());
       }
    }

    public void writeVertex(Vertex vertex) {
        try {
             if (! this.useFixLength) {
                //write Id
                long keyPosition = this.output.position();
                this.output.writeFixedInt(0);
                vertex.id().write(this.output);
                long keyLength = this.output.position() - keyPosition -
                                     Constants.INT_LEN;
                this.output.writeFixedInt(keyPosition, (int) keyLength);
             }
             else {
                long lid = (long)(vertex.id().asObject());
                ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
                buffer.putLong(0, lid);
                byte[] bid = buffer.array();
                for (int j = 0; j < this.idBytes; j++) {
                    int j_ = j + Long.BYTES - this.idBytes;
                    this.output.writeByte(bid[j_]);
                }
             }

             //write values
             long valuePosition = this.output.position();
             this.output.writeFixedInt(0);
               
             //write label
             byte[] blabel = CoderUtil.encode(vertex.label());
             this.output.writeByte((int)blabel.length);
             for (int i = 0; i < blabel.length; i++) {
                 this.output.writeByte((int)blabel[i]);
             }

             //write properties
             Map<String, Value<?>> keyValues = properties.get();
             this.writeVInt(this.output, -1);
             for (Map.Entry<String, Value<?>> entry : keyValues.entrySet()) {
                 String key = entry.getKey();
                 byte[] bkey = CoderUtil.encode(key);
                 this.output.writeByte((int)bkey.length);
                 for (int i = 0; i < bkey.length; i++) {
                     this.output.writeByte((int)bkey[i]);
                 }

                 Value<?> value = entry.getValue();
                 this.output.writeByte(value.valueType().code());
                 value.write(this.output);
              }

              long valueLength = this.output.position() - valuePosition -
                                     Constants.INT_LEN;
              this.output.writeFixedInt(valuePosition, (int) valueLength);
        } catch (IOException e) {
            throw new ComputerException("Can't read vertex from input '%s'",
                                        e, this.vertexFile.getAbsolutePath());
        }
    }

    private void writeVInt(RandomAccessOutput output, int value) 
                            throws IOException { 
        if (value > 0x0fffffff || value < 0) {
           output.writeByte(0x80 | ((value >>> 28) & 0x7f));
        }
        if (value > 0x1fffff || value < 0) {
           output.writeByte(0x80 | ((value >>> 21) & 0x7f));
        }
        if (value > 0x3fff || value < 0) {
            output.writeByte(0x80 | ((value >>> 14) & 0x7f));
        }
        if (value > 0x7f || value < 0) {
            output.writeByte(0x80 | ((value >>> 7) & 0x7f));
        }
        output.writeByte(value & 0x7f); 
     }
}
