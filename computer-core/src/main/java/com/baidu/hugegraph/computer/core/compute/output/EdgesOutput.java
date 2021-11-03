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
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.EdgeFrequency;
import com.baidu.hugegraph.computer.core.graph.GraphFactory;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.edge.Edges;
import com.baidu.hugegraph.computer.core.graph.value.BooleanValue;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.io.BufferedFileOutput;
import com.baidu.hugegraph.computer.core.io.RandomAccessOutput;
import java.io.File;
import java.io.IOException;
import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.computer.core.util.CoderUtil;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import java.util.Map;
import java.nio.ByteBuffer;

public class EdgesOutput {

    private RandomAccessOutput output;
    //private final ReusablePointer idPointer;
    //private final ReusablePointer valuePointer;
    private final File edgeFile;
    private final GraphFactory graphFactory;
    private final int flushThreshold;
    private final EdgeFrequency frequency;
    private boolean useFixLength;
    private boolean useInvEdge;
    private int idBytes ;

    public EdgesOutput(ComputerContext context, File edgeFile) {
        this.graphFactory = context.graphFactory();
        this.edgeFile = edgeFile;
        this.flushThreshold = context.config().get(
                              ComputerOptions.INPUT_MAX_EDGES_IN_ONE_VERTEX);
        this.frequency = context.config().get(ComputerOptions.INPUT_EDGE_FREQ);
        this.useInvEdge = context.config().get(ComputerOptions.
                                              VERTEX_WITH_EDGES_BOTHDIRECTION);
        this.useFixLength = false;
        this.idBytes = context.config().get(ComputerOptions.ID_FIXLENGTH_BYTES);
    }

    public void init() throws IOException {
        this.output = new BufferedFileOutput(this.edgeFile);
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
            throw new ComputerException("Failed to read edges from input '%s'",
                                        e, this.edgeFile.getAbsoluteFile());
        }
    }

    // TODO: use one reused Edges instance to read batches for each vertex.
    public void writeEdges(Vertex vertex, Edges edges) {
        int count = edges.size();
        int goodcount = 0;
        try {
            //write vertex id
            if (!this.useFixLength) {
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
            //write place holder for how large the edges buffer
            long valuePosition = this.output.position();
            this.output.writeFixedInt(0);
            //write how many edges are
            long countPosition = this.output.position();
            this.output.writeFixedInt(count);

            if (this.frequency == EdgeFrequency.SINGLE) {
                for (Edge edge : edges) {
                    //Edge edge = this.graphFactory.createEdge();
                    //Only use targetId as subKey, use props as subValue
                    BooleanValue inv = edge.properties().get("inv");
                    boolean inv_ = (inv == null) ? false : inv.value();
                    if (inv_ && !useInvEdge) {
                        continue;
                    }
                    goodcount++;
                    byte binv = (byte)(inv_ ? 0x01 : 0x00);
                    this.output.writeByte(binv);

                    //write target id
                    if (!this.useFixLength) {                  
                        edge.targetId().write(this.output);
                    }
                    else {
                        long lid = (long)(edge.targetId().asObject());
                        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
                        buffer.putLong(0, lid);
                        byte[] bid = buffer.array();
                        for (int j = 0; j < this.idBytes; j++) {
                            int j_ = j + Long.BYTES - this.idBytes;
                            this.output.writeByte(bid[j_]);
                        }
                    }
                    //write edge id
                    edge.id().write(this.output);

                    //write label
                    byte[] blabel = CoderUtil.encode(edge.label());
                    this.output.writeByte(blabel.length);
                    for (int i = 0; i < blabel.length; i++) {
                        this.output.writeByte((int)blabel[i]);
                    }

                    //write properties
                    Map<String, Value<?>> keyValues = edge.properties().get();
                    this.output.writeByte(keyValues.size());
                    for (Map.Entry<String, Value<?>> 
                           entry : keyValues.entrySet()) {
        
                        String key = entry.getKey();
                        byte[] bkey = CoderUtil.encode(key);
                        this.output.writeByte(bkey.length);
                        for (int i = 0; i < bkey.length; i++) {
                            this.output.writeByte((int)bkey[i]);
                        }
        
                        Value<?> value = entry.getValue();
                        this.output.writeByte(value.valueType().code());
                        value.write(this.output);
                    }
                }
            }
            else if (this.frequency == EdgeFrequency.SINGLE_PER_LABEL) {
                for (Edge edge : edges) {
                    //
                    BooleanValue inv = edge.properties().get("inv");
                    boolean inv_ = (inv == null) ? false : inv.value();
                    if (inv_ && !useInvEdge) {
                        continue;
                    }
                    goodcount++;
                    byte binv = (byte)(inv_ ? 0x01 : 0x00);
                    this.output.writeByte(binv);
         
                    //write label 
                    byte[] blabel = CoderUtil.encode(edge.label());
                    this.output.writeByte(blabel.length);
                    for (int i = 0; i < blabel.length; i++) {
                        this.output.writeByte((int)blabel[i]);
                    }

                    //write targetid
                    if (!this.useFixLength) {
                        edge.targetId().write(this.output);
                    }
                    else {
                        long lid = (long)(edge.targetId().asObject());
                        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
                        buffer.putLong(0, lid);
                        byte[] bid = buffer.array();
                        for (int j = 0; j < this.idBytes; j++) {
                            int j_ = j + Long.BYTES - this.idBytes;
                            this.output.writeByte(bid[j_]);
                        }
                    }

                    //write edge id
                    edge.id().write(this.output);

                    //write properties
                    Map<String, Value<?>> keyValues = edge.properties().get();
                    this.output.writeByte(keyValues.size());
                    for (Map.Entry<String, Value<?>>
                           entry : keyValues.entrySet()) {

                        String key = entry.getKey();
                        byte[] bkey = CoderUtil.encode(key);
                        this.output.writeByte(bkey.length);
                        for (int i = 0; i < bkey.length; i++) {
                            this.output.writeByte((int)bkey[i]);
                        }

                        Value<?> value = entry.getValue();
                        this.output.writeByte(value.valueType().code());
                        value.write(this.output);
                    } 
                }
            }
            else {
                assert this.frequency == EdgeFrequency.MULTIPLE;
                for (Edge edge : edges) {
                    //
                    BooleanValue inv = edge.properties().get("inv");
                    boolean inv_ = (inv == null) ? false : inv.value();
                    if (inv_ && !useInvEdge) {
                        continue;
                    }
                    goodcount++;
                    byte binv = (byte)(inv_ ? 0x01 : 0x00);
                    this.output.writeByte(binv);

                    //write label
                    byte[] blabel = CoderUtil.encode(edge.label());
                    this.output.writeByte(blabel.length);
                    for (int i = 0; i < blabel.length; i++) {
                        this.output.writeByte((int)blabel[i]);
                    }

                    //write name 
                    byte[] bname = CoderUtil.encode(edge.name());
                    this.output.writeByte(bname.length);
                    for (int i = 0; i < bname.length; i++) {
                        this.output.writeByte((int)bname[i]);
                    }

                    //write targetid
                    if (!this.useFixLength) {
                        edge.targetId().write(this.output);
                    }
                    else {
                        long lid = (long)(edge.targetId().asObject());
                        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
                        buffer.putLong(0, lid);
                        byte[] bid = buffer.array();
                        for (int j = 0; j < this.idBytes; j++) {
                            int j_ = j + Long.BYTES - this.idBytes;
                            this.output.writeByte(bid[j_]);
                        }
                    }

                    //write edge id
                    edge.id().write(this.output);

                    //write properties
                    Map<String, Value<?>> keyValues = edge.properties().get();
                    this.output.writeByte(keyValues.size());
                    for (Map.Entry<String, Value<?>>
                           entry : keyValues.entrySet()) {

                        String key = entry.getKey();
                        byte[] bkey = CoderUtil.encode(key);
                        this.output.writeByte(bkey.length);
                        for (int i = 0; i < bkey.length; i++) {
                            this.output.writeByte((int)bkey[i]);
                        }

                        Value<?> value = entry.getValue();
                        this.output.writeByte(value.valueType().code());
                        value.write(this.output);
                    }
                }
            }

            long valueLength = this.output.position() - valuePosition -
                                     Constants.INT_LEN;
            this.output.writeFixedInt(valuePosition, (int) valueLength);
            this.output.writeFixedInt(countPosition, goodcount);
        } catch (IOException e) {
            throw new ComputerException("Failed to read edges from input '%s'",
                                        e, this.edgeFile.getAbsoluteFile());
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
