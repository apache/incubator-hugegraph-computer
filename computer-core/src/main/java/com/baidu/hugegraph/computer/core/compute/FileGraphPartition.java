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

package com.baidu.hugegraph.computer.core.compute;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.compute.input.EdgesInput;
import com.baidu.hugegraph.computer.core.compute.input.MessageInput;
import com.baidu.hugegraph.computer.core.compute.input.VertexInput;
import com.baidu.hugegraph.computer.core.compute.output.EdgesOutput;
import com.baidu.hugegraph.computer.core.compute.output.VertexOutput;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.graph.edge.Edges;
import com.baidu.hugegraph.computer.core.graph.partition.PartitionStat;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.io.BufferedFileInput;
import com.baidu.hugegraph.computer.core.io.BufferedFileOutput;
import com.baidu.hugegraph.computer.core.manager.Managers;
import com.baidu.hugegraph.computer.core.output.ComputerOutput;
import com.baidu.hugegraph.computer.core.sort.flusher.PeekableIterator;
import com.baidu.hugegraph.computer.core.store.FileGenerator;
import com.baidu.hugegraph.computer.core.store.FileManager;
import com.baidu.hugegraph.computer.core.store.hgkvfile.buffer.EntryIterator;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.EntriesUtil;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.Pointer;
import com.baidu.hugegraph.computer.core.worker.Computation;
import com.baidu.hugegraph.computer.core.worker.ComputationContext;
import com.baidu.hugegraph.util.E;

import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.value.IdList;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.sender.MessageSendManager;
import com.baidu.hugegraph.computer.core.graph.value.BooleanValue;

import com.baidu.hugegraph.util.Log;
import org.slf4j.Logger;

public class FileGraphPartition<M extends Value<M>> {

    private static final Logger LOG = Log.logger("partition");

    private static final String VERTEX = "vertex";
    private static final String EDGE = "edge";
    private static final String STATUS = "status";
    private static final String VALUE = "value";

    private final ComputerContext context;
    private final FileGenerator fileGenerator;
    private final int partition;

    private final File vertexFile;
    private final File edgeFile;
    private final File vertexComputeFile;
    private final File edgeComputeFile;

    private File preStatusFile;
    private File curStatusFile;
    private File preValueFile;
    private File curValueFile;

    private long vertexCount;
    private long edgeCount;

    private BufferedFileOutput curStatusOutput;
    private BufferedFileOutput curValueOutput;

    private BufferedFileInput preStatusInput;
    private BufferedFileInput preValueInput;

    private VertexInput vertexInput;
    private EdgesInput edgesInput;
    private VertexInput vertexOriginInput;
    private MessageInput<M> messageInput;

    private final MessageSendManager sendManager;
    private boolean useVariableLengthOnly;

    public FileGraphPartition(ComputerContext context,
                              Managers managers,
                              int partition) {
        this.context = context;
        this.fileGenerator = managers.get(FileManager.NAME);
        this.partition = partition;
        this.vertexFile = new File(this.fileGenerator.randomDirectory(VERTEX));
        this.edgeFile = new File(this.fileGenerator.randomDirectory(EDGE));
        this.vertexComputeFile = 
                           new File(this.fileGenerator.randomDirectory(VERTEX));
        this.edgeComputeFile = 
                           new File(this.fileGenerator.randomDirectory(EDGE));
        this.vertexCount = 0L;
        this.edgeCount = 0L;
        this.sendManager = managers.get(MessageSendManager.NAME);
        this.useVariableLengthOnly = false;
    }

    protected PartitionStat input(PeekableIterator<KvEntry> vertices,
                                  PeekableIterator<KvEntry> edges) {
        try {
            createFile(this.vertexFile);
            createFile(this.edgeFile);
            createFile(this.vertexComputeFile);
            createFile(this.edgeComputeFile);
            BufferedFileOutput vertexOut = new BufferedFileOutput(
                                           this.vertexFile);
            BufferedFileOutput edgeOut = new BufferedFileOutput(
                                         this.edgeFile);
            while (vertices.hasNext()) {
                KvEntry entry = vertices.next();
                Pointer key = entry.key();
                Pointer value = entry.value();
                this.writeVertex(key, value, vertexOut);
                this.writeEdges(key, edges, edgeOut);
            }
            vertexOut.close();
            edgeOut.close();
        } catch (IOException e) {
            throw new ComputerException(
                      "Failed to init FileGraphPartition '%s'",
                      e, this.partition);
        }

        return new PartitionStat(this.partition, this.vertexCount,
                                 this.edgeCount, 0L);
    }

    protected PartitionStat compute0(ComputationContext context,
                                     Computation<M> computation) {
        long activeVertexCount = 0L;
        try {
            this.beforeCompute(0);
        } catch (IOException e) {
            throw new ComputerException(
                      "Error occurred when beforeCompute at superstep 0", e);
        }
        while (this.vertexInput.hasNext()) {
            Vertex vertex = this.vertexInput.next();
            Edges edges = this.edgesInput.edges(this.vertexInput.idPointer());
            vertex.edges(edges);
            computation.compute0(context, vertex);
            if (vertex.active()) {
                activeVertexCount++;
            }
            try {
                this.saveVertex(vertex);
            } catch (IOException e) {
                throw new ComputerException(
                          "Error occurred when saveVertex: %s", e, vertex);
            }
        }
        try {
            this.afterCompute(0);
        } catch (Exception e) {
            throw new ComputerException("Error occurred when afterCompute", e);
        }
        return new PartitionStat(this.partition, this.vertexCount,
                                 this.edgeCount,
                                 this.vertexCount - activeVertexCount);
    }

    public void sendIdHash(ComputationContext context) {
        try {
            //this.beforeCompute(-1);
            this.vertexInput = new VertexInput(this.context,
                                       this.vertexFile, this.vertexCount);
            this.edgesInput = new EdgesInput(this.context, this.edgeFile);
            this.vertexInput.init();
            this.edgesInput.init();
        } catch (IOException e) {
            throw new ComputerException(
                      "Error occurred when beforeCompute at superstep %s",
                      e, -1);
        }
        int selfIncreaseID = 0;
        int partitionID = this.partition;
        while (this.vertexInput.hasNext()) {
            Vertex vertex = this.vertexInput.next();
            Edges edges = this.edgesInput.edges(this.vertexInput.idPointer());
            for (Edge edge : edges) {
                BooleanValue inv = edge.properties().get("inv");
                boolean inv_ = (inv == null) ? false : inv.value();
                if (!inv_) {
                    continue;
                }

                Id targetId = edge.targetId();
                long nid = (((long)partitionID) << 32) | 
                            (selfIncreaseID & 0xffffffffL);
                Id id = this.context.graphFactory().createId(nid);
                IdList path = new IdList();
                path.add(vertex.id());
                path.add(id);

                this.sendManager.sendHashIdMessage(targetId, path);
            }
            selfIncreaseID++;
        }
        try {
            this.vertexInput.close();
            this.edgesInput.close();
        } catch (IOException e) {
            throw new ComputerException(
                      "Error occurred when beforeCompute at superstep %s",
                      e, -1);
        }
    }

    public void partitionHashId() {
        try {
            //this.beforeCompute(-1);
            this.vertexInput = new VertexInput(this.context,
                                       this.vertexFile, this.vertexCount);
            this.edgesInput = new EdgesInput(this.context, this.edgeFile);
            this.vertexInput.init();
            this.edgesInput.init();
        } catch (IOException e) {
            throw new ComputerException(
                      "Error occurred when beforeCompute at superstep %s",
                      e, -1);
        }

        VertexOutput vertexOutput = new VertexOutput(
                                    this.context, this.vertexComputeFile);
        EdgesOutput edgesOutput = new EdgesOutput(
                                  this.context, this.edgeComputeFile);
        try {
            vertexOutput.init();
            edgesOutput.init();

            vertexOutput.switchToFixLength();
            edgesOutput.switchToFixLength();

            vertexOutput.writeIdBytes();
            edgesOutput.writeIdBytes();
        } catch (IOException e) {
            throw new ComputerException(
                      "Error occurred when beforeCompute at superstep %d",
                      e, 0);
        }

        LOG.info("{} begin hash and write id", this);
        long selfIncreaseID = 0;
        while (this.vertexInput.hasNext()) {
            Vertex vertex = this.vertexInput.next();
            Id id = this.context.graphFactory().createId(selfIncreaseID);
            vertex.id(id);
            vertexOutput.writeVertex(vertex);

            Iterator<M> messageIter = this.messageInput.iterator(
                                      this.vertexInput.idPointer());
 
            Edges edges = this.edgesInput.edges(
                              this.vertexInput.idPointer());
            Iterator<Edge> it = edges.iterator();
            edgesOutput.startWriteEdge(vertex);
            while (messageIter.hasNext()) {
                if (it.hasNext()) {
                    Edge edge = it.next();

                    IdList idList = (IdList)(messageIter.next());
                    Id originId = idList.get(0);
                    Id newId = idList.get(1);
             
                    edge.targetId(newId);
                    edgesOutput.writeEdge(edge);
                }
                else {
                    LOG.info("{} no hash id, may drop edge", this);
                }
            }
            edgesOutput.finishWriteEdge();
            selfIncreaseID++;
        }
        LOG.info("{} end hash and write id", this);

        try {
            this.vertexInput.close();
            this.edgesInput.close();
            vertexOutput.close();
            edgesOutput.close();
        } catch (IOException e) {
            throw new ComputerException(
                      "Error occurred when beforeCompute at superstep %d",
                      e, 0);
        }
    }

    protected PartitionStat compute(ComputationContext context,
                                    Computation<M> computation,
                                    int superstep) {
        try {
            this.beforeCompute(superstep);
        } catch (IOException e) {
            throw new ComputerException(
                      "Error occurred when beforeCompute at superstep %s",
                      e, superstep);
        }
        Value<?> result = this.context.config().createObject(
                          ComputerOptions.ALGORITHM_RESULT_CLASS);
        long activeVertexCount = 0L;
        while (this.vertexInput.hasNext()) {
            Vertex vertex = this.vertexInput.next();
            this.readVertexStatusAndValue(vertex, result);
            Iterator<M> messageIter;
            if (this.useVariableLengthOnly) {
                messageIter = this.messageInput.iterator(
                                            this.vertexInput.idPointer());
            }
            else {
                long lid = (long)vertex.id().asObject();
                messageIter = this.messageInput.iterator(lid);
            }
            if (messageIter.hasNext()) {
                vertex.reactivate();
            }

            /*
             * If the vertex is inactive, it's edges will be skipped
             * automatically at the next vertex.
             */
            if (vertex.active()) {
                Edges edges = this.edgesInput.edges(
                              this.vertexInput.idPointer());
                vertex.edges(edges);
                computation.compute(context, vertex, messageIter);
            }

            // The vertex status may be changed after computation
            if (vertex.active()) {
                activeVertexCount++;
            }

            try {
                this.saveVertex(vertex);
            } catch (IOException e) {
                throw new ComputerException(
                          "Error occurred when saveVertex", e);
            }
        }
        try {
            this.afterCompute(superstep);
        } catch (Exception e) {
            throw new ComputerException(
                      "Error occurred when afterCompute at superstep %s",
                      e, superstep);
        }
        return new PartitionStat(this.partition, this.vertexCount,
                                 this.edgeCount,
                                 this.vertexCount - activeVertexCount);
    }

    protected PartitionStat output() {
        ComputerOutput output = this.context.config().createObject(
                                ComputerOptions.OUTPUT_CLASS);
        output.init(this.context.config(), this.partition);
        try {
            this.beforeOutput();
        } catch (IOException e) {
            throw new ComputerException("Error occurred when beforeOutput", e);
        }

        Value<?> result = this.context.config().createObject(
                          ComputerOptions.ALGORITHM_RESULT_CLASS);
        while (this.vertexInput.hasNext()) {
            Vertex vertex = this.vertexInput.next();
            this.readVertexStatusAndValue(vertex, result);

            if (!this.useVariableLengthOnly) {
                if (this.vertexOriginInput.hasNext()) {
                    Vertex vertex1 = this.vertexOriginInput.next();
                    vertex.id(vertex1.id());
                }
            }
            Edges edges = this.edgesInput.edges(this.vertexInput.idPointer());
            vertex.edges(edges);
            output.write(vertex);
        }

        try {
            this.afterOutput();
        } catch (IOException e) {
            throw new ComputerException("Error occurred when afterOutput", e);
        }
        output.close();
        return new PartitionStat(this.partition, this.vertexCount,
                                 this.edgeCount, 0L);
    }

    /**
     * Put the messages sent at previous superstep from MessageRecvManager to
     * this partition. The messages is null if no messages sent to this
     * partition at previous superstep.
     */
    protected void messages(PeekableIterator<KvEntry> messages, 
                                              boolean inCompute) {
        if (!inCompute) {
            this.messageInput = new MessageInput<>(this.context,
                                                   messages, false);
        }
        else {
            this.messageInput = new MessageInput<>(this.context, 
                                                   messages, true);
        }
    }

    protected int partition() {
        return this.partition;
    }

    private void readVertexStatusAndValue(Vertex vertex, Value<?> result) {
        try {
            boolean activate = this.preStatusInput.readBoolean();
            if (activate) {
                vertex.reactivate();
            } else {
                vertex.inactivate();
            }
        } catch (IOException e) {
            throw new ComputerException(
                      "Failed to read status of vertex %s", e, vertex);
        }

        try {
            result.read(this.preValueInput);
            vertex.value(result);
        } catch (IOException e) {
            throw new ComputerException(
                      "Failed to read value of vertex %s", e, vertex);
        }
    }

    private void saveVertex(Vertex vertex) throws IOException {
        this.curStatusOutput.writeBoolean(vertex.active());
        Value<?> value = vertex.value();
        E.checkNotNull(value, "Vertex's value can't be null");
        value.write(this.curValueOutput);
    }

    private void writeVertex(Pointer key, Pointer value,
                             BufferedFileOutput vertexOut) throws IOException {
        byte[] keyBytes = key.bytes();
        vertexOut.writeFixedInt(keyBytes.length);
        vertexOut.write(keyBytes);

        byte[] valueBytes = value.bytes();
        vertexOut.writeFixedInt(valueBytes.length);
        vertexOut.write(valueBytes);

        this.vertexCount++;
    }

    private void writeEdges(Pointer vid, PeekableIterator<KvEntry> edges,
                            BufferedFileOutput edgeOut) throws IOException {
        byte[] vidBytes = vid.bytes();
        while (edges.hasNext()) {
            KvEntry entry = edges.peek();
            Pointer key = entry.key();
            int matched = vid.compareTo(key);
            if (matched < 0) {
                return;
            }

            edges.next();
            if (matched > 0) {
                // Skip stale edges
                continue;
            }
            assert matched == 0;
            edgeOut.writeFixedInt(vidBytes.length);
            edgeOut.write(vidBytes);

            long valuePosition = edgeOut.position();
            edgeOut.writeFixedInt(0);

            this.edgeCount += entry.numSubEntries();
            edgeOut.writeFixedInt((int) entry.numSubEntries());
            EntryIterator subKvIt = EntriesUtil.subKvIterFromEntry(entry);
            while (subKvIt.hasNext()) {
                KvEntry subEntry = subKvIt.next();
                // Not write sub-key length
                edgeOut.write(subEntry.key().bytes());
                // Not write sub-value length
                edgeOut.write(subEntry.value().bytes());
            }
            long valueLength = edgeOut.position() - valuePosition -
                               Constants.INT_LEN;
            edgeOut.writeFixedInt(valuePosition, (int) valueLength);
        }
    }

    //only for test when no id map applied
    public void useVariableLengthOnly() {
        this.useVariableLengthOnly = true;
    }

    private void beforeCompute(int superstep) throws IOException {

        if (this.useVariableLengthOnly) {
            LOG.info("{} workerservice use variable length id", this);
            this.vertexInput = new VertexInput(this.context, 
                                        this.vertexFile, this.vertexCount);
            this.edgesInput = new EdgesInput(this.context, this.edgeFile);
            this.vertexInput.init();
            this.edgesInput.init();
        }
        else {
            LOG.info("{} workerservice use fix length id", this);
            this.vertexInput = new VertexInput(this.context, 
                                    this.vertexComputeFile, this.vertexCount);
            this.edgesInput = new EdgesInput(this.context, 
                                    this.edgeComputeFile);
            this.vertexInput.init();
            this.edgesInput.init();
            this.vertexInput.switchToFixLength();
            this.edgesInput.switchToFixLength();
            this.vertexInput.readIdBytes();
            this.edgesInput.readIdBytes();
        }

        if (superstep > 0) {
            this.preStatusFile = this.curStatusFile;
            this.preValueFile = this.curValueFile;
            this.preStatusInput = new BufferedFileInput(this.preStatusFile);
            this.preValueInput = new BufferedFileInput(this.preValueFile);
        }

        // Outputs of vertex's status and vertex's value.
        String statusPath = this.fileGenerator.randomDirectory(
                            STATUS, Integer.toString(superstep),
                            Integer.toString(this.partition));
        String valuePath = this.fileGenerator.randomDirectory(
                           VALUE, Integer.toString(superstep),
                           Integer.toString(this.partition));
        this.curStatusFile = new File(statusPath);
        this.curValueFile = new File(valuePath);
        createFile(this.curStatusFile);
        createFile(this.curValueFile);

        this.curStatusOutput = new BufferedFileOutput(this.curStatusFile);
        this.curValueOutput = new BufferedFileOutput(this.curValueFile);
    }

    private void afterCompute(int superstep) throws Exception {
        this.vertexInput.close();
        this.edgesInput.close();
        if (superstep > 0) {
            this.messageInput.close();
            this.preStatusInput.close();
            this.preValueInput.close();
            this.preStatusFile.delete();
            this.preValueFile.delete();
        }
        this.curStatusOutput.close();
        this.curValueOutput.close();
    }

    private void beforeOutput() throws IOException {
        if (this.useVariableLengthOnly) {
            this.vertexInput = new VertexInput(this.context, 
                                     this.vertexFile, this.vertexCount);
            this.edgesInput = new EdgesInput(this.context, this.edgeFile);

            this.vertexInput.init();
            this.edgesInput.init();
        }
        else {
            this.vertexOriginInput = new VertexInput(this.context,
                                     this.vertexFile, this.vertexCount);
            this.vertexInput = new VertexInput(this.context, 
                                     this.vertexComputeFile, this.vertexCount);
            this.edgesInput = new EdgesInput(this.context, 
                                     this.edgeComputeFile);
            this.vertexOriginInput.init();
            this.vertexInput.init();
            this.edgesInput.init();

            vertexInput.switchToFixLength();
            edgesInput.switchToFixLength();

            vertexInput.readIdBytes();
            edgesInput.readIdBytes();
        }

        this.preStatusFile = this.curStatusFile;
        this.preValueFile = this.curValueFile;
        this.preStatusInput = new BufferedFileInput(this.preStatusFile);
        this.preValueInput = new BufferedFileInput(this.preValueFile);
    }

    private void afterOutput() throws IOException {
        this.vertexInput.close();
        this.edgesInput.close();

        this.preStatusInput.close();
        this.preValueInput.close();

        assert this.preStatusFile == this.curStatusFile;
        assert this.preValueFile == this.curValueFile;
        this.preStatusFile.delete();
        this.preValueFile.delete();

        this.vertexFile.delete();
        this.edgeFile.delete();

        this.vertexComputeFile.delete();
        this.edgeComputeFile.delete();
    }

    private static void createFile(File file) throws IOException {
        file.getParentFile().mkdirs();
        E.checkArgument(file.createNewFile(), "Already exists file: %s", file);
    }
}
