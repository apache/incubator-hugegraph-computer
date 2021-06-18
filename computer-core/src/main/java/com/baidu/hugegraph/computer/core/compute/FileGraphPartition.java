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

public class FileGraphPartition<M extends Value<M>> {

    private final ComputerContext context;
    private static final String VERTEX = "vertex";
    private static final String EDGE = "edge";
    private static final String STATUS = "status";
    private static final String VALUE = "value";

    private final FileGenerator fileGenerator;
    private final int partition;

    private File vertexFile;
    private File edgeFile;
    private File preStatusFile;
    private File curStatusFile;
    private File preValueFile;
    private File curValueFile;

    private long vertexCount;
    private long edgeCount;

    private BufferedFileOutput curStatusOut;
    private BufferedFileOutput curValueOut;
    private BufferedFileInput preStatusIn;
    private BufferedFileInput preValueIn;
    private VertexInput vertexInput;
    private EdgesInput edgesInput;
    private MessageInput<M> messageInput;
    private final Value<?> result;

    public FileGraphPartition(ComputerContext context, Managers managers,
                       int partition) {
        this.context = context;
        this.fileGenerator = managers.get(FileManager.NAME);
        this.partition = partition;
        this.vertexFile = new File(this.fileGenerator.randomDirectory(VERTEX));
        this.edgeFile = new File(this.fileGenerator.randomDirectory(EDGE));
        this.result = this.context.config().createObject(
                      ComputerOptions.ALGORITHM_RESULT_CLASS);
        this.vertexCount = 0L;
        this.edgeCount = 0L;
    }

    public PartitionStat init(PeekableIterator<KvEntry> vertices,
                              PeekableIterator<KvEntry> edges) {
        if (edges == null) {
            edges = PeekableIterator.emptyIterator();
        }
        try {
            createFile(this.vertexFile);
            createFile(this.edgeFile);
            BufferedFileOutput vertexOut = new BufferedFileOutput(
                                           this.vertexFile);
            BufferedFileOutput edgeOut = new BufferedFileOutput(this.edgeFile);
            while (vertices.hasNext()) {
                KvEntry entry = vertices.next();
                Pointer key = entry.key();
                vertexOut.writeFixedInt(key.bytes().length);
                vertexOut.write(key.bytes());
                Pointer value = entry.value();
                vertexOut.writeFixedInt(value.bytes().length);
                vertexOut.write(value.bytes());
                this.writeEdges(key, edges, edgeOut);
                this.vertexCount++;
            }
            vertexOut.close();
            edgeOut.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new ComputerException("Init vertex input failed", e);
        }

        return new PartitionStat(this.partition, this.vertexCount,
                                 this.edgeCount);
    }

    // Package access
    PartitionStat compute0(ComputationContext context,
                           Computation<M> computation) {
        long activeVertexCount = 0;
        this.beforeCompute(0);

        while (this.vertexInput.hasNext()) {
            Vertex vertex = this.vertexInput.next();
            Edges edges = this.edgesInput.edges(this.vertexInput.idPointer());
            vertex.edges(edges);
            computation.compute0(context, vertex);
            if (vertex.active()) {
                activeVertexCount++;
            }
            this.saveVertex(vertex);
        }
        this.afterCompute(0);

        return new PartitionStat(this.partition, this.vertexCount,
                                 this.edgeCount,
                                 this.vertexCount - activeVertexCount, 0L, 0L);
    }

    // Package access
    PartitionStat compute(ComputationContext context,
                          Computation<M> computation,
                          int superstep) {
        this.beforeCompute(superstep);
        long activeVertexCount = 0L;
        while (this.vertexInput.hasNext()) {
            Vertex vertex = this.vertexInput.next();
            Iterator<M> messageIt = this.messageInput.iterator(
                                    this.vertexInput.idPointer());
            try {
                boolean status = this.preStatusIn.readBoolean();
                this.result.read(this.preValueIn);
                vertex.value(this.result);
                if (status || messageIt.hasNext()) {
                    vertex.reactivate();
                } else {
                    vertex.inactivate();
                }
            } catch (IOException e) {
                throw new ComputerException("Can't read status or result", e);
            }

            /*
             * If the vertex is inactive, it's edges will be skipped
             * automatically at the next vertex.
             */
            if (vertex.active()) {
                Edges edges = this.edgesInput.edges(
                              this.vertexInput.idPointer());
                vertex.edges(edges);
                computation.compute(context, vertex, messageIt);
            }

            // Computation may change vertex status.
            if (vertex.active()) {
                activeVertexCount++;
            }
            this.saveVertex(vertex);
        }
        this.afterCompute(superstep);
        return new PartitionStat(this.partition, this.vertexCount,
                                 this.edgeCount,
                                 this.vertexCount - activeVertexCount, 0L, 0L);
    }

    public PartitionStat output() {
        ComputerOutput output = this.context.config().createObject(
                                ComputerOptions.OUTPUT_CLASS);
        output.init(this.context.config(), this.partition);
        this.beforeOutput();
        while (this.vertexInput.hasNext()) {
            Vertex vertex = this.vertexInput.next();
            try {
                boolean status = this.preStatusIn.readBoolean();
                this.result.read(this.preValueIn);
                vertex.value(this.result);
                if (status) {
                    vertex.reactivate();
                } else {
                    vertex.inactivate();
                }
            } catch (IOException e) {
                throw new ComputerException("Can't read status or result", e);
            }

            Edges edges = this.edgesInput.edges(this.vertexInput.idPointer());
            vertex.edges(edges);
            output.write(vertex);
        }
        this.afterOutput();
        output.close();
        return new PartitionStat(this.partition, this.vertexCount,
                                 this.edgeCount);
    }

    public void messages(PeekableIterator<KvEntry> messages) {
        this.messageInput = new MessageInput<>(this.context, messages);
    }

    public int partition() {
        return this.partition;
    }

    private void saveVertex(Vertex vertex) {
        try {
            this.curStatusOut.writeBoolean(vertex.active());
            Value<?> value = vertex.value();
            E.checkNotNull(value, "Vertex's value can't be null");
            vertex.value().write(this.curValueOut);
        } catch (IOException e) {
            throw new ComputerException("Save vertex failed, vertex id {}",
                                        vertex.id());
        }
    }

    private void writeEdges(Pointer id, PeekableIterator<KvEntry> edges,
                            BufferedFileOutput edgeOut)
                            throws IOException {
        while (edges.hasNext()) {
            KvEntry entry = edges.peek();
            Pointer key = entry.key();
            int status = id.compareTo(key);
            if (status < 0) {
                return;
            } else if (status == 0) {
                edges.next();
                edgeOut.writeFixedInt(id.bytes().length);
                edgeOut.write(id.bytes());

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
                int valueLength = (int) (edgeOut.position() - valuePosition -
                                         Constants.INT_LEN);
                edgeOut.writeFixedInt(valuePosition, valueLength);
            } else  { // status > 0
                edges.next();
            }
        }
    }

    private void beforeCompute(int superstep) {
        this.vertexInput = new VertexInput(this.context,
                                           this.vertexFile,
                                           this.vertexCount);
        this.edgesInput = new EdgesInput(this.context, this.edgeFile);
        // Inputs
        try {
            this.vertexInput.init();
            this.edgesInput.init();
            if (superstep != 0) {
                this.preStatusFile = this.curStatusFile;
                this.preValueFile = this.curValueFile;
                this.preStatusIn = new BufferedFileInput(this.preStatusFile);
                this.preValueIn = new BufferedFileInput(this.preValueFile);
            }

            // Outputs
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

            this.curStatusOut = new BufferedFileOutput(this.curStatusFile);
            this.curValueOut = new BufferedFileOutput(this.curValueFile);

        } catch (IOException e) {
            throw new ComputerException("Before compute call failed", e);
        }
    }

    private void afterCompute(int superstep) {
        try {
            this.vertexInput.close();
            this.edgesInput.close();
            if (superstep != 0) {
                this.preStatusIn.close();
                this.preValueIn.close();
                this.preStatusFile.delete();
                this.preValueFile.delete();
            }
            this.curStatusOut.close();
            this.curValueOut.close();
        } catch (IOException e) {
            throw new ComputerException("After compute call failed", e);
        }
    }

    private void beforeOutput() {
        this.vertexInput = new VertexInput(this.context,
                                           this.vertexFile,
                                           this.vertexCount);
        this.edgesInput = new EdgesInput(this.context, this.edgeFile);
        try {
            this.vertexInput.init();
            this.edgesInput.init();

            this.preStatusFile = this.curStatusFile;
            this.preValueFile = this.curValueFile;
            this.preStatusIn = new BufferedFileInput(this.preStatusFile);
            this.preValueIn = new BufferedFileInput(this.preValueFile);
        } catch (IOException e) {
            throw new ComputerException("Before output call failed", e);
        }
    }

    private void afterOutput() {
        try {
            this.vertexInput.close();
            this.edgesInput.close();
            this.preStatusIn.close();
            this.preValueIn.close();
            this.preStatusFile.delete();
            this.preValueFile.delete();
            this.vertexInput.close();
            this.edgesInput.close();
        } catch (IOException e) {
            throw new ComputerException("After output call failed", e);
        }
    }

    private static void createFile(File file) throws IOException {
        file.getParentFile().mkdirs();
        file.createNewFile();
    }
}
