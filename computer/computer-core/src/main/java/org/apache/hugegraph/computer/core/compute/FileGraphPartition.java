/*
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

package org.apache.hugegraph.computer.core.compute;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hugegraph.computer.core.common.ComputerContext;
import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.compute.input.EdgesInput;
import org.apache.hugegraph.computer.core.compute.input.MessageInput;
import org.apache.hugegraph.computer.core.compute.input.VertexInput;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.graph.edge.Edges;
import org.apache.hugegraph.computer.core.graph.partition.PartitionStat;
import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.computer.core.io.BufferedFileInput;
import org.apache.hugegraph.computer.core.io.BufferedFileOutput;
import org.apache.hugegraph.computer.core.manager.Managers;
import org.apache.hugegraph.computer.core.output.ComputerOutput;
import org.apache.hugegraph.computer.core.sort.flusher.PeekableIterator;
import org.apache.hugegraph.computer.core.store.EntryIterator;
import org.apache.hugegraph.computer.core.store.FileGenerator;
import org.apache.hugegraph.computer.core.store.FileManager;
import org.apache.hugegraph.computer.core.store.entry.EntriesUtil;
import org.apache.hugegraph.computer.core.store.entry.KvEntry;
import org.apache.hugegraph.computer.core.store.entry.Pointer;
import org.apache.hugegraph.computer.core.worker.Computation;
import org.apache.hugegraph.computer.core.worker.ComputationContext;
import org.apache.hugegraph.computer.core.worker.WorkerContext;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public class FileGraphPartition {

    private static final Logger LOG = Log.logger(FileGraphPartition.class);

    private static final String VERTEX = "vertex";
    private static final String EDGE = "edge";
    private static final String STATUS = "status";
    private static final String VALUE = "value";

    private final ComputerContext context;
    private final Computation<Value> computation;
    private final FileGenerator fileGenerator;
    private final int partition;

    private final File vertexFile;
    private final File edgeFile;

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
    private MessageInput<Value> messageInput;

    public FileGraphPartition(ComputerContext context,
                              Managers managers,
                              int partition) {
        this.context = context;
        this.computation = context.config()
                                  .createObject(
                                          ComputerOptions.WORKER_COMPUTATION_CLASS);
        this.computation.init(context.config());
        this.fileGenerator = managers.get(FileManager.NAME);
        this.partition = partition;
        this.vertexFile = new File(this.fileGenerator.randomDirectory(VERTEX));
        this.edgeFile = new File(this.fileGenerator.randomDirectory(EDGE));
        this.vertexCount = 0L;
        this.edgeCount = 0L;
    }

    protected PartitionStat input(PeekableIterator<KvEntry> vertices,
                                  PeekableIterator<KvEntry> edges) {
        try {
            createFile(this.vertexFile);
            createFile(this.edgeFile);
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

    protected PartitionStat compute(WorkerContext context,
                                    int superstep) {
        LOG.info("Partition {} begin compute in superstep {}",
                 this.partition, superstep);
        try {
            this.beforeCompute(superstep);
        } catch (IOException e) {
            throw new ComputerException(
                      "Error occurred when beforeCompute at superstep %s",
                      e, superstep);
        }

        long activeVertexCount;
        try {
            this.computation.beforeSuperstep(context);
            activeVertexCount = superstep == 0 ?
                                this.compute0(context) :
                                this.compute1(context);
            this.computation.afterSuperstep(context);
        } catch (Exception e) {
            throw new ComputerException(
                      "Error occurred when compute at superstep %s",
                      e, superstep);
        }

        try {
            this.afterCompute(superstep);
        } catch (Exception e) {
            throw new ComputerException(
                      "Error occurred when afterCompute at superstep %s",
                      e, superstep);
        }

        LOG.info("Partition {} finish compute in superstep {}",
                 this.partition, superstep);

        return new PartitionStat(this.partition, this.vertexCount,
                                 this.edgeCount,
                                 this.vertexCount - activeVertexCount);
    }


    private long compute0(ComputationContext context) {
        long activeVertexCount = 0L;
        while (this.vertexInput.hasNext()) {
            Vertex vertex = this.vertexInput.next();
            vertex.reactivate();

            Edges edges = this.edgesInput.edges(this.vertexInput.idPointer());
            vertex.edges(edges);

            this.computation.compute0(context, vertex);

            if (vertex.active()) {
                activeVertexCount++;
            }

            try {
                this.saveVertexStatusAndValue(vertex);
            } catch (IOException e) {
                throw new ComputerException(
                          "Error occurred when saveVertex: %s", e, vertex);
            }
        }
        return activeVertexCount;
    }

    private long compute1(ComputationContext context) {
        Value result = this.context.config().createObject(
                       ComputerOptions.ALGORITHM_RESULT_CLASS);
        long activeVertexCount = 0L;
        while (this.vertexInput.hasNext()) {
            Vertex vertex = this.vertexInput.next();
            this.readVertexStatusAndValue(vertex, result);

            Iterator<Value> messageIter = this.messageInput.iterator(
                                          this.vertexInput.idPointer());
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
                this.computation.compute(context, vertex, messageIter);
            }

            // The vertex status may be changed after computation
            if (vertex.active()) {
                activeVertexCount++;
            }

            try {
                this.saveVertexStatusAndValue(vertex);
            } catch (IOException e) {
                throw new ComputerException(
                          "Error occurred when saveVertex", e);
            }
        }
        return activeVertexCount;
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

        Value result = this.context.config().createObject(
                       ComputerOptions.ALGORITHM_RESULT_CLASS);
        while (this.vertexInput.hasNext()) {
            Vertex vertex = this.vertexInput.next();
            this.readVertexStatusAndValue(vertex, result);

            Edges edges = this.edgesInput.edges(this.vertexInput.idPointer());
            vertex.edges(edges);

            if (output.filter(this.context.config(), this.computation, vertex)) {
                output.write(vertex);
            }
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
    protected void messages(PeekableIterator<KvEntry> messages) {
        this.messageInput = new MessageInput<>(this.context, messages);
    }

    protected int partition() {
        return this.partition;
    }

    private void readVertexStatusAndValue(Vertex vertex, Value result) {
        try {
            boolean activate = this.preStatusInput.readBoolean();
            if (activate) {
                vertex.reactivate();
            } else {
                vertex.inactivate();
            }
        } catch (IOException e) {
            throw new ComputerException(
                      "Failed to read status of vertex '%s'", e, vertex);
        }

        try {
            result.read(this.preValueInput);
            vertex.value(result);
        } catch (IOException e) {
            throw new ComputerException(
                      "Failed to read value of vertex '%s'", e, vertex);
        }
    }

    private void saveVertexStatusAndValue(Vertex vertex) throws IOException {
        this.curStatusOutput.writeBoolean(vertex.active());
        Value value = vertex.value();
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

    private void beforeCompute(int superstep) throws IOException {
        this.vertexInput = new VertexInput(this.context, this.vertexFile,
                                           this.vertexCount);
        this.edgesInput = new EdgesInput(this.context, this.edgeFile);
        // Inputs of vertex, edges, status, and value.
        this.vertexInput.init();
        this.edgesInput.init();
        if (superstep != 0) {
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
        if (superstep != 0) {
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
        this.vertexInput = new VertexInput(this.context, this.vertexFile,
                                           this.vertexCount);
        this.edgesInput = new EdgesInput(this.context, this.edgeFile);

        this.vertexInput.init();
        this.edgesInput.init();

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
    }

    private static void createFile(File file) throws IOException {
        file.getParentFile().mkdirs();
        E.checkArgument(file.createNewFile(), "Already exists file: %s", file);
    }
}
