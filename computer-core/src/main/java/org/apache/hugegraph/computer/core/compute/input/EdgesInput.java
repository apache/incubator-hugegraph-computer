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

package org.apache.hugegraph.computer.core.compute.input;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

import javax.annotation.Nonnull;

import org.apache.hugegraph.computer.core.common.ComputerContext;
import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.EdgeFrequency;
import org.apache.hugegraph.computer.core.graph.GraphFactory;
import org.apache.hugegraph.computer.core.graph.edge.Edge;
import org.apache.hugegraph.computer.core.graph.edge.Edges;
import org.apache.hugegraph.computer.core.graph.properties.Properties;
import org.apache.hugegraph.computer.core.io.BufferedFileInput;
import org.apache.hugegraph.computer.core.io.RandomAccessInput;
import org.apache.hugegraph.computer.core.io.StreamGraphInput;

public class EdgesInput {

    private RandomAccessInput input;
    private final ReusablePointer idPointer;
    private final ReusablePointer valuePointer;
    private final File edgeFile;
    private final GraphFactory graphFactory;
    private final int flushThreshold;
    private final EdgeFrequency frequency;

    public EdgesInput(ComputerContext context, File edgeFile) {
        this.graphFactory = context.graphFactory();
        this.idPointer = new ReusablePointer();
        this.valuePointer = new ReusablePointer();
        this.edgeFile = edgeFile;
        this.flushThreshold = context.config().get(
                ComputerOptions.INPUT_MAX_EDGES_IN_ONE_VERTEX);
        this.frequency = context.config().get(ComputerOptions.INPUT_EDGE_FREQ);
    }

    public void init() throws IOException {
        this.input = new BufferedFileInput(this.edgeFile);
    }

    public void close() throws IOException {
        this.input.close();
    }

    public Edges edges(ReusablePointer vidPointer) {
        try {
            while (this.input.available() > 0) {
                long startPosition = this.input.position();
                this.idPointer.read(this.input);
                int status = vidPointer.compareTo(this.idPointer);
                if (status < 0) { // No edges
                    /*
                     * The current batch belong to vertex that vertex id is
                     * bigger than specified id.
                     */
                    this.input.seek(startPosition);
                    return EmptyEdges.instance();
                } else if (status == 0) { // Has edges
                    this.valuePointer.read(this.input);
                    Edges edges = this.readEdges(this.valuePointer.input());
                    if (edges.size() < this.flushThreshold) {
                        return edges;
                    } else {
                        return new SuperEdges(vidPointer, edges, startPosition);
                    }
                } else {
                    /*
                     * The current batch belong to vertex that vertex id is
                     * smaller than specified id.
                     */
                    int valueLength = this.input.readFixedInt();
                    this.input.skip(valueLength);
                }
            }
            return EmptyEdges.instance();
        } catch (IOException e) {
            throw new ComputerException("Can't read from edges input '%s'",
                                        e, this.edgeFile.getAbsoluteFile());
        }
    }

    private class SuperEdges implements Edges {

        private final ReusablePointer vid;
        private final long startPosition;
        private Iterator<Edge> currentEdgesIter;
        private int size;
        private int iterationTime;

        SuperEdges(ReusablePointer vid, Edges edges, long startPosition) {
            this.vid = vid;
            this.startPosition = startPosition;
            this.currentEdgesIter = edges.iterator();
            this.size = 0;
            this.iterationTime = 0;
        }

        @Override
        public int size() {
            if (this.size == 0) {
                this.calculateSize();
            }
            return this.size;
        }

        private void calculateSize() {
            long currentPosition = EdgesInput.this.input.position();
            try {
                EdgesInput.this.input.seek(this.startPosition);
                EdgesInput.this.idPointer.read(EdgesInput.this.input);
                while (EdgesInput.this.idPointer.compareTo(this.vid) == 0) {
                    long valueLength = EdgesInput.this.input.readFixedInt();
                    this.size += EdgesInput.this.input.readInt();
                    long bytesToSkip = valueLength - Constants.INT_LEN;
                    EdgesInput.this.input.skip(bytesToSkip);
                    if (EdgesInput.this.input.available() > 0) {
                        EdgesInput.this.idPointer.read(EdgesInput.this.input);
                    } else {
                        break;
                    }
                }
                EdgesInput.this.input.seek(currentPosition);
            } catch (IOException e) {
                throw new ComputerException("Compute size of edges failed", e);
            }
        }

        @Override
        public void add(Edge edge) {
            throw new ComputerException(
                      "Not support adding edges during computing");
        }

        @Override
        @Nonnull
        public Iterator<Edge> iterator() {
            try {
                if (this.iterationTime != 0) {
                    EdgesInput.this.input.seek(this.startPosition);
                }
                this.iterationTime++;
            } catch (IOException e) {
                throw new ComputerException("Can't seek to %s",
                                            e, this.startPosition);
            }
            return new EdgesIterator();

        }

        private class EdgesIterator implements Iterator<Edge> {

            @Override
            public boolean hasNext() {
                if (currentEdgesIter.hasNext()) {
                    return true;
                }
                long currentPosition = input.position();
                try {
                    if (input.available() > 0) {
                        idPointer.read(input);
                        if (idPointer.compareTo(vid) == 0) {
                            valuePointer.read(input);
                            currentEdgesIter = readEdges(valuePointer.input())
                                               .iterator();
                        } else {
                            input.seek(currentPosition);
                        }
                    }
                } catch (IOException e) {
                    throw new ComputerException(
                              "Error occurred when read edges from edges " +
                              "input '%s' at position %s", e,
                              edgeFile.getAbsoluteFile(), currentPosition);
                }
                return currentEdgesIter.hasNext();
            }

            @Override
            public Edge next() {
                return currentEdgesIter.next();
            }
        }
    }

    // TODO: use one reused Edges instance to read batches for each vertex.
    private Edges readEdges(RandomAccessInput in) {
        try {
            int count = in.readFixedInt();
            Edges edges = this.graphFactory.createEdges(count);
            if (this.frequency == EdgeFrequency.SINGLE) {
                for (int i = 0; i < count; i++) {
                    Edge edge = this.graphFactory.createEdge();
                    // Only use targetId as subKey, use props as subValue
                    edge.targetId(StreamGraphInput.readId(in));
                    // Read subValue
                    Properties props = this.graphFactory.createProperties();
                    props.read(in);
                    edge.properties(props);
                    edges.add(edge);
                }
            } else if (this.frequency == EdgeFrequency.SINGLE_PER_LABEL) {
                for (int i = 0; i < count; i++) {
                    Edge edge = this.graphFactory.createEdge();
                    // Use label + targetId as subKey, use props as subValue
                    edge.label(StreamGraphInput.readLabel(in));
                    edge.targetId(StreamGraphInput.readId(in));
                    // Read subValue
                    Properties props = this.graphFactory.createProperties();
                    props.read(in);
                    edge.properties(props);
                    edges.add(edge);
                }
            } else {
                assert this.frequency == EdgeFrequency.MULTIPLE;
                for (int i = 0; i < count; i++) {
                    Edge edge = this.graphFactory.createEdge();
                    /*
                     * Use label + sortValues + targetId as subKey,
                     * use properties as subValue
                     */
                    edge.label(StreamGraphInput.readLabel(in));
                    edge.name(StreamGraphInput.readLabel(in));
                    edge.targetId(StreamGraphInput.readId(in));
                    // Read subValue
                    Properties props = this.graphFactory.createProperties();
                    props.read(in);
                    edge.properties(props);
                    edges.add(edge);
                }
            }
            return edges;
        } catch (IOException e) {
            throw new ComputerException("Failed to read edges from input '%s'",
                                        e, this.edgeFile.getAbsoluteFile());
        }
    }

    public static class EmptyEdges implements Edges {

        private static final EmptyEdges INSTANCE = new EmptyEdges();

        private EmptyEdges() {
            // pass
        }

        public static EmptyEdges instance() {
            return INSTANCE;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public void add(Edge edge) {
            throw new ComputerException(
                      "Not support adding edges during computing");
        }

        @Override
        public Iterator<Edge> iterator() {
            return Collections.emptyIterator();
        }
    }
}
