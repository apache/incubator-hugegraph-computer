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

package org.apache.hugegraph.computer.core.store.file.select;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hugegraph.computer.core.store.file.hgkvfile.HgkvDir;
import org.apache.hugegraph.computer.core.store.file.hgkvfile.HgkvDirImpl;
import org.apache.hugegraph.computer.core.store.file.hgkvfile.HgkvFile;
import org.apache.hugegraph.util.E;

import com.google.common.collect.Lists;

public class DisperseEvenlySelector implements InputFilesSelector {

    @Override
    public List<SelectedFiles> selectedByHgkvFile(List<String> inputs,
                                                  List<String> outputs)
                                                  throws IOException {
        E.checkArgument(inputs.size() >= outputs.size(),
                        "The inputs size of InputFilesSelector must be >= " +
                        "outputs size, but got %s inputs < %s outputs",
                        inputs.size(), outputs.size());

        List<HgkvDir> inputDirs = new ArrayList<>();
        for (String input : inputs) {
            inputDirs.add(HgkvDirImpl.open(input));
        }
        /*
         * Reverse sort by entries number first
         * will make the disperse result more uniform
         */
        inputDirs = inputDirs.stream()
                             .sorted(Comparator.comparingLong(
                                     HgkvFile::numEntries).reversed())
                             .collect(Collectors.toList());

        // Init heap data
        List<Node> heapNodes = new ArrayList<>(outputs.size());
        int i = 0;
        for (; i < outputs.size(); i++) {
            HgkvDir inputDir = inputDirs.get(i);
            Node heapNode = new Node(inputDir.numEntries(),
                                     Lists.newArrayList(inputDir.path()),
                                     outputs.get(i));
            heapNodes.add(heapNode);
        }
        Heap<Node> heap = new Heap<>(heapNodes,
                                     Comparator.comparingLong(Node::num));
        // Distribute the remaining input
        for (; i < inputDirs.size(); i++) {
            HgkvDir inputDir = inputDirs.get(i);
            Node topNode = heap.top();
            topNode.addInput(inputDir.path());
            topNode.addNum(inputDir.numEntries());
            heap.adjust(0);
        }

        List<SelectedFiles> results = new ArrayList<>();
        for (Node node : heapNodes) {
            SelectedFiles result = new DefaultSelectedFiles(node.output(),
                                                            node.inputs());
            results.add(result);
        }

        return results;
    }

    @Override
    public List<SelectedFiles> selectedByBufferFile(List<String> inputs,
                                                    List<String> outputs) {
        E.checkArgument(inputs.size() >= outputs.size(),
                        "The inputs size of InputFilesSelector must be >= " +
                        "outputs size, but got %s inputs < %s outputs",
                        inputs.size(), outputs.size());

        // TODO: design a better way of distribute
        int size = outputs.size();
        List<List<String>> group = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            group.add(new ArrayList<>());
        }
        for (int i = 0; i < inputs.size(); i++) {
            List<String> item = group.get(i % size);
            item.add(inputs.get(i));
        }
        List<SelectedFiles> result = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            result.add(new DefaultSelectedFiles(outputs.get(i), group.get(i)));
        }

        return result;
    }

    private static class Heap<T> {

        private final List<T> data;
        private final Comparator<T> comparator;
        private final int size;

        public Heap(List<T> data, Comparator<T> comparator) {
            this.data = data;
            this.size = data.size();
            this.comparator = comparator;

            this.buildHeap(this.size);
        }

        private void buildHeap(int size) {
            for (int index = (size >> 1) - 1; index >= 0; index--) {
                this.adjust(index);
            }
        }

        private void adjust(int index) {
            int child;
            while ((child = (index << 1) + 1) < this.size) {
                if (child < this.size - 1 &&
                    this.compare(this.data.get(child),
                                 this.data.get(child + 1)) > 0) {
                    child++;
                }
                if (this.compare(this.data.get(index),
                                 this.data.get(child)) > 0) {
                    this.swap(index, child);
                    index = child;
                } else {
                    break;
                }
            }
        }

        private T top() {
            return this.data.get(0);
        }

        private void swap(int i, int j) {
            T tmp = this.data.get(i);
            this.data.set(i, this.data.get(j));
            this.data.set(j, tmp);
        }

        private int compare(T t1, T t2) {
            return this.comparator.compare(t1, t2);
        }
    }

    private static class Node {

        private long num;
        private final List<String> inputs;
        private final String output;

        public Node(long numEntries, List<String> inputs, String output) {
            this.num = numEntries;
            this.inputs = inputs;
            this.output = output;
        }

        public long num() {
            return this.num;
        }

        public void addNum(long num) {
            this.num += num;
        }

        public List<String> inputs() {
            return this.inputs;
        }

        public void addInput(String input) {
            this.inputs.add(input);
        }

        public String output() {
            return this.output;
        }
    }
}
