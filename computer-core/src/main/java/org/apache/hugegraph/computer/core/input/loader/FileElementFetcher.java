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

package org.apache.hugegraph.computer.core.input.loader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.input.ElementFetcher;
import org.apache.hugegraph.computer.core.input.InputSplit;
import org.apache.hugegraph.iterator.FlatMapperIterator;
import org.apache.hugegraph.loader.builder.ElementBuilder;
import org.apache.hugegraph.loader.builder.SchemaCache;
import org.apache.hugegraph.loader.constant.Constants;
import org.apache.hugegraph.loader.executor.ComputerLoadOptions;
import org.apache.hugegraph.loader.executor.LoadContext;
import org.apache.hugegraph.loader.mapping.InputStruct;
import org.apache.hugegraph.loader.reader.InputReader;
import org.apache.hugegraph.loader.reader.file.FileReader;
import org.apache.hugegraph.loader.reader.line.Line;
import org.apache.hugegraph.loader.source.file.FileSource;
import org.apache.hugegraph.loader.util.JsonUtil;
import org.apache.hugegraph.structure.GraphElement;

public abstract class FileElementFetcher<T extends GraphElement>
                implements ElementFetcher<T>  {

    private final LoadContext context;
    private List<ElementBuilder<T>> builders;
    private InputReader inputReader;
    private FlatMapperIterator<Line, T> localBatch;
    private T next;

    public FileElementFetcher(Config config) {
        String schemaPath = config.get(
                            ComputerOptions.INPUT_LOADER_SCHEMA_PATH);
        SchemaCache schemaCache;
        try {
             String json = FileUtils.readFileToString(new File(schemaPath),
                                                      Constants.CHARSET);
             schemaCache = JsonUtil.fromJson(json, SchemaCache.class);
        } catch (IOException exception) {
            throw new ComputerException("Failed to load schema from file, " +
                                        "path:%s", schemaPath);
        }

        ComputerLoadOptions options = new ComputerLoadOptions(schemaCache);
        this.context = new LoadContext(options);
    }

    @Override
    public void prepareLoadInputSplit(InputSplit split) {
        if (this.inputReader != null) {
            this.inputReader.close();
        }

        FileInputSplit fileInputSplit = (FileInputSplit) split;
        this.builders = this.elementBuilders(this.context,
                                             fileInputSplit.struct());
        this.inputReader = this.createReader(fileInputSplit);
        this.localBatch = new FlatMapperIterator<>(this.inputReader, line -> {
               List<T> allElements = new ArrayList<>();
               for (ElementBuilder<T> builder : this.builders) {
                   List<T> elements = this.buildElement(line, builder);
                   allElements.addAll(elements);
               }
               return allElements.iterator();
        });
    }

    @Override
    public boolean hasNext() {
        if (this.next != null) {
            return true;
        }

        if (this.localBatch != null && this.localBatch.hasNext()) {
            this.next = this.localBatch.next();
            return true;
        }

        this.localBatch = null;
        return false;
    }

    @Override
    public T next() {
        if (!this.hasNext()) {
            throw new NoSuchElementException();
        }
        T current = this.next;
        this.next = null;
        return current;
    }

    private InputReader createReader(FileInputSplit split) {
        InputStruct struct = split.struct();
        FileSource source = (FileSource) struct.input();
        source.path(split.path());
        FileReader reader = (FileReader) InputReader.create(struct.input());
        reader.init(this.context, struct);
        return reader;
    }

    protected List<T> buildElement(Line line, ElementBuilder<T> builder) {
        return builder.build(line.names(), line.values());
    }

    protected abstract List<ElementBuilder<T>> elementBuilders(
                                               LoadContext context,
                                               InputStruct struct);

    public void close() {
        if (this.inputReader != null) {
            this.inputReader.close();
        }
    }
}
