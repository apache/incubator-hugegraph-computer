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

package com.baidu.hugegraph.computer.core.input.loader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.input.ElementFetcher;
import com.baidu.hugegraph.computer.core.input.InputSplit;
import com.baidu.hugegraph.loader.builder.ElementBuilder;
import com.baidu.hugegraph.loader.builder.SchemaCache;
import com.baidu.hugegraph.loader.constant.Constants;
import com.baidu.hugegraph.loader.executor.ComputerLoadOptions;
import com.baidu.hugegraph.loader.executor.LoadContext;
import com.baidu.hugegraph.loader.mapping.InputStruct;
import com.baidu.hugegraph.loader.reader.InputReader;
import com.baidu.hugegraph.loader.reader.file.FileReader;
import com.baidu.hugegraph.loader.reader.line.Line;
import com.baidu.hugegraph.loader.source.file.FileSource;
import com.baidu.hugegraph.loader.util.JsonUtil;
import com.baidu.hugegraph.structure.GraphElement;

public abstract class LoaderElementFetcher<T extends GraphElement>
       implements ElementFetcher<T>  {

    private final Config config;
    private final LoadContext context;
    private Iterator<T> localBatch;
    private Iterator<Line> localLineBatch;
    private List<ElementBuilder<T>> builders;
    private T next;

    public LoaderElementFetcher(Config config) {
        this.config = config;
        String schemaPath = this.config.get(
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
        FileInputSplit fileInputSplit = (FileInputSplit) split;
        this.localLineBatch = this.fetch(fileInputSplit);
        this.builders = this.elementBuilders(this.context,
                                             fileInputSplit.struct());
    }

    @Override
    public boolean hasNext() {
        if (this.next != null) {
            return true;
        }

        if (this.localBatch != null && this.localBatch.hasNext()) {
            this.next = this.localBatch.next();
            return true;
        } else {
            this.localBatch = null;

            if (this.localLineBatch != null) {
                while (this.localLineBatch.hasNext()) {
                    Line line = this.localLineBatch.next();
                    List<T> allElements = new ArrayList<>();
                    for (ElementBuilder<T> builder : this.builders) {
                        List<T> elements = this.buildElement(line, builder);
                        if (CollectionUtils.isNotEmpty(elements)) {
                            allElements.addAll(elements);
                        }
                    }
                    if (CollectionUtils.isNotEmpty(allElements)) {
                        this.localBatch = allElements.iterator();
                        this.next = this.localBatch.next();
                        return true;
                    }
                }
                this.localLineBatch = null;
            }
            return false;
        }
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

    public Iterator<Line> fetch(FileInputSplit split) {
        String path = split.path();
        InputStruct struct = split.struct();
        FileSource source = (FileSource) struct.input();
        source.path(path);
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
}
