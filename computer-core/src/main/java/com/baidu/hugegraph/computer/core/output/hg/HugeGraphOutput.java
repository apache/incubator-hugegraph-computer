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

package com.baidu.hugegraph.computer.core.output.hg;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.value.ValueType;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.output.AbstractComputerOutput;
import com.baidu.hugegraph.computer.core.output.hg.task.TaskManager;
import com.baidu.hugegraph.driver.HugeClient;
import com.baidu.hugegraph.exception.NotSupportException;
import com.baidu.hugegraph.structure.constant.WriteType;
import com.baidu.hugegraph.structure.schema.PropertyKey;
import com.baidu.hugegraph.util.Log;

public class HugeGraphOutput<T> extends AbstractComputerOutput {

    private static final Logger LOG = Log.logger(HugeGraphOutput.class);

    private TaskManager taskManager;
    private List<com.baidu.hugegraph.structure.graph.Vertex> localVertices;
    private int batchSize;
    private Value result;
    private WriteType defaultWriteType;

    @Override
    public void init(Config config, int partition) {
        super.init(config, partition);

        this.taskManager = new TaskManager(config);
        this.batchSize = config.get(ComputerOptions.OUTPUT_BATCH_SIZE);
        this.localVertices = new ArrayList<>(this.batchSize);
        this.result = config.createObject(
                      ComputerOptions.ALGORITHM_RESULT_CLASS);
        this.defaultWriteType = WriteType.valueOf(
             config.get(ComputerOptions.OUTPUT_DEFAULT_RESULT_WRITE_TYPE));
        this.prepareSchema();
    }

    public HugeClient client() {
        return this.taskManager.client();
    }

    @Override
    public void write(Vertex vertex) {
        this.localVertices.add(this.constructHugeVertex(vertex));
        if (this.localVertices.size() >= this.batchSize) {
            this.commit();
        }
    }

    @Override
    public void close() {
        if (!this.localVertices.isEmpty()) {
            this.commit();
        }
        this.taskManager.waitFinished();
        this.taskManager.shutdown();
        LOG.info("End write back partition {}", this.partition());
    }

    protected void commit() {
        this.taskManager.submitBatch(this.localVertices);
        LOG.info("Write back {} vertices", this.localVertices.size());

        this.localVertices = new ArrayList<>(this.batchSize);
    }

    protected com.baidu.hugegraph.structure.graph.Vertex constructHugeVertex(
                                                         Vertex vertex) {
        com.baidu.hugegraph.structure.graph.Vertex hugeVertex =
                new com.baidu.hugegraph.structure.graph.Vertex(null);
        hugeVertex.id(vertex.id().asObject());
        hugeVertex.property(this.name(), this.value(vertex));
        return hugeVertex;
    }

    @SuppressWarnings("unchecked")
    protected T value(Vertex vertex) {
        if (vertex.value().valueType() == ValueType.ID) {
            return (T) vertex.value().string();
        }
        return (T) vertex.value().value();
    }

    protected void prepareSchema() {
        PropertyKey.Builder builder = this.client().schema()
                                          .propertyKey(this.name());
        ValueType valueType = this.result.valueType();
        switch (valueType) {
            case INT:
                builder.asInt();
                break;
            case DOUBLE:
                builder.asDouble();
                break;
            case FLOAT:
                builder.asFloat();
                break;
            case STRING:
            case ID:
                builder.asText();
                break;
            case CUSTOMIZE_VALUE:
                Class<T> tClass = this.getTClass(this.result.getClass());
                if (Integer.class.isAssignableFrom(tClass)) {
                    builder.asInt();
                    break;
                } else if (Double.class.isAssignableFrom(tClass)) {
                    builder.asDouble();
                    break;
                } else if (Float.class.isAssignableFrom(tClass)) {
                    builder.asFloat();
                    break;
                } else if (String.class.isAssignableFrom(tClass)) {
                    builder.asText();
                    break;
                }
            default:
                throw new NotSupportException("Not Support result type: %s, " +
                                              "please implement customize " +
                                              "hugegraph output class",
                                              valueType);
        }
        builder.writeType(this.defaultWriteType);
        builder.ifNotExist().create();
    }

    private Class<T> getTClass(Class<? extends Value> clz) {
        Type type = clz.getGenericSuperclass();
        ParameterizedType parameterizedType = (ParameterizedType) type;
        Type[] types = parameterizedType.getActualTypeArguments();
        @SuppressWarnings("unchecked")
        Class<T> crClass = (Class<T>) types[0];
        return crClass;
    }
}
