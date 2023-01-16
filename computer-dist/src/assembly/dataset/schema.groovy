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
// Define schema
schema.propertyKey("id").asInt().ifNotExist().create();
schema.propertyKey("rate").asDouble().ifNotExist().create();
schema.propertyKey("title").asText().ifNotExist().create();
schema.propertyKey("tag").asText().ifNotExist().create();
schema.propertyKey("genres").asText().valueSet().ifNotExist().create();
schema.propertyKey("timestamp").asText().ifNotExist().create();

schema.vertexLabel("user")
        .properties("id")
        .primaryKeys("id")
        .ifNotExist()
        .create();
schema.vertexLabel("movie")
        .properties("id", "title", "genres")
        .primaryKeys("id")
        .ifNotExist()
        .create();

schema.edgeLabel("rating")
        .sourceLabel("user")
        .targetLabel("movie")
        .properties("rate", "timestamp")
        .ifNotExist()
        .create();
schema.edgeLabel("taged")
        .sourceLabel("user")
        .targetLabel("movie")
        .properties("tag", "timestamp")
        .ifNotExist()
        .create();
