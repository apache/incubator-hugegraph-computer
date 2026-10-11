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

import java.util.ArrayList;
import java.util.List;

import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.input.VertexFetcher;
import org.apache.hugegraph.loader.builder.ElementBuilder;
import org.apache.hugegraph.loader.builder.VertexBuilder;
import org.apache.hugegraph.loader.executor.LoadContext;
import org.apache.hugegraph.loader.mapping.InputStruct;
import org.apache.hugegraph.loader.mapping.VertexMapping;
import org.apache.hugegraph.loader.reader.line.Line;
import org.apache.hugegraph.structure.graph.Vertex;
import org.apache.hugegraph.util.E;

public class FileVertxFetcher extends FileElementFetcher<Vertex>
                              implements VertexFetcher {

    public FileVertxFetcher(Config config) {
        super(config);
    }

    @Override
    protected List<Vertex> buildElement(Line line, ElementBuilder<Vertex> builder) {
        List<Vertex> vertices = super.buildElement(line, builder);
        if (vertices.stream().noneMatch(vertex -> vertex.id() == null)) {
            return vertices;
        }
        // Loader leaves primary-key IDs for online insertion; offline input needs both IDs and properties.
        E.checkState(!this.context().options().usePrefilter,
                     "Can't restore offline vertex IDs with prefiltering enabled");
        List<Vertex> identities = ((VertexBuilder) builder).buildIdentity(line.names(), line.values());
        E.checkState(vertices.size() == identities.size(),
                     "Offline vertex and identity counts differ");
        for (int i = 0; i < vertices.size(); i++) {
            Vertex vertex = vertices.get(i);
            Vertex identity = identities.get(i);
            E.checkState(vertex.label().equals(identity.label()) && identity.id() != null,
                         "Invalid offline vertex identity for label '%s'", vertex.label());
            if (vertex.id() == null) {
                vertex.id(identity.id());
            }
        }
        return vertices;
    }

    @Override
    protected List<ElementBuilder<Vertex>> elementBuilders(LoadContext context,
                                                           InputStruct struct) {
        List<ElementBuilder<Vertex>> builders = new ArrayList<>();
        for (VertexMapping mapping : struct.vertices()) {
            if (mapping.skip()) {
                continue;
            }
            builders.add(new VertexBuilder(context, struct, mapping));
        }
        return builders;
    }

    @Override
    public void close() {
        super.close();
    }
}
