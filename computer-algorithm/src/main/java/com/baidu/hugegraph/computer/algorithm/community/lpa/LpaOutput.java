package com.baidu.hugegraph.computer.algorithm.community.lpa;

import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.output.hg.HugeOutput;
import com.baidu.hugegraph.structure.constant.WriteType;

public class LpaOutput extends HugeOutput {

    @Override
    public void prepareSchema() {
        this.client().schema().propertyKey(this.name())
                     .asText()
                     .writeType(WriteType.OLAP_COMMON)
                     .ifNotExist()
                     .create();
    }

    @Override
    public com.baidu.hugegraph.structure.graph.Vertex constructHugeVertex(
            Vertex vertex) {
        com.baidu.hugegraph.structure.graph.Vertex hugeVertex =
                new com.baidu.hugegraph.structure.graph.Vertex(null);
        hugeVertex.id(vertex.id().asObject());
        hugeVertex.property(this.name(), vertex.value().toString());
        return hugeVertex;
    }
}
