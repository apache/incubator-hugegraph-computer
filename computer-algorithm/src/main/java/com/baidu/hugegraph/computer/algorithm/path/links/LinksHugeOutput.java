package com.baidu.hugegraph.computer.algorithm.path.links;

import com.baidu.hugegraph.computer.core.output.hg.HugeOutput;
import com.baidu.hugegraph.structure.constant.WriteType;
import com.baidu.hugegraph.structure.graph.Vertex;

import java.util.ArrayList;
import java.util.List;

public class LinksHugeOutput extends HugeOutput {

    @Override
    public void prepareSchema() {
        this.client().schema().propertyKey(this.name())
                     .asText()
                     .writeType(WriteType.OLAP_COMMON)
                     .valueList()
                     .ifNotExist()
                     .create();
    }

    @Override
    public Vertex constructHugeVertex(
           com.baidu.hugegraph.computer.core.graph.vertex.Vertex vertex) {
        com.baidu.hugegraph.structure.graph.Vertex hugeVertex =
                new com.baidu.hugegraph.structure.graph.Vertex(null);
        hugeVertex.id(vertex.id().asObject());

        LinksValue value = vertex.value();
        List<String> propValue = new ArrayList<>();
        for (int i = 0; i < value.size(); i++) {
            propValue.add(value.values().get(i).toString());
        }

        hugeVertex.property(this.name(), propValue);
        return hugeVertex;
    }
}
