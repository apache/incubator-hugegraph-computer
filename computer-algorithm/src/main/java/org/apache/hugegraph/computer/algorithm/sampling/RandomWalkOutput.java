package org.apache.hugegraph.computer.algorithm.sampling;

import org.apache.hugegraph.computer.core.graph.value.IdListList;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.computer.core.output.hg.HugeGraphOutput;

import java.util.ArrayList;
import java.util.List;

/**
 * @author: diaohancai
 * @date: 2023-10-20
 */
public class RandomWalkOutput extends HugeGraphOutput<List<String>> {

    @Override
    protected void prepareSchema() {
        this.client().schema().propertyKey(this.name())
                .asText()
                .writeType(this.writeType())
                .valueList()
                .ifNotExist()
                .create();
    }

    @Override
    protected List<String> value(Vertex vertex) {
        IdListList value = vertex.value();
        List<String> propValues = new ArrayList<>();
        for (int i = 0; i < value.size(); i++) {
            propValues.add(value.get(i).toString());
        }
        return propValues;
    }

}
