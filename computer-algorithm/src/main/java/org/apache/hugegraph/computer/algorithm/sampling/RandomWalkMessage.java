package org.apache.hugegraph.computer.algorithm.sampling;

import org.apache.hugegraph.computer.core.graph.value.BooleanValue;
import org.apache.hugegraph.computer.core.graph.value.IdList;
import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.computer.core.io.RandomAccessInput;
import org.apache.hugegraph.computer.core.io.RandomAccessOutput;

import java.io.IOException;
import java.util.List;

/**
 * @author: diaohancai
 * @date: 2023-10-20
 */
public class RandomWalkMessage implements Value.CustomizeValue<List<Object>> {

    /**
     * random walk path
     */
    private final IdList path;

    /**
     * finish flag
     */
    private BooleanValue isFinish;

    public RandomWalkMessage() {
        this.path = new IdList();
        this.isFinish = new BooleanValue(false);
    }

    @Override
    public void read(RandomAccessInput in) throws IOException {
        this.path.read(in);
        this.isFinish.read(in);
    }

    @Override
    public void write(RandomAccessOutput out) throws IOException {
        this.path.write(out);
        this.isFinish.write(out);
    }

    @Override
    public List<Object> value() {
        return this.path.value();
    }

    public IdList path() {
        return this.path;
    }

    public void addPath(Vertex vertex) {
        this.path.add(vertex.id());
    }

    public Boolean getIsFinish() {
        return isFinish.value();
    }

    public void setIsFinish(Boolean isFinish) {
        this.isFinish = new BooleanValue(isFinish);
    }

}
