package org.apache.hugegraph.computer.algorithm.sampling;

import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.graph.edge.Edge;
import org.apache.hugegraph.computer.core.graph.edge.Edges;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.graph.value.IdList;
import org.apache.hugegraph.computer.core.graph.value.IdListList;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.computer.core.worker.Computation;
import org.apache.hugegraph.computer.core.worker.ComputationContext;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import java.util.Iterator;
import java.util.Random;

/**
 * @author: diaohancai
 * @date: 2023-10-19
 */
public class RandomWalk implements Computation<RandomWalkMessage> {

    private static final Logger LOG = Log.logger(RandomWalk.class);

    public static final String OPTION_WALK_PER_NODE = "randomwalk.walk_per_node";
    public static final String OPTION_WALK_LENGTH = "randomwalk.walk_length";

    /**
     * number of walk to the same vertex(source vertex)
     */
    private Integer walkPerNode;

    /**
     * walk length
     */
    private Integer walkLength;

    /**
     * random
     */
    private Random random;

    @Override
    public String category() {
        return "sampling";
    }

    @Override
    public String name() {
        return "randomWalk";
    }

    @Override
    public void init(Config config) {
        this.walkPerNode = config.getInt(OPTION_WALK_PER_NODE, 3);
        if (this.walkPerNode < 1) {
            throw new ComputerException("The param %s must be greater than 1, " +
                    "actual got '%s'",
                    OPTION_WALK_PER_NODE, this.walkPerNode);
        }

        this.walkLength = config.getInt(OPTION_WALK_LENGTH, 3);
        if (this.walkLength < 1) {
            throw new ComputerException("The param %s must be greater than 1, " +
                    "actual got '%s'",
                    OPTION_WALK_LENGTH, this.walkLength);
        }

        this.random = new Random();
    }

    @Override
    public void compute0(ComputationContext context, Vertex vertex) {
        vertex.value(new IdListList());

        RandomWalkMessage message = new RandomWalkMessage();
        message.addPath(vertex);

        if (vertex.numEdges() <= 0) {
            // isolated vertex
            vertex.value(message.path());
            vertex.inactivate();
            return;
        }

        for (int i = 0; i < walkPerNode; ++i) {
            // random select one edge and walk
            Edge selectedEdge = this.randomSelectEdge(vertex.edges());
            context.sendMessage(selectedEdge.targetId(), message);
        }
    }

    @Override
    public void compute(ComputationContext context, Vertex vertex, Iterator<RandomWalkMessage> messages) {
        while (messages.hasNext()) {
            RandomWalkMessage message = messages.next();

            if (message.getIsFinish()) {
                // save result
                this.savePath(vertex, message.path());

                vertex.inactivate();
                continue;
            }

            message.addPath(vertex);

            if (vertex.numEdges() <= 0) {
                // there is nowhere to walk，finish eariler
                message.setIsFinish(true);
                context.sendMessage(this.getSourceId(message.path()), message);

                vertex.inactivate();
                continue;
            }

            if (message.path().size() >= this.walkLength + 1) {
                message.setIsFinish(true);
                Id sourceId = this.getSourceId(message.path());

                if (vertex.id().equals(sourceId)) {
                    // current vertex is the source vertex，no need to send message once more
                    // save result
                    this.savePath(vertex, message.path());
                } else {
                    context.sendMessage(sourceId, message);
                }

                vertex.inactivate();
                continue;
            }

            // random select one edge and walk
            Edge selectedEdge = this.randomSelectEdge(vertex.edges());
            context.sendMessage(selectedEdge.targetId(), message);
        }
    }

    /**
     * random select one edge
     */
    private Edge randomSelectEdge(Edges edges) {
        Edge selectedEdge = null;
        int randomNum = random.nextInt(edges.size());

        int i = 0;
        Iterator<Edge> iterator = edges.iterator();
        while (iterator.hasNext()) {
            selectedEdge = iterator.next();
            if (i == randomNum) {
                break;
            }
            i++;
        }

        return selectedEdge;
    }

    /**
     * get source id of path
     */
    private Id getSourceId(IdList path) {
        // the first id of path is the source id
        return path.get(0);
    }

    /**
     * save path
     */
    private void savePath(Vertex sourceVertex, IdList path) {
        IdListList curValue = sourceVertex.value();
        curValue.add(path.copy());
    }

}
