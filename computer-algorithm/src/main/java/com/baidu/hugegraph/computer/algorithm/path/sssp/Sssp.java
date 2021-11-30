package com.baidu.hugegraph.computer.algorithm.path.sssp;

import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.graph.value.DoubleValue;
import com.baidu.hugegraph.computer.core.combiner.Combiner;
import com.baidu.hugegraph.computer.core.worker.Computation;
import com.baidu.hugegraph.computer.core.worker.ComputationContext;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;


public class Sssp implements Computation<DoubleValue> {
    public static final String OPTION_SSSP_TARGET =
                               "pathfinding.targetstring";
    public static final String OPTION_ANALYZE_CONFIG = "sssp.analyze_config";
    private SsspDescribe describe;
    private String weightString;

    @Override
    public String name() {
        return "sssp";
    }

    @Override
    public String category() {
        return "path";
    }

    @Override
    public void init(Config config) {
        String targetString = config.getString(OPTION_SSSP_TARGET, "");
        String configstr = config.getString(OPTION_ANALYZE_CONFIG, "{}");
        this.describe = SsspDescribe.of(configstr);
        //if we have no json input, we set target as OPTION_PATHFINDING_TARGET
        if (this.describe.targetVertexes() == null) {
            List<String> targets = new ArrayList();
            targets.add(targetString);
            this.describe.setTargetVertexes(targets);
        }
        this.weightString = this.describe.labelAsWeight();
        if (this.weightString == null) {
            this.weightString = "weight";
        }
    }

    @Override
    public void close(Config config) {
        // pass
    }

    
    @Override
    public void compute0(ComputationContext context, Vertex vertex) {
        vertex.value(new DoubleValue(-1.0D));
        List<String> targets = this.describe.targetVertexes();
        boolean needSendMessage = false;
        for (String target : targets) {
            if (vertex.id().toString().equals(target)) {
                needSendMessage = true;
                break;
            }
        }

        if (!needSendMessage) {
            return;
        }

        if (vertex.edges().size() == 0) {
            vertex.inactivate();
            return;
        }

        // Init path
        boolean asTarget = this.describe.directionToTarget();
        DoubleValue currValue = new DoubleValue(0.0D);
        vertex.value(currValue);

        for (Edge edge : vertex.edges()) {
            boolean asInverse = 
                 (edge.properties().get("inv") == null) ? false : true;
            if (!(asTarget ^ asInverse)) {
                continue;
            }
            DoubleValue weight = edge.properties().get(this.weightString);
            if (weight == null) {
                weight = new DoubleValue(1.0D);
            }
            System.out.printf("%s %s \n", vertex.id(), edge.targetId());
            context.sendMessage(edge.targetId(), weight);
        }
        vertex.inactivate();
    }
    
    @Override
    public void compute(ComputationContext context, Vertex vertex,
    Iterator<DoubleValue> messages) {
        boolean asTarget = this.describe.directionToTarget();
        DoubleValue message = Combiner.combineAll(context.combiner(), messages);
        if (message == null) {
            return;
        }
        DoubleValue currValue = vertex.value();

        if (message.value() < currValue.value() || currValue.value() < 0.0) {
            vertex.value(message);
        }
        Id id = vertex.id();
        for (Edge edge :vertex.edges()) {
             boolean asInverse =
                      (edge.properties().get("inv") == null) ? false : true;
             if (!(asTarget ^ asInverse)) {
                 continue;
             }
             DoubleValue weight = edge.property(this.weightString);  
             if (weight == null) {
                 weight = new DoubleValue(1.0D);
             }
             currValue = vertex.value();  
             DoubleValue forwardvalue = new DoubleValue(weight.value() +
                                                   currValue.value()); 
             context.sendMessage(edge.targetId(), forwardvalue);
        }
    }
}
