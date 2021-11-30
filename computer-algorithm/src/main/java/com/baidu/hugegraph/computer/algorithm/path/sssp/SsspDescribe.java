package com.baidu.hugegraph.computer.algorithm.path.sssp;

import java.util.List;
import com.baidu.hugegraph.util.JsonUtil;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SsspDescribe {

    private List<String> targetVertexes;
    private boolean directionToTarget = true;
    private String labelAsWeight;

    @JsonCreator
    private SsspDescribe(@JsonProperty("target_vertexes")
                         List<String> targetVertexes,
                         @JsonProperty("direction_to_target")
                         boolean directionToTarget,
                         @JsonProperty("label_as_weight")
                         String labelAsWeight) {
        this.targetVertexes = targetVertexes;
        this.directionToTarget = directionToTarget;
        this.labelAsWeight = labelAsWeight; 
    }

    public static SsspDescribe of(String describe) {
        return JsonUtil.fromJson(describe, SsspDescribe.class);
    }

    public List<String> targetVertexes() {
        return this.targetVertexes;
    }

    public void setTargetVertexes(List<String> targetVertexes) {
        this.targetVertexes = targetVertexes;
    }

    public boolean directionToTarget() {
        return this.directionToTarget;
    }

    public String labelAsWeight() {
        return this.labelAsWeight;
    } 
}
