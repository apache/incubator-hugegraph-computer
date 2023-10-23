package org.apache.hugegraph.computer.algorithm.sampling;

import org.apache.hugegraph.computer.algorithm.AlgorithmParams;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.graph.value.IdListList;

import java.util.Map;

/**
 * @author: diaohancai
 * @date: 2023-10-20
 */
public class RandomWalkParams implements AlgorithmParams {

    @Override
    public void setAlgorithmParameters(Map<String, String> params) {
        this.setIfAbsent(params, ComputerOptions.WORKER_COMPUTATION_CLASS,
                RandomWalk.class.getName());
        this.setIfAbsent(params, ComputerOptions.ALGORITHM_MESSAGE_CLASS,
                RandomWalkMessage.class.getName());
        this.setIfAbsent(params, ComputerOptions.ALGORITHM_RESULT_CLASS,
                IdListList.class.getName());
        this.setIfAbsent(params, ComputerOptions.OUTPUT_CLASS,
                RandomWalkOutput.class.getName());

        this.setIfAbsent(params, RandomWalk.OPTION_WALK_PER_NODE,
                "3");
        this.setIfAbsent(params, RandomWalk.OPTION_WALK_LENGTH,
                "3");
    }

}
