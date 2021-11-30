package com.baidu.hugegraph.computer.algorithm.path.sssp;

import com.baidu.hugegraph.computer.core.graph.value.DoubleValue;
import com.baidu.hugegraph.computer.algorithm.AlgorithmTestBase;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import java.util.Map;
import org.junit.Test;

public class SsspTest extends AlgorithmTestBase {
    @Test
    public void test() throws InterruptedException {
        String analyze = "{" +
                         "    \"target_vertexes\": [" +
                         "        \"A\"" +
                         "    ]," +
                         "    \"label_as_weight\":" +
                         "    \"weight\"" + 
                         "}";
        runAlgorithm(SsspTestParams.class.getName(),
                     Sssp.OPTION_ANALYZE_CONFIG, analyze);
    }

    public static class SsspTestParams extends SsspParams {

        @Override
        public void setAlgorithmParameters(Map<String, String> params) {
            this.setIfAbsent(params, ComputerOptions.OUTPUT_CLASS,
                             SsspTestOutput.class.getName());
            super.setAlgorithmParameters(params);
        }
    }

   public static class SsspTestOutput extends SsspOutput {

        @Override
        public void write(
               com.baidu.hugegraph.computer.core.graph.vertex.Vertex vertex) {
            DoubleValue value = vertex.value();
            System.out.printf("%s %f\n", vertex.id(),
                   value.value().doubleValue());
            super.write(vertex);
        }
    }
}
