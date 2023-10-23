package org.apache.hugegraph.computer.algorithm.sampling;

import org.apache.hugegraph.computer.algorithm.AlgorithmTestBase;
import org.apache.hugegraph.computer.algorithm.sampling.RandomWalkParams;
import org.junit.Test;

/**
 * @author: diaohancai
 * @date: 2023-10-20
 */
public class RandomWalkTest extends AlgorithmTestBase {

    @Test
    public void testRunAlgorithm() throws InterruptedException {
        runAlgorithm(RandomWalkParams.class.getName());
    }

}
