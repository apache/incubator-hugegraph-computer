package com.baidu.hugegraph.computer.driver;

import org.junit.Test;

import com.baidu.hugegraph.computer.driver.util.JsonUtil;
import com.baidu.hugegraph.testutil.Assert;

public class DriverTest {

    @Test
    public void jsonUtilTest() {
        SuperstepStat superstepStat = new SuperstepStat();
        String json = JsonUtil.toJson(superstepStat);
        SuperstepStat superstepStat1 = JsonUtil.fromJson(json,
                                                         SuperstepStat.class);
        Assert.assertEquals(superstepStat, superstepStat1);

        Assert.assertThrows(RuntimeException.class, () -> {
            JsonUtil.fromJson("123", SuperstepStat.class);
        });
    }

    @Test
    public void testJobStatus() {
        Assert.assertFalse(JobStatus.finished(JobStatus.INITIALIZING));
        Assert.assertFalse(JobStatus.finished(JobStatus.RUNNING));
        Assert.assertTrue(JobStatus.finished(JobStatus.FAILED));
        Assert.assertTrue(JobStatus.finished(JobStatus.SUCCEEDED));
        Assert.assertTrue(JobStatus.finished(JobStatus.CANCELLED));
    }

    @Test
    public void testJobStateAndSuperstepStat() {
        DefaultJobState jobState = new DefaultJobState();
        jobState.jobStatus(JobStatus.INITIALIZING);
        jobState.superstep(3);
        jobState.maxSuperstep(99);
        SuperstepStat superstepStat = new SuperstepStat();
        superstepStat.vertexCount(1);
        superstepStat.edgeCount(1);
        superstepStat.finishedVertexCount(1);
        superstepStat.messageCount(1);
        superstepStat.active(true);
        superstepStat.messageBytes(1);
        jobState.lastSuperstepStat(superstepStat);

        Assert.assertEquals(JobStatus.INITIALIZING, jobState.jobStatus());
        Assert.assertEquals(3, jobState.superstep());
        Assert.assertEquals(99, jobState.maxSuperstep());

        Assert.assertEquals(1, superstepStat.vertexCount());
        Assert.assertEquals(1, superstepStat.edgeCount());
        Assert.assertEquals(1, superstepStat.finishedVertexCount());
        Assert.assertEquals(1, superstepStat.messageCount());
        Assert.assertTrue(superstepStat.active());
        Assert.assertEquals(1, superstepStat.messageBytes());

        SuperstepStat superstepStat2 = new SuperstepStat();
        superstepStat2.vertexCount(1);
        superstepStat2.edgeCount(1);
        superstepStat2.finishedVertexCount(1);
        superstepStat2.messageCount(1);
        superstepStat2.active(true);
        superstepStat2.messageBytes(1);
        Assert.assertEquals(superstepStat, superstepStat2);
        Assert.assertEquals(superstepStat.hashCode(),
                            superstepStat2.hashCode());
    }
}
