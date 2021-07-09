package com.baidu.hugegraph.computer.driver;

import org.junit.Assert;
import org.junit.Test;

import com.baidu.hugegraph.computer.driver.util.JsonUtil;

public class DriverTest {

    @Test
    public void jsonTest() {
        SuperstepStat superstepStat = new SuperstepStat();
        String json = JsonUtil.toJson(superstepStat);
        SuperstepStat superstepStat1 = JsonUtil.fromJson(json,
                                                         SuperstepStat.class);
        Assert.assertEquals(superstepStat, superstepStat1);
    }

    @Test
    public void testJobStatus() {
        Assert.assertFalse(JobStatus.finished(JobStatus.INITIALIZING));
        Assert.assertFalse(JobStatus.finished(JobStatus.RUNNING));
        Assert.assertTrue(JobStatus.finished(JobStatus.FAILED));
        Assert.assertTrue(JobStatus.finished(JobStatus.SUCCEEDED));
        Assert.assertTrue(JobStatus.finished(JobStatus.CANCELLED));
    }
}
