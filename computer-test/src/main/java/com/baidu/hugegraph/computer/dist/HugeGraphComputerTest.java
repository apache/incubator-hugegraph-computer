package com.baidu.hugegraph.computer.dist;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.util.Log;
import org.junit.Test;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.suite.unit.UnitTestBase;

public class HugeGraphComputerTest extends UnitTestBase {

    private static final Logger LOG = Log.logger(HugeGraphComputerTest.class);

    @Test
    public void testServiceWith1Worker() throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch countDownLatch = new CountDownLatch(2);
        Throwable[] exceptions = new Throwable[2];
        String masterConfPath = HugeGraphComputerTest.class.getResource(
                                "/computer-master.properties").getPath();
        String work1ConfPath = HugeGraphComputerTest.class.getResource(
                               "/computer-worker1.properties").getPath();
        pool.submit(() -> {
            try {
                Thread.sleep(2000L);
                String[] args = {work1ConfPath, "worker", "local"};
                HugeGraphComputer.main(args);
            } catch (Throwable e) {
                LOG.error("Failed to start worker", e);
                exceptions[0] = e;
            } finally {
                countDownLatch.countDown();
            }
        });

        pool.submit(() -> {
            try {
                String[] args = {masterConfPath, "master", "local"};
                HugeGraphComputer.main(args);
            } catch (Throwable e) {
                LOG.error("Failed to start master", e);
                exceptions[1] = e;
            } finally {
                countDownLatch.countDown();
            }
        });

        countDownLatch.await();
        pool.shutdownNow();

        Assert.assertFalse(Arrays.asList(exceptions).toString(),
                           this.existError(exceptions));
    }

    @Test
    public void testServiceWithError() {
        String work1ConfPath = HugeGraphComputerTest.class.getResource(
                               "/computer-worker1.properties").getPath();
        Assert.assertThrows(IllegalArgumentException.class,
                            () -> {
                                String[] args1 = {work1ConfPath, "worker111",
                                                  "local"};
                                HugeGraphComputer.main(args1);
                            });
    }

    @Test
    public void testPrintUncaughtException() throws InterruptedException {
        AtomicBoolean isRun = new AtomicBoolean(false);
        Thread.UncaughtExceptionHandler handler = (t, e) -> {
            isRun.compareAndSet(false, true);
        };
        Thread.setDefaultUncaughtExceptionHandler(handler);
        HugeGraphComputer.setUncaughtExceptionHandler();
        Thread t = new Thread(() -> {
            throw new RuntimeException();
        });
        t.start();
        t.join();
        Assert.assertTrue(isRun.get());
    }
}
