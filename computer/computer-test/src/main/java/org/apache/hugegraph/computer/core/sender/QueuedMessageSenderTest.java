/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.computer.core.sender;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.common.exception.TransportException;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.network.ConnectionId;
import org.apache.hugegraph.computer.core.network.TransportClient;
import org.apache.hugegraph.computer.core.network.message.MessageType;
import org.apache.hugegraph.computer.core.worker.MockComputation2;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class QueuedMessageSenderTest extends UnitTestBase {

    private Config config;

    @Before
    public void setup() {
        this.config = UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.JOB_ID, "local_002",
                ComputerOptions.JOB_WORKERS_COUNT, "2",
                ComputerOptions.JOB_PARTITIONS_COUNT, "2",
                ComputerOptions.TRANSPORT_SERVER_PORT, "8086",
                ComputerOptions.BSP_REGISTER_TIMEOUT, "30000",
                ComputerOptions.BSP_LOG_INTERVAL, "10000",
                ComputerOptions.BSP_MAX_SUPER_STEP, "2",
                ComputerOptions.WORKER_COMPUTATION_CLASS,
                MockComputation2.class.getName()
        );
    }

    private QueuedMessageSender newSender(TransportClient first, TransportClient second) {
        QueuedMessageSender sender = new QueuedMessageSender(this.config);
        sender.addWorkerClient(1, first);
        sender.addWorkerClient(2, second);
        sender.init();
        return sender;
    }

    @Test
    public void testInitAndClose() {
        QueuedMessageSender sender = this.newSender(new MockTransportClient(),
                                                    new MockTransportClient());
        Thread sendExecutor = Whitebox.getInternalState(sender,
                                                        "sendExecutor");
        try {
            Assert.assertTrue(ImmutableSet.of(Thread.State.NEW,
                                              Thread.State.RUNNABLE,
                                              Thread.State.WAITING)
                                          .contains(sendExecutor.getState()));
        } finally {
            sender.close();
        }
        Assert.assertEquals(Thread.State.TERMINATED, sendExecutor.getState());
    }

    @Test
    public void testFatalExecutorCompletesQueuedFinish() throws Exception {
        ControlFutureClient client = new ControlFutureClient();
        ControlFutureClient second = new ControlFutureClient(2);
        client.dataSendBusy = true;
        second.dataSendBusy = true;
        QueuedMessageSender sender = this.newSender(client, second);
        try {
            sender.send(1, new QueuedMessage(0, MessageType.MSG,
                                             ByteBuffer.allocate(1)));
            sender.send(2, new QueuedMessage(0, MessageType.MSG,
                                             ByteBuffer.allocate(1)));
            Assert.assertTrue(await(client.dataSendCalled));
            Assert.assertTrue(await(second.dataSendCalled));
            CompletableFuture<Void> finish = sender.send(1, MessageType.FINISH);
            Thread executor = Whitebox.getInternalState(sender, "sendExecutor");
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
            while (executor.getState() != Thread.State.WAITING &&
                   System.nanoTime() < deadline) {
                Thread.sleep(10L);
            }
            Assert.assertEquals(Thread.State.WAITING, executor.getState());
            executor.interrupt();
            executor.join(TimeUnit.SECONDS.toMillis(1));
            Assert.assertFalse(executor.isAlive());
            AtomicReference<Throwable> fatal =
                    Whitebox.getInternalState(sender, "fatalError");
            Assert.assertThrows(ComputerException.class, sender::checkFatal);
            assertFutureFailedWith(finish, fatal.get());
        } finally {
            sender.close();
        }
    }

    @Test
    public void testInterruptedIdleExecutorFailsNextFinishFast() throws Exception {
        ControlFutureClient client = new ControlFutureClient();
        QueuedMessageSender sender = this.newSender(client, new MockTransportClient());
        try {
            Thread executor = Whitebox.getInternalState(sender, "sendExecutor");
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
            while (executor.getState() != Thread.State.WAITING &&
                   System.nanoTime() < deadline) {
                Thread.sleep(10L);
            }
            Assert.assertEquals(Thread.State.WAITING, executor.getState());
            executor.interrupt();
            executor.join(TimeUnit.SECONDS.toMillis(1));
            Assert.assertFalse(executor.isAlive());
            Assert.assertThrows(ComputerException.class, sender::checkFatal);
            /*
             * The next FINISH must fail fast with the recorded fatal error
             * instead of waiting out the control timeout on the dead executor.
             */
            Assert.assertThrows(ComputerException.class,
                                () -> sender.send(1, MessageType.FINISH));
        } finally {
            sender.close();
        }
    }

    @Test
    public void testCloseCompletesQueuedFinish() throws Exception {
        ControlFutureClient client = new ControlFutureClient();
        client.dataSendBusy = true;
        QueuedMessageSender sender = this.newSender(client, new MockTransportClient());
        try {
            sender.send(1, new QueuedMessage(0, MessageType.MSG,
                                             ByteBuffer.allocate(1)));
            Assert.assertTrue(await(client.dataSendCalled));
            CompletableFuture<Void> finish = sender.send(1, MessageType.FINISH);
            sender.close();
            assertFutureFailedWithMessage(finish, "closed");
        } finally {
            sender.close();
        }
    }

    @Test
    public void testRejectsMessageTypeFromWrongOverload() {
        QueuedMessageSender sender = new QueuedMessageSender(this.config);
        sender.addWorkerClient(1, new ControlFutureClient());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            sender.send(1, MessageType.MSG);
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            sender.send(1, MessageType.PING);
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            sender.send(1, new QueuedMessage(-1, MessageType.START, null));
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            sender.send(1, new QueuedMessage(-1, MessageType.FINISH, null));
        });
    }

    @Test
    public void testControlBeforeCompletionFinishes() throws Exception {
        ControlFutureClient client = new ControlFutureClient();
        QueuedMessageSender sender = this.newSender(client, new MockTransportClient());

        CountDownLatch completionStarted = new CountDownLatch(1);
        CountDownLatch allowCompletion = new CountDownLatch(1);
        Thread completionThread = null;
        try {
            CompletableFuture<Void> startFuture = sender.send(1, MessageType.START);
            Assert.assertTrue(await(client.startCalled));
            startFuture.whenComplete((r, e) -> {
                completionStarted.countDown();
                try {
                    allowCompletion.await();
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(exception);
                }
            });

            completionThread = new Thread(
                               () -> client.startFuture.complete(null));
            completionThread.start();
            Assert.assertTrue(completionStarted.await(1, TimeUnit.SECONDS));

            CompletableFuture<Void> finishFuture = sender.send(1, MessageType.FINISH);
            Assert.assertTrue(await(client.finishCalled));
            allowCompletion.countDown();
            completionThread.join(TimeUnit.SECONDS.toMillis(1));
            Assert.assertFalse(completionThread.isAlive());
            client.finishFuture.complete(null);
            finishFuture.get(1, TimeUnit.SECONDS);
        } finally {
            allowCompletion.countDown();
            if (completionThread != null) {
                completionThread.join(TimeUnit.SECONDS.toMillis(1));
            }
            sender.close();
        }
    }

    @Test
    public void testTransportExceptionControlFuture() throws Exception {
        ControlFutureClient client = new ControlFutureClient();
        QueuedMessageSender sender = this.newSender(client, new MockTransportClient());

        try {
            CompletableFuture<Void> startFuture = sender.send(1, MessageType.START);
            Assert.assertTrue(await(client.startCalled));

            TransportException cause = new TransportException("connection failed");
            sender.transportExceptionCaught(cause, client.connectionId());
            assertFutureFailedWith(startFuture, cause);

            CompletableFuture<Void> finishFuture = sender.send(1, MessageType.FINISH);
            Assert.assertTrue(await(client.finishCalled));
            client.startFuture.complete(null);
            assertFutureFailedWith(startFuture, cause);
            Assert.assertFalse(finishFuture.isDone());

            client.finishFuture.complete(null);
            finishFuture.get(1, TimeUnit.SECONDS);
        } finally {
            sender.close();
        }
    }

    @Test
    public void testExceptionalCompletionCasLossFailsNextControl()
            throws Exception {
        ControlFutureClient client = new ControlFutureClient();
        QueuedMessageSender sender = this.newSender(
                                     client, new MockTransportClient());
        CountDownLatch failureObserved = new CountDownLatch(1);
        CountDownLatch resumeFailure = new CountDownLatch(1);
        Thread failureThread = null;

        try {
            CompletableFuture<Void> startFuture = sender.send(
                    1, MessageType.START);
            Assert.assertTrue(await(client.startCalled));

            Object[] channels = Whitebox.getInternalState(sender, "channels");
            Object channel = channels[0];
            AtomicReference<CompletableFuture<Void>> controlFutureRef =
                    Whitebox.getInternalState(channel, "controlFutureRef");
            AtomicReference<CompletableFuture<Void>> observedFuture =
                    new AtomicReference<>();
            TransportException cause =
                    new TransportException("connection failed");
            failureThread = new Thread(() -> {
                observedFuture.set(controlFutureRef.get());
                failureObserved.countDown();
                try {
                    resumeFailure.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
                Whitebox.invoke(channel.getClass(), new Class<?>[] {
                                        CompletableFuture.class,
                                        Throwable.class},
                                "completeControlFuture", channel,
                                observedFuture.get(), cause);
            });
            failureThread.start();
            Assert.assertTrue(await(failureObserved));

            client.startFuture.complete(null);
            startFuture.get(1, TimeUnit.SECONDS);
            CompletableFuture<Void> finishFuture = sender.send(
                    1, MessageType.FINISH);
            Assert.assertTrue(await(client.finishCalled));

            resumeFailure.countDown();
            assertFutureFailedWith(finishFuture, cause);
            client.finishFuture.complete(null);
        } finally {
            resumeFailure.countDown();
            if (failureThread != null) {
                failureThread.join(TimeUnit.SECONDS.toMillis(1L));
            }
            sender.close();
        }
    }

    @Test
    public void testTransportExceptionDispatch() throws Exception {
        ControlFutureClient client = new ControlFutureClient();
        QueuedMessageSender sender = this.newSender(client, new MockTransportClient());

        client.blockDataSend = true;
        try {
            sender.send(1, new QueuedMessage(0, MessageType.MSG, ByteBuffer.allocate(1)));
            Assert.assertTrue(await(client.dataSendCalled));

            CompletableFuture<Void> startFuture = sender.send(1, MessageType.START);
            TransportException cause = new TransportException("connection failed before start");
            sender.transportExceptionCaught(cause, client.connectionId());
            assertFutureFailedWith(startFuture, cause);

            client.allowDataSend.countDown();
            Assert.assertFalse(await(client.startCalled));
        } finally {
            client.allowDataSend.countDown();
            sender.close();
        }
    }

    @Test
    public void testExecutorAlive() throws Exception {
        ControlFutureClient client = new ControlFutureClient();
        QueuedMessageSender sender = this.newSender(client, new MockTransportClient());

        try {
            RuntimeException startCause = new IllegalArgumentException("start session failed");
            client.startFailure = startCause;
            CompletableFuture<Void> startFuture = sender.send(1, MessageType.START);
            assertFutureFailedWith(startFuture, startCause);

            RuntimeException finishCause = new IllegalArgumentException("finish session failed");
            client.finishFailure = finishCause;
            CompletableFuture<Void> finishFuture = sender.send(1, MessageType.FINISH);
            assertFutureFailedWith(finishFuture, finishCause);

            Thread sendExecutor = Whitebox.getInternalState(sender, "sendExecutor");
            sendExecutor.join(TimeUnit.SECONDS.toMillis(1));
            Assert.assertTrue(sendExecutor.isAlive());

            client.startFailure = null;
            CompletableFuture<Void> nextStartFuture = sender.send(1, MessageType.START);
            Assert.assertTrue(await(client.startCalled));
            client.startFuture.complete(null);
            nextStartFuture.get(1, TimeUnit.SECONDS);
        } finally {
            sender.close();
        }
    }

    @Test
    public void testAsyncControlFutureFailures() throws Exception {
        ControlFutureClient client = new ControlFutureClient();
        QueuedMessageSender sender = this.newSender(client,
                                                    new MockTransportClient());

        try {
            CompletableFuture<Void> startFuture = sender.send(
                    1, MessageType.START);
            Assert.assertTrue(await(client.startCalled));
            TransportException startCause =
                    new TransportException("async start failed");
            client.startFuture.completeExceptionally(startCause);
            assertFutureFailedWith(startFuture, startCause);

            CompletableFuture<Void> finishFuture = sender.send(
                    1, MessageType.FINISH);
            Assert.assertTrue(await(client.finishCalled));
            TransportException finishCause =
                    new TransportException("async finish failed");
            client.finishFuture.completeExceptionally(finishCause);
            assertFutureFailedWith(finishFuture, finishCause);
        } finally {
            sender.close();
        }
    }

    @Test
    public void testOtherClients() throws Exception {
        ControlFutureClient failedClient = new ControlFutureClient(1);
        ControlFutureClient activeClient = new ControlFutureClient(2);
        QueuedMessageSender sender = this.newSender(failedClient, activeClient);

        try {
            Assert.assertFalse(failedClient.connectionId()
                                           .equals(activeClient.connectionId()));
            TransportException startCause =
                    new TransportException("start session failed");
            failedClient.startFailure = startCause;
            CompletableFuture<Void> failedStart = sender.send(
                    1, MessageType.START);
            assertFutureFailedWith(failedStart, startCause);

            CompletableFuture<Void> activeStart = sender.send(2, MessageType.START);
            Assert.assertTrue(await(activeClient.startCalled));
            activeClient.startFuture.complete(null);
            activeStart.get(1, TimeUnit.SECONDS);

            failedClient.startFailure = null;
            CompletableFuture<Void> callbackStart = sender.send(
                    1, MessageType.START);
            Assert.assertTrue(await(failedClient.startCalled));
            TransportException callbackCause =
                    new TransportException("connection failed");
            sender.transportExceptionCaught(callbackCause,
                                            failedClient.connectionId());
            assertFutureFailedWith(callbackStart, callbackCause);

            CompletableFuture<Void> activeStartAfterCallback = sender.send(
                    2, MessageType.START);
            activeStartAfterCallback.get(1, TimeUnit.SECONDS);

            TransportException finishCause =
                    new TransportException("finish session failed");
            failedClient.finishFailure = finishCause;
            CompletableFuture<Void> failedFinish = sender.send(
                    1, MessageType.FINISH);
            assertFutureFailedWith(failedFinish, finishCause);

            CompletableFuture<Void> activeFinish = sender.send(2, MessageType.FINISH);
            Assert.assertTrue(await(activeClient.finishCalled));
            activeClient.finishFuture.complete(null);
            activeFinish.get(1, TimeUnit.SECONDS);

            Thread sendExecutor = Whitebox.getInternalState(sender, "sendExecutor");
            Assert.assertTrue(sendExecutor.isAlive());
        } finally {
            sender.close();
        }
    }

    @Test
    public void testQueuedFinish() throws Exception {
        this.assertSynchronousDataFailureCompletesQueuedFinish(
                new TransportException("data send failed"));
    }

    @Test
    public void testCompletesQueuedFinish() throws Exception {
        this.assertSynchronousDataFailureCompletesQueuedFinish(
                new IllegalStateException("data send failed"));
    }

    @Test
    public void testFinishFailsFinish() throws Exception {
        this.assertSynchronousDataFailureBeforeFinishFailsFinish(
                new TransportException("data send failed before finish"));
    }

    @Test
    public void testDataRuntimeFinish() throws Exception {
        this.assertSynchronousDataFailureBeforeFinishFailsFinish(
                new IllegalStateException("data send failed before finish"));
    }

    @Test
    public void testConflictKeepsSendExecutorAlive() throws Exception {
        ControlFutureClient client = new ControlFutureClient();
        QueuedMessageSender sender = this.newSender(client, new MockTransportClient());

        try {
            CompletableFuture<Void> startFuture = sender.send(1, MessageType.START);
            Assert.assertTrue(await(client.startCalled));

            CompletableFuture<Void> conflictingFinishFuture = sender.send(1, MessageType.FINISH);
            assertFutureFailedWithMessage(conflictingFinishFuture, "The origin future must be null");

            Thread sendExecutor = Whitebox.getInternalState(sender, "sendExecutor");
            sendExecutor.join(TimeUnit.SECONDS.toMillis(1));
            Assert.assertTrue(sendExecutor.isAlive());

            client.startFuture.complete(null);
            startFuture.get(1, TimeUnit.SECONDS);

            CompletableFuture<Void> finishFuture = sender.send(1, MessageType.FINISH);
            Assert.assertTrue(await(client.finishCalled));
            client.finishFuture.complete(null);
            finishFuture.get(1, TimeUnit.SECONDS);
        } finally {
            sender.close();
        }
    }

    private static void assertFutureFailedWith(CompletableFuture<Void> future, Throwable cause)
            throws InterruptedException, TimeoutException {
        try {
            future.get(1, TimeUnit.SECONDS);
            Assert.fail("Expected control future to fail");
        } catch (ExecutionException exception) {
            Assert.assertSame(cause, exception.getCause());
        }
    }

    private static boolean await(CountDownLatch latch) throws InterruptedException {
        return latch.await(1, TimeUnit.SECONDS);
    }

    private void assertSynchronousDataFailureCompletesQueuedFinish(
            Throwable cause) throws Exception {
        ControlFutureClient failedClient = new ControlFutureClient();
        ControlFutureClient activeClient = new ControlFutureClient(2);
        QueuedMessageSender sender = this.newSender(failedClient, activeClient);

        failedClient.blockDataSend = true;
        try {
            sender.send(1, new QueuedMessage(0, MessageType.MSG,
                                              ByteBuffer.allocate(1)));
            Assert.assertTrue(await(failedClient.dataSendCalled));

            CompletableFuture<Void> finishFuture = sender.send(1, MessageType.FINISH);
            failedClient.dataFailure = cause;
            failedClient.allowDataSend.countDown();
            assertFutureFailedWith(finishFuture, cause);
            waitForQueueEmpty(sender, 1);
            Assert.assertEquals(1L, failedClient.finishCalled.getCount());

            CompletableFuture<Void> activeStart = sender.send(2, MessageType.START);
            Assert.assertTrue(await(activeClient.startCalled));
            activeClient.startFuture.complete(null);
            activeStart.get(1, TimeUnit.SECONDS);
        } finally {
            failedClient.allowDataSend.countDown();
            sender.close();
        }
    }

    @Test
    public void testTransportExceptionDuringDataSendFailsLaterFinish()
            throws Exception {
        ControlFutureClient client = new ControlFutureClient();
        QueuedMessageSender sender = this.newSender(
                                     client, new MockTransportClient());

        client.blockDataSend = true;
        try {
            sender.send(1, new QueuedMessage(0, MessageType.MSG,
                                             ByteBuffer.allocate(1)));
            Assert.assertTrue(await(client.dataSendCalled));

            TransportException cause =
                    new TransportException("connection failed during data");
            sender.transportExceptionCaught(cause, client.connectionId());
            client.allowDataSend.countDown();
            waitForQueueEmpty(sender, 1);

            CompletableFuture<Void> finishFuture = sender.send(
                    1, MessageType.FINISH);
            assertFutureFailedWith(finishFuture, cause);
            Assert.assertFalse(await(client.finishCalled));
        } finally {
            client.allowDataSend.countDown();
            sender.close();
        }
    }

    private void assertSynchronousDataFailureBeforeFinishFailsFinish(
            Throwable cause) throws Exception {
        ControlFutureClient failedClient = new ControlFutureClient();
        QueuedMessageSender sender = this.newSender(
                                     failedClient, new MockTransportClient());

        failedClient.dataFailure = cause;
        try {
            sender.send(1, new QueuedMessage(0, MessageType.MSG,
                                             ByteBuffer.allocate(1)));
            waitForQueueEmpty(sender, 1);

            CompletableFuture<Void> finishFuture = sender.send(1, MessageType.FINISH);
            assertFutureFailedWith(finishFuture, cause);
            Assert.assertFalse(await(failedClient.finishCalled));
        } finally {
            sender.close();
        }
    }

    private static void waitForQueueEmpty(QueuedMessageSender sender,
                                          int workerId)
            throws InterruptedException {
        Object[] channels = Whitebox.getInternalState(sender, "channels");
        MessageQueue queue = Whitebox.getInternalState(channels[workerId - 1],
                                                       "queue");
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1L);
        while (queue.peek() != null && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
        Assert.assertTrue("Timed out to wait for sender queue to be empty",
                          queue.peek() == null);
    }

    private static void assertFutureFailedWithMessage(CompletableFuture<Void> future,
                                                       String message)
            throws InterruptedException, TimeoutException {
        try {
            future.get(1, TimeUnit.SECONDS);
            Assert.fail("Expected control future to fail");
        } catch (ExecutionException exception) {
            Assert.assertContains(message, exception.getCause().getMessage());
        }
    }

    private static class ControlFutureClient extends MockTransportClient {

        private final ConnectionId connectionId;
        private final CountDownLatch startCalled = new CountDownLatch(1);
        private final CountDownLatch finishCalled = new CountDownLatch(1);
        private final CountDownLatch dataSendCalled = new CountDownLatch(1);
        private final CountDownLatch allowDataSend = new CountDownLatch(1);
        private final CompletableFuture<Void> startFuture = new CompletableFuture<>();
        private final CompletableFuture<Void> finishFuture = new CompletableFuture<>();
        private Throwable startFailure;
        private Throwable finishFailure;
        private Throwable dataFailure;
        private boolean blockDataSend;
        private boolean dataSendBusy;

        private ControlFutureClient() {
            this(1);
        }

        private ControlFutureClient(int clientIndex) {
            this.connectionId = new ConnectionId(
                                new InetSocketAddress("localhost", 8080),
                                clientIndex);
        }

        @Override
        public ConnectionId connectionId() {
            return this.connectionId;
        }

        @Override
        public CompletableFuture<Void> startSessionAsync() throws TransportException {
            throwFailure(this.startFailure);
            this.startCalled.countDown();
            return this.startFuture;
        }

        @Override
        public CompletableFuture<Void> finishSessionAsync() throws TransportException {
            throwFailure(this.finishFailure);
            this.finishCalled.countDown();
            return this.finishFuture;
        }

        @Override
        public boolean send(MessageType messageType, int partition,
                            ByteBuffer buffer) throws TransportException {
            if (this.dataSendBusy) {
                this.dataSendCalled.countDown();
                return false;
            }
            if (this.blockDataSend) {
                this.dataSendCalled.countDown();
                try {
                    this.allowDataSend.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new TransportException("Interrupted data send", e);
                }
            }
            throwFailure(this.dataFailure);
            return true;
        }

        private static void throwFailure(Throwable failure) throws TransportException {
            if (failure instanceof TransportException) {
                throw (TransportException) failure;
            }
            if (failure instanceof RuntimeException) {
                throw (RuntimeException) failure;
            }
        }

        @Override
        public boolean sessionActive() {
            return false;
        }

        @Override
        public InetSocketAddress remoteAddress() {
            return new InetSocketAddress("127.0.0.1", 8080);
        }

    }
}
