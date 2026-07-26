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

import org.apache.hugegraph.computer.core.common.exception.TransportException;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
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

    @Test
    public void testInitAndClose() {
        QueuedMessageSender sender = new QueuedMessageSender(this.config);
        sender.addWorkerClient(1, new MockTransportClient());
        sender.addWorkerClient(2, new MockTransportClient());
        sender.init();

        Thread sendExecutor = Whitebox.getInternalState(sender, "sendExecutor");
        Assert.assertTrue(ImmutableSet.of(Thread.State.NEW,
                                          Thread.State.RUNNABLE,
                                          Thread.State.WAITING)
                                      .contains(sendExecutor.getState()));

        sender.close();
        Assert.assertTrue(ImmutableSet.of(Thread.State.TERMINATED)
                                      .contains(sendExecutor.getState()));
    }

    @Test
    public void testControlFutureCanQueueNextControlBeforeCompletionDependentFinishes()
            throws Exception {
        QueuedMessageSender sender = new QueuedMessageSender(this.config);
        ControlFutureClient client = new ControlFutureClient();
        sender.addWorkerClient(1, client);
        sender.addWorkerClient(2, new MockTransportClient());
        sender.init();

        CountDownLatch completionStarted = new CountDownLatch(1);
        CountDownLatch allowCompletion = new CountDownLatch(1);
        Thread completionThread = null;
        try {
            CompletableFuture<Void> startFuture = sender.send(1,
                                                               MessageType.START);
            Assert.assertTrue(client.awaitStart());
            startFuture.whenComplete((r, e) -> {
                completionStarted.countDown();
                try {
                    allowCompletion.await();
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(exception);
                }
            });

            completionThread = new Thread(client::completeStart);
            completionThread.start();
            Assert.assertTrue(completionStarted.await(1, TimeUnit.SECONDS));

            CompletableFuture<Void> finishFuture = sender.send(1,
                                                                MessageType.FINISH);
            Assert.assertTrue(client.awaitFinish());
            allowCompletion.countDown();
            completionThread.join(TimeUnit.SECONDS.toMillis(1));
            Assert.assertFalse(completionThread.isAlive());
            client.completeFinish();
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
    public void testTransportExceptionCompletesInFlightControlFuture()
            throws Exception {
        QueuedMessageSender sender = new QueuedMessageSender(this.config);
        ControlFutureClient client = new ControlFutureClient();
        sender.addWorkerClient(1, client);
        sender.addWorkerClient(2, new MockTransportClient());
        sender.init();

        try {
            CompletableFuture<Void> startFuture = sender.send(1,
                                                               MessageType.START);
            Assert.assertTrue(client.awaitStart());

            TransportException cause =
                    new TransportException("connection failed");
            sender.transportExceptionCaught(cause, client.connectionId());
            assertFutureFailedWith(startFuture, cause);

            CompletableFuture<Void> finishFuture = sender.send(1,
                                                                MessageType.FINISH);
            Assert.assertTrue(client.awaitFinish());
            client.completeStart();
            assertFutureFailedWith(startFuture, cause);

            client.completeFinish();
            finishFuture.get(1, TimeUnit.SECONDS);
        } finally {
            sender.close();
        }
    }

    @Test
    public void testSynchronousControlFailureCompletesFutureAndKeepsExecutorAlive()
            throws Exception {
        QueuedMessageSender sender = new QueuedMessageSender(this.config);
        ControlFutureClient client = new ControlFutureClient();
        sender.addWorkerClient(1, client);
        sender.addWorkerClient(2, new MockTransportClient());
        sender.init();

        try {
            RuntimeException startCause =
                    new IllegalArgumentException("start session failed");
            client.failStartWith(startCause);
            CompletableFuture<Void> startFuture = sender.send(1,
                                                               MessageType.START);
            assertFutureFailedWith(startFuture, startCause);

            RuntimeException finishCause =
                    new IllegalArgumentException("finish session failed");
            client.failFinishWith(finishCause);
            CompletableFuture<Void> finishFuture = sender.send(1,
                                                                MessageType.FINISH);
            assertFutureFailedWith(finishFuture, finishCause);

            Thread sendExecutor = Whitebox.getInternalState(sender,
                                                             "sendExecutor");
            sendExecutor.join(TimeUnit.SECONDS.toMillis(1));
            Assert.assertTrue(sendExecutor.isAlive());

            client.failStartWith(null);
            CompletableFuture<Void> nextStartFuture = sender.send(1,
                                                                   MessageType.START);
            Assert.assertTrue(client.awaitStart());
            client.completeStart();
            nextStartFuture.get(1, TimeUnit.SECONDS);
        } finally {
            sender.close();
        }
    }

    @Test
    public void testControlFutureConflictKeepsSendExecutorAlive()
            throws Exception {
        QueuedMessageSender sender = new QueuedMessageSender(this.config);
        ControlFutureClient client = new ControlFutureClient();
        sender.addWorkerClient(1, client);
        sender.addWorkerClient(2, new MockTransportClient());
        sender.init();

        try {
            CompletableFuture<Void> startFuture = sender.send(1,
                                                               MessageType.START);
            Assert.assertTrue(client.awaitStart());

            CompletableFuture<Void> conflictingFinishFuture = sender.send(
                    1, MessageType.FINISH);
            assertFutureFailedWithMessage(conflictingFinishFuture,
                                          "The origin future must be null");

            Thread sendExecutor = Whitebox.getInternalState(sender,
                                                             "sendExecutor");
            sendExecutor.join(TimeUnit.SECONDS.toMillis(1));
            Assert.assertTrue(sendExecutor.isAlive());

            client.completeStart();
            startFuture.get(1, TimeUnit.SECONDS);

            CompletableFuture<Void> finishFuture = sender.send(1,
                                                                 MessageType.FINISH);
            Assert.assertTrue(client.awaitFinish());
            client.completeFinish();
            finishFuture.get(1, TimeUnit.SECONDS);
        } finally {
            sender.close();
        }
    }

    private static void assertFutureFailedWith(CompletableFuture<Void> future,
                                                Throwable cause)
            throws InterruptedException, TimeoutException {
        try {
            future.get(1, TimeUnit.SECONDS);
            Assert.fail("Expected control future to fail");
        } catch (ExecutionException exception) {
            Assert.assertSame(cause, exception.getCause());
        }
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

        private final CountDownLatch startCalled;
        private final CountDownLatch finishCalled;
        private final CompletableFuture<Void> startFuture;
        private final CompletableFuture<Void> finishFuture;
        private RuntimeException startFailure;
        private RuntimeException finishFailure;

        public ControlFutureClient() {
            this.startCalled = new CountDownLatch(1);
            this.finishCalled = new CountDownLatch(1);
            this.startFuture = new CompletableFuture<>();
            this.finishFuture = new CompletableFuture<>();
        }

        @Override
        public CompletableFuture<Void> startSessionAsync() {
            if (this.startFailure != null) {
                throw this.startFailure;
            }
            this.startCalled.countDown();
            return this.startFuture;
        }

        @Override
        public CompletableFuture<Void> finishSessionAsync() {
            if (this.finishFailure != null) {
                throw this.finishFailure;
            }
            this.finishCalled.countDown();
            return this.finishFuture;
        }

        @Override
        public boolean send(MessageType messageType, int partition,
                            ByteBuffer buffer) {
            return true;
        }

        @Override
        public boolean sessionActive() {
            return false;
        }

        @Override
        public InetSocketAddress remoteAddress() {
            return new InetSocketAddress("127.0.0.1", 8080);
        }

        public boolean awaitStart() throws InterruptedException {
            return this.startCalled.await(1, TimeUnit.SECONDS);
        }

        public boolean awaitFinish() throws InterruptedException {
            return this.finishCalled.await(1, TimeUnit.SECONDS);
        }

        public void completeStart() {
            this.startFuture.complete(null);
        }

        public void completeFinish() {
            this.finishFuture.complete(null);
        }

        public void failStartWith(RuntimeException failure) {
            this.startFailure = failure;
        }

        public void failFinishWith(RuntimeException failure) {
            this.finishFailure = failure;
        }
    }
}
