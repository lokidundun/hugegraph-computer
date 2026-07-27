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

package org.apache.hugegraph.computer.suite.integrate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.hugegraph.computer.algorithm.centrality.pagerank.PageRankParams;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.common.exception.TransportException;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.graph.value.DoubleValue;
import org.apache.hugegraph.computer.core.manager.Managers;
import org.apache.hugegraph.computer.core.master.MasterService;
import org.apache.hugegraph.computer.core.network.DataClientManager;
import org.apache.hugegraph.computer.core.network.connection.ConnectionManager;
import org.apache.hugegraph.computer.core.network.message.Message;
import org.apache.hugegraph.computer.core.network.netty.NettyTransportClient;
import org.apache.hugegraph.computer.core.network.session.ClientSession;
import org.apache.hugegraph.computer.core.util.ComputerContextUtil;
import org.apache.hugegraph.computer.core.worker.WorkerService;
import org.apache.hugegraph.config.RpcOptions;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.util.Log;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;

public class SenderIntegrateTest {

    public static final Logger LOG = Log.logger(SenderIntegrateTest.class);

    private static final Class<?> COMPUTATION = MockComputation.class;
    private static final long BSP_WAIT_TIMEOUT = TimeUnit.MINUTES.toMillis(5L);
    private static final long SERVICE_WAIT_TIMEOUT =
            BSP_WAIT_TIMEOUT + TimeUnit.SECONDS.toMillis(10L);

    @BeforeClass
    public static void init() {
        // pass
    }

    @AfterClass
    public static void clear() {
        // pass
    }

    @Test
    public void testWaitForServicesFailsFast() {
        CompletableFuture<Void> failedWorker = new CompletableFuture<>();
        CompletableFuture<Void> waitingMaster = new CompletableFuture<>();
        IllegalStateException cause = new IllegalStateException("worker failed");
        failedWorker.completeExceptionally(cause);

        try {
            waitForServices(Arrays.asList(failedWorker, waitingMaster));
            Assert.fail("Expected worker failure to stop service wait");
        } catch (ComputerException e) {
            Assert.assertSame(cause, e.getCause());
        }
    }

    @Test
    public void testCleanupFailureDoesNotHideWaitFailure() {
        ServiceLifecycle lifecycle = new ServiceLifecycle();
        CompletableFuture<Void> failedWorker = new CompletableFuture<>();
        IllegalStateException waitFailure =
                new IllegalStateException("worker failed");
        IllegalStateException cleanupFailure =
                new IllegalStateException("worker close failed");
        failedWorker.completeExceptionally(waitFailure);
        lifecycle.registerWorker(() -> {
            throw cleanupFailure;
        });

        try {
            waitForServicesAndClose(lifecycle, Arrays.asList(failedWorker),
                                    new ArrayList<>(), null);
            Assert.fail("Expected worker failure to be preserved");
        } catch (ComputerException e) {
            Assert.assertSame(waitFailure, e.getCause());
            Assert.assertEquals(1, e.getSuppressed().length);
            Assert.assertSame(cleanupFailure, e.getSuppressed()[0].getCause());
        }
    }

    @Test
    public void testCiTimeoutsAllowHeavyInputStep() {
        Assert.assertEquals(TimeUnit.MINUTES.toMillis(5L), BSP_WAIT_TIMEOUT);
        Assert.assertEquals(BSP_WAIT_TIMEOUT + TimeUnit.SECONDS.toMillis(10L),
                            SERVICE_WAIT_TIMEOUT);
    }

    @Test
    public void testServiceLifecycleClosesLateRegisteredService() {
        ServiceLifecycle lifecycle = new ServiceLifecycle();
        AtomicBoolean closed = new AtomicBoolean();

        lifecycle.closeAll();

        Assert.assertFalse(lifecycle.registerWorker(() -> closed.set(true)));
        Assert.assertTrue(closed.get());
    }

    @Test
    public void testServiceLifecycleClosesWorkersBeforeMasterAfterFailure() {
        ServiceLifecycle lifecycle = new ServiceLifecycle();
        AtomicBoolean activeWorkerClosed = new AtomicBoolean();
        AtomicBoolean masterClosed = new AtomicBoolean();
        RuntimeException workerFailure =
                new IllegalStateException("worker close failed");

        lifecycle.registerMaster(() -> {
            Assert.assertTrue(activeWorkerClosed.get());
            masterClosed.set(true);
        });
        lifecycle.registerWorker(() -> {
            throw workerFailure;
        });
        lifecycle.registerWorker(() -> activeWorkerClosed.set(true));

        Throwable failure = lifecycle.closeAll();

        Assert.assertSame(workerFailure, failure);
        Assert.assertTrue(activeWorkerClosed.get());
        Assert.assertTrue(masterClosed.get());
    }

    @Test
    public void testInitializeServiceClosesPartiallyInitializedService() {
        AtomicBoolean closed = new AtomicBoolean();
        RuntimeException cause = new IllegalStateException("init failed");

        try {
            initializeService(new Object(), service -> {
                throw cause;
            }, service -> closed.set(true));
            Assert.fail("Expected initialization to fail");
        } catch (RuntimeException e) {
            Assert.assertSame(cause, e);
        }

        Assert.assertTrue(closed.get());
    }

    @Test
    public void testCloseServicesAndJoinStopsSpawnedThreads() throws Exception {
        ServiceLifecycle lifecycle = new ServiceLifecycle();
        AtomicBoolean closed = new AtomicBoolean();
        CountDownLatch started = new CountDownLatch(1);
        Thread thread = new Thread(() -> {
            started.countDown();
            try {
                Thread.sleep(Long.MAX_VALUE);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        lifecycle.registerWorker(() -> closed.set(true));
        thread.start();
        Assert.assertTrue(started.await(1, TimeUnit.SECONDS));

        closeServicesAndJoin(lifecycle, Arrays.asList(thread), null);

        Assert.assertTrue(closed.get());
        Assert.assertFalse(thread.isAlive());
    }

    @Test
    public void testCloseServicesAndJoinUsesOneTimeoutBudget()
            throws Exception {
        long timeout = 200L;
        AtomicBoolean stopping = new AtomicBoolean();
        CountDownLatch started = new CountDownLatch(3);
        Thread firstWorker = newInterruptIgnoringThread(stopping, started);
        Thread secondWorker = newInterruptIgnoringThread(stopping, started);
        Thread master = newInterruptIgnoringThread(stopping, started);
        firstWorker.start();
        secondWorker.start();
        master.start();
        Assert.assertTrue(started.await(1, TimeUnit.SECONDS));

        long start = System.nanoTime();
        try {
            closeServicesAndJoin(new ServiceLifecycle(),
                                 Arrays.asList(firstWorker, secondWorker),
                                 master, timeout);
            Assert.fail("Expected service threads to time out");
        } catch (ComputerException ignored) {
            // The timeout is expected; the assertion below verifies its budget
        } finally {
            stopping.set(true);
            firstWorker.interrupt();
            secondWorker.interrupt();
            master.interrupt();
            firstWorker.join(TimeUnit.SECONDS.toMillis(1L));
            secondWorker.join(TimeUnit.SECONDS.toMillis(1L));
            master.join(TimeUnit.SECONDS.toMillis(1L));
        }
        long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        Assert.assertTrue("Cleanup exceeded its shared timeout budget: " + elapsed,
                          elapsed < timeout * 2L);
    }

    @Test
    public void testCloseServicesAndJoinStopsWorkersBeforeMaster()
            throws Exception {
        ServiceLifecycle lifecycle = new ServiceLifecycle();
        CountDownLatch workerStarted = new CountDownLatch(1);
        CountDownLatch workerStopped = new CountDownLatch(1);
        CountDownLatch masterStarted = new CountDownLatch(1);
        AtomicBoolean masterInterruptedBeforeWorkerStopped =
                new AtomicBoolean();
        Thread workerThread = new Thread(() -> {
            workerStarted.countDown();
            try {
                Thread.sleep(Long.MAX_VALUE);
            } catch (InterruptedException e) {
                try {
                    Thread.sleep(200L);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            } finally {
                workerStopped.countDown();
            }
        });
        Thread masterThread = new Thread(() -> {
            masterStarted.countDown();
            try {
                workerStopped.await();
            } catch (InterruptedException e) {
                masterInterruptedBeforeWorkerStopped.set(
                        workerStopped.getCount() != 0L);
                Thread.currentThread().interrupt();
            }
        });
        workerThread.start();
        masterThread.start();
        Assert.assertTrue(workerStarted.await(1, TimeUnit.SECONDS));
        Assert.assertTrue(masterStarted.await(1, TimeUnit.SECONDS));

        closeServicesAndJoin(lifecycle, Arrays.asList(workerThread),
                             masterThread);

        Assert.assertFalse(masterInterruptedBeforeWorkerStopped.get());
        Assert.assertFalse(workerThread.isAlive());
        Assert.assertFalse(masterThread.isAlive());
    }

    @Test
    public void testOneWorker() {
        ServiceLifecycle lifecycle = new ServiceLifecycle();
        CompletableFuture<Void> masterFuture = new CompletableFuture<>();
        Thread masterThread = new Thread(() -> {
            String[] args = OptionsBuilder.newInstance()
                                          .withJobId("local_002")
                                          .withAlgorithm(PageRankParams.class)
                                          .withResultName("rank")
                                          .withResultClass(DoubleValue.class)
                                          .withMessageClass(DoubleValue.class)
                                          .withMaxSuperStep(3)
                                           .withComputationClass(COMPUTATION)
                                           .withWorkerCount(1)
                                           .withTestBspTimeouts()
                                           .withBufferThreshold(50)
                                          .withBufferCapacity(60)
                                          .withRpcServerHost("127.0.0.1")
                                          .withRpcServerPort(8611)
                                          .withRpcServerPort(0)
                                          .build();
            try {
                MasterService service = initMaster(args);
                try {
                    if (!lifecycle.registerMaster(service::close)) {
                        masterFuture.cancel(false);
                        return;
                    }
                    service.execute();
                    masterFuture.complete(null);
                } finally {
                    closeMaster(service);
                }
            } catch (Exception e) {
                LOG.error("Failed to execute master service", e);
                masterFuture.completeExceptionally(e);
            }
        });
        masterThread.setDaemon(true);

        CompletableFuture<Void> workerFuture = new CompletableFuture<>();
        Thread workerThread = new Thread(() -> {
            String[] args = OptionsBuilder.newInstance()
                                          .withJobId("local_002")
                                          .withAlgorithm(PageRankParams.class)
                                          .withResultName("rank")
                                          .withResultClass(DoubleValue.class)
                                          .withMessageClass(DoubleValue.class)
                                          .withMaxSuperStep(3)
                                           .withComputationClass(COMPUTATION)
                                           .withWorkerCount(1)
                                           .withTestBspTimeouts()
                                           .withBufferThreshold(50)
                                          .withBufferCapacity(60)
                                          .withTransoprtServerPort(0)
                                          .build();
            try {
                WorkerService service = initWorker(args);
                try {
                    if (!lifecycle.registerWorker(service::close)) {
                        workerFuture.cancel(false);
                        return;
                    }
                    service.execute();
                    workerFuture.complete(null);
                } finally {
                    closeWorker(service);
                }
            } catch (Throwable e) {
                LOG.error("Failed to execute worker service", e);
                workerFuture.completeExceptionally(e);
            }
        });
        workerThread.setDaemon(true);
        masterThread.start();
        workerThread.start();

        waitForServicesAndClose(lifecycle,
                                Arrays.asList(workerFuture, masterFuture),
                                Arrays.asList(workerThread), masterThread);
    }

    @Test
    public void testMultiWorkers() throws IOException {
        int workerCount = 3;
        int partitionCount = 3;
        ServiceLifecycle lifecycle = new ServiceLifecycle();
        CompletableFuture<Void> masterFuture = new CompletableFuture<>();
        Thread masterThread = new Thread(() -> {
            String[] args = OptionsBuilder.newInstance()
                                          .withJobId("local_003")
                                          .withAlgorithm(PageRankParams.class)
                                          .withResultName("rank")
                                          .withResultClass(DoubleValue.class)
                                          .withMessageClass(DoubleValue.class)
                                          .withMaxSuperStep(3)
                                           .withComputationClass(COMPUTATION)
                                           .withWorkerCount(workerCount)
                                           .withPartitionCount(partitionCount)
                                           .withTestBspTimeouts()
                                           .withRpcServerHost("127.0.0.1")
                                          .withRpcServerPort(0)
                                          .build();
            try {
                MasterService service = initMaster(args);
                try {
                    if (!lifecycle.registerMaster(service::close)) {
                        masterFuture.cancel(false);
                        return;
                    }
                    service.execute();
                    masterFuture.complete(null);
                } finally {
                    closeMaster(service);
                }
            } catch (Throwable e) {
                LOG.error("Failed to execute master service", e);
                masterFuture.completeExceptionally(e);
            }
        });
        masterThread.setDaemon(true);

        Map<Thread, CompletableFuture<Void>> workers = new HashMap<>(workerCount);
        for (int i = 1; i <= workerCount; i++) {
            String dir = "[jobs-" + i + "]";

            CompletableFuture<Void> workerFuture = new CompletableFuture<>();
            Thread thread = new Thread(() -> {
                String[] args;
                args = OptionsBuilder.newInstance()
                        .withJobId("local_003")
                        .withAlgorithm(PageRankParams.class)
                        .withResultName("rank")
                        .withResultClass(DoubleValue.class)
                        .withMessageClass(DoubleValue.class)
                        .withMaxSuperStep(3)
                        .withComputationClass(COMPUTATION)
                        .withWorkerCount(workerCount)
                        .withPartitionCount(partitionCount)
                        .withTestBspTimeouts()
                        .withTransoprtServerPort(0)
                        .withDataDirs(dir)
                        .build();
                try {
                    WorkerService service = initWorker(args);
                    try {
                        if (!lifecycle.registerWorker(service::close)) {
                            workerFuture.cancel(false);
                            return;
                        }
                        service.execute();
                        workerFuture.complete(null);
                    } finally {
                        closeWorker(service);
                    }
                } catch (Throwable e) {
                    LOG.error("Failed to execute worker service", e);
                    workerFuture.completeExceptionally(e);
                }
            });
            thread.setDaemon(true);
            workers.put(thread, workerFuture);
        }

        masterThread.start();
        for (Thread worker : workers.keySet()) {
            worker.start();
        }

        List<CompletableFuture<Void>> futures = new ArrayList<>(workers.values());
        futures.add(masterFuture);
        waitForServicesAndClose(lifecycle, futures,
                                new ArrayList<>(workers.keySet()),
                                masterThread);
    }

    @Test
    public void testOneWorkerWithBusyClient() {
        ServiceLifecycle lifecycle = new ServiceLifecycle();
        CompletableFuture<Void> masterFuture = new CompletableFuture<>();
        Thread masterThread = new Thread(() -> {
            String[] args = OptionsBuilder.newInstance()
                                          .withJobId("local_002")
                                          .withAlgorithm(PageRankParams.class)
                                          .withResultName("rank")
                                          .withResultClass(DoubleValue.class)
                                          .withMessageClass(DoubleValue.class)
                                          .withMaxSuperStep(3)
                                           .withComputationClass(COMPUTATION)
                                           .withWorkerCount(1)
                                           .withTestBspTimeouts()
                                           .withWriteBufferHighMark(10)
                                          .withWriteBufferLowMark(5)
                                          .withRpcServerHost("127.0.0.1")
                                          .withRpcServerPort(0)
                                          .build();
            try {
                MasterService service = initMaster(args);
                try {
                    if (!lifecycle.registerMaster(service::close)) {
                        masterFuture.cancel(false);
                        return;
                    }
                    service.execute();
                    masterFuture.complete(null);
                } finally {
                    closeMaster(service);
                }
            } catch (Throwable e) {
                LOG.error("Failed to execute master service", e);
                masterFuture.completeExceptionally(e);
            }
        });
        masterThread.setDaemon(true);

        CompletableFuture<Void> workerFuture = new CompletableFuture<>();
        int transoprtServerPort = 8998;
        Thread workerThread = new Thread(() -> {
            String[] args = OptionsBuilder.newInstance()
                                          .withJobId("local_002")
                                          .withAlgorithm(PageRankParams.class)
                                          .withResultName("rank")
                                          .withResultClass(DoubleValue.class)
                                          .withMessageClass(DoubleValue.class)
                                          .withMaxSuperStep(3)
                                           .withComputationClass(COMPUTATION)
                                           .withWorkerCount(1)
                                           .withTestBspTimeouts()
                                           .withWriteBufferHighMark(20)
                                          .withWriteBufferLowMark(10)
                                          .withTransoprtServerPort(transoprtServerPort)
                                          .build();
            try {
                WorkerService service = initWorker(args);
                try {
                    if (!lifecycle.registerWorker(service::close)) {
                        workerFuture.cancel(false);
                        return;
                    }
                    // Let send rate slowly
                    this.slowSendFunc(service, transoprtServerPort);
                    service.execute();
                    workerFuture.complete(null);
                } finally {
                    closeWorker(service);
                }
            } catch (Throwable e) {
                LOG.error("Failed to execute worker service", e);
                workerFuture.completeExceptionally(e);
            }
        });
        workerThread.setDaemon(true);
        masterThread.start();
        workerThread.start();

        waitForServicesAndClose(lifecycle,
                                Arrays.asList(workerFuture, masterFuture),
                                Arrays.asList(workerThread), masterThread);
    }

    private void slowSendFunc(WorkerService service, int port) throws TransportException {
        Managers managers = Whitebox.getInternalState(service, "managers");
        DataClientManager clientManager = managers.get(
                                          DataClientManager.NAME);
        ConnectionManager connManager = Whitebox.getInternalState(
                                        clientManager, "connManager");
        NettyTransportClient client = (NettyTransportClient)
                                      connManager.getOrCreateClient(
                                      "127.0.0.1", port);
        ClientSession clientSession = Whitebox.invoke(client.getClass(),
                                                      "clientSession", client);
        Function<Message, Future<Void>> sendFuncBak = Whitebox.getInternalState(
                                                      clientSession,
                                                      "sendFunction");
        Function<Message, Future<Void>> sendFunc = message -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return sendFuncBak.apply(message);
        };
        Whitebox.setInternalState(clientSession, "sendFunction", sendFunc);
    }

    private MasterService initMaster(String[] args) {
        Config config = ComputerContextUtil.initContext(
                        ComputerContextUtil.convertToMap(args));
        MasterService service = new MasterService();
        return initializeService(service, s -> s.init(config),
                                 SenderIntegrateTest::closeMaster);
    }

    private WorkerService initWorker(String[] args) {
        Config config = ComputerContextUtil.initContext(
                        ComputerContextUtil.convertToMap(args));
        WorkerService service = new WorkerService();
        return initializeService(service, s -> s.init(config),
                                 SenderIntegrateTest::closeWorker);
    }

    private static <T> T initializeService(T service, Consumer<T> initializer,
                                           Consumer<T> closer) {
        try {
            initializer.accept(service);
            return service;
        } catch (RuntimeException | Error e) {
            try {
                closer.accept(service);
            } catch (RuntimeException | Error closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
    }

    private static void waitForServices(List<CompletableFuture<Void>> futures) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        for (CompletableFuture<Void> future : futures) {
            future.whenComplete((r, e) -> {
                if (e != null) {
                    result.completeExceptionally(e);
                }
            });
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                         .whenComplete((r, e) -> {
            if (e == null) {
                result.complete(null);
            } else {
                result.completeExceptionally(e);
            }
        });
        try {
            result.get(SERVICE_WAIT_TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new ComputerException("Timed out to wait for master and " +
                                        "worker services", e);
        } catch (ExecutionException e) {
            throw new ComputerException("Failed to wait for master and " +
                                        "worker services", e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ComputerException("Interrupted when waiting for master " +
                                        "and worker services", e);
        }
    }

    private static void waitForServicesAndClose(
            ServiceLifecycle lifecycle, List<CompletableFuture<Void>> futures,
            List<Thread> workerThreads, Thread masterThread) {
        try {
            waitForServices(futures);
        } catch (RuntimeException | Error e) {
            try {
                closeServicesAndJoin(lifecycle, workerThreads, masterThread);
            } catch (RuntimeException | Error closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
        closeServicesAndJoin(lifecycle, workerThreads, masterThread);
    }

    private static void closeServicesAndJoin(ServiceLifecycle lifecycle,
                                             List<Thread> workerThreads,
                                             Thread masterThread) {
        closeServicesAndJoin(lifecycle, workerThreads, masterThread,
                             SERVICE_WAIT_TIMEOUT);
    }

    private static void closeServicesAndJoin(ServiceLifecycle lifecycle,
                                             List<Thread> workerThreads,
                                             Thread masterThread,
                                             long timeout) {
        long deadline = System.nanoTime() +
                        TimeUnit.MILLISECONDS.toNanos(timeout);
        Throwable closeFailure = lifecycle.closeAll();
        Throwable workerFailure = interruptAndJoinThreads(
                                  workerThreads,
                                  remainingTimeout(deadline));
        Throwable masterFailure = null;
        if (masterThread != null) {
            masterFailure = interruptAndJoinThreads(
                            Arrays.asList(masterThread),
                            remainingTimeout(deadline));
        }
        if (closeFailure != null) {
            addFailure(closeFailure, workerFailure);
            addFailure(closeFailure, masterFailure);
            throw new ComputerException("Failed to close service", closeFailure);
        }
        if (workerFailure != null) {
            addFailure(workerFailure, masterFailure);
            throw new ComputerException("Failed to close worker service thread",
                                        workerFailure);
        }
        if (masterFailure != null) {
            throw new ComputerException("Failed to close master service thread",
                                        masterFailure);
        }
    }

    private static Throwable interruptAndJoinThreads(List<Thread> threads,
                                                     long timeout) {
        long deadline = System.nanoTime() +
                        TimeUnit.MILLISECONDS.toNanos(timeout);
        for (Thread thread : threads) {
            thread.interrupt();
        }
        Throwable failure = null;
        for (Thread thread : threads) {
            long remaining = remainingTimeout(deadline);
            if (remaining > 0L) {
                try {
                    thread.join(remaining);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    failure = addFailure(failure, new ComputerException(
                                    "Interrupted when waiting for service " +
                                    "thread to stop", e));
                }
            }
            if (thread.isAlive()) {
                failure = addFailure(failure, new ComputerException(
                                     "Timed out to wait for service thread " +
                                     "to stop"));
            }
        }
        return failure;
    }

    private static Thread newInterruptIgnoringThread(AtomicBoolean stopping,
                                                      CountDownLatch started) {
        Thread thread = new Thread(() -> {
            started.countDown();
            while (!stopping.get()) {
                try {
                    Thread.sleep(Long.MAX_VALUE);
                } catch (InterruptedException ignored) {
                    // Keep waiting until the test explicitly stops this thread
                }
            }
        });
        thread.setDaemon(true);
        return thread;
    }

    private static long remainingTimeout(long deadline) {
        long remaining = deadline - System.nanoTime();
        return remaining <= 0L ? 0L :
               TimeUnit.NANOSECONDS.toMillis(remaining) + 1L;
    }

    private static Throwable addFailure(Throwable failure, Throwable cause) {
        if (cause == null) {
            return failure;
        }
        if (failure == null) {
            return cause;
        }
        failure.addSuppressed(cause);
        return failure;
    }

    private static void closeWorker(WorkerService service) {
        if (service != null) {
            service.close();
        }
    }

    private static void closeMaster(MasterService service) {
        if (service != null) {
            service.close();
        }
    }

    private static class ServiceLifecycle {

        private final List<Runnable> workerClosers = new ArrayList<>();
        private final List<Runnable> masterClosers = new ArrayList<>();
        private boolean closing;

        public boolean registerWorker(Runnable closer) {
            return this.register(this.workerClosers, closer);
        }

        public boolean registerMaster(Runnable closer) {
            return this.register(this.masterClosers, closer);
        }

        private boolean register(List<Runnable> closers, Runnable closer) {
            boolean closeImmediately;
            synchronized (this) {
                closeImmediately = this.closing;
                if (!closeImmediately) {
                    closers.add(closer);
                }
            }
            if (closeImmediately) {
                closer.run();
            }
            return !closeImmediately;
        }

        public Throwable closeAll() {
            List<Runnable> workerClosers;
            List<Runnable> masterClosers;
            synchronized (this) {
                this.closing = true;
                workerClosers = new ArrayList<>(this.workerClosers);
                masterClosers = new ArrayList<>(this.masterClosers);
                this.workerClosers.clear();
                this.masterClosers.clear();
            }
            Throwable failure = closeAll(workerClosers, null);
            return closeAll(masterClosers, failure);
        }

        private static Throwable closeAll(List<Runnable> closers,
                                          Throwable failure) {
            for (Runnable closer : closers) {
                try {
                    closer.run();
                } catch (Throwable e) {
                    if (failure == null) {
                        failure = e;
                    } else {
                        failure.addSuppressed(e);
                    }
                }
            }
            return failure;
        }
    }

    private static class OptionsBuilder {

        private final List<String> options;

        public static OptionsBuilder newInstance() {
            return new OptionsBuilder();
        }

        public OptionsBuilder() {
            this.options = new ArrayList<>();
        }

        public String[] build() {
            return this.options.toArray(new String[0]);
        }

        public OptionsBuilder withJobId(String jobId) {
            this.options.add(ComputerOptions.JOB_ID.name());
            this.options.add(jobId);
            return this;
        }

        public OptionsBuilder withAlgorithm(Class<?> clazz) {
            this.options.add(ComputerOptions.ALGORITHM_PARAMS_CLASS.name());
            this.options.add(clazz.getName());
            return this;
        }

        public OptionsBuilder withResultClass(Class<?> clazz) {
            this.options.add(ComputerOptions.ALGORITHM_RESULT_CLASS.name());
            this.options.add(clazz.getName());
            return this;
        }

        public OptionsBuilder withMessageClass(Class<?> clazz) {
            this.options.add(ComputerOptions.ALGORITHM_MESSAGE_CLASS.name());
            this.options.add(clazz.getName());
            return this;
        }

        public OptionsBuilder withResultName(String name) {
            this.options.add(ComputerOptions.OUTPUT_RESULT_NAME.name());
            this.options.add(name);
            return this;
        }

        public OptionsBuilder withMaxSuperStep(int maxSuperStep) {
            this.options.add(ComputerOptions.BSP_MAX_SUPER_STEP.name());
            this.options.add(String.valueOf(maxSuperStep));
            return this;
        }

        public OptionsBuilder withComputationClass(Class<?> clazz) {
            this.options.add(ComputerOptions.WORKER_COMPUTATION_CLASS.name());
            this.options.add(clazz.getName());
            return this;
        }

        public OptionsBuilder withWorkerCount(int count) {
            this.options.add(ComputerOptions.JOB_WORKERS_COUNT.name());
            this.options.add(String.valueOf(count));
            return this;
        }

        public OptionsBuilder withTestBspTimeouts() {
            this.options.add(ComputerOptions.BSP_WAIT_WORKERS_TIMEOUT.name());
            this.options.add(String.valueOf(BSP_WAIT_TIMEOUT));
            this.options.add(ComputerOptions.BSP_WAIT_MASTER_TIMEOUT.name());
            this.options.add(String.valueOf(BSP_WAIT_TIMEOUT));
            return this;
        }

        public OptionsBuilder withPartitionCount(int count) {
            this.options.add(ComputerOptions.JOB_PARTITIONS_COUNT.name());
            this.options.add(String.valueOf(count));
            return this;
        }

        public OptionsBuilder withBufferThreshold(int sizeInByte) {
            this.options.add(ComputerOptions.WORKER_WRITE_BUFFER_THRESHOLD
                                            .name());
            this.options.add(String.valueOf(sizeInByte));
            return this;
        }

        public OptionsBuilder withBufferCapacity(int sizeInByte) {
            this.options.add(ComputerOptions.WORKER_WRITE_BUFFER_INIT_CAPACITY
                                            .name());
            this.options.add(String.valueOf(sizeInByte));
            return this;
        }

        public OptionsBuilder withTransoprtServerPort(int dataPort) {
            this.options.add(ComputerOptions.TRANSPORT_SERVER_PORT.name());
            this.options.add(String.valueOf(dataPort));
            return this;
        }

        public OptionsBuilder withWriteBufferHighMark(int mark) {
            this.options.add(ComputerOptions.TRANSPORT_WRITE_BUFFER_HIGH_MARK
                                            .name());
            this.options.add(String.valueOf(mark));
            return this;
        }

        public OptionsBuilder withWriteBufferLowMark(int mark) {
            this.options.add(ComputerOptions.TRANSPORT_WRITE_BUFFER_LOW_MARK
                                            .name());
            this.options.add(String.valueOf(mark));
            return this;
        }

        public OptionsBuilder withRpcServerHost(String host) {
            this.options.add(RpcOptions.RPC_SERVER_HOST.name());
            this.options.add(host);
            return this;
        }

        public OptionsBuilder withRpcServerPort(int port) {
            this.options.add(RpcOptions.RPC_SERVER_PORT.name());
            this.options.add(String.valueOf(port));
            return this;
        }

        public OptionsBuilder withDataDirs(String dataDirs) {
            this.options.add(ComputerOptions.WORKER_DATA_DIRS.name());
            this.options.add(String.valueOf(dataDirs));
            return this;
        }
    }
}
