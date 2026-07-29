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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

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
    private static final long TEST_THREAD_JOIN_TIMEOUT = TimeUnit.SECONDS.toMillis(5L);

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
    public void testMasterErrorCompletesServiceFuture() throws Exception {
        ServiceLifecycle lifecycle = new ServiceLifecycle();
        Error cause = new AssertionError("master failed");
        ServiceTask master = newServiceTask(
                             Object::new, lifecycle::registerMaster,
                             service -> {
                                 throw cause;
                             }, service -> {
                             });
        master.thread.start();

        try {
            master.future.get(1, TimeUnit.SECONDS);
            Assert.fail("Expected master error to fail the service future");
        } catch (ExecutionException e) {
            Assert.assertSame(cause, e.getCause());
        } catch (TimeoutException e) {
            Assert.fail("Timed out to wait for master error");
        } finally {
            lifecycle.closeAll();
            interruptAndJoinThreads(Arrays.asList(master.thread),
                                    TEST_THREAD_JOIN_TIMEOUT);
        }
    }

    @Test
    public void testServiceTaskPreservesExecutionFailure() throws Exception {
        RuntimeException executeFailure =
                new IllegalStateException("service execution failed");
        RuntimeException closeFailure =
                new IllegalStateException("service close failed");
        ServiceTask task = newServiceTask(
                           Object::new, closer -> true,
                           service -> {
                               throw executeFailure;
                           }, service -> {
                               throw closeFailure;
                           });
        task.thread.start();

        try {
            task.future.get(1, TimeUnit.SECONDS);
            Assert.fail("Expected service task to fail");
        } catch (ExecutionException e) {
            Assert.assertSame(executeFailure, e.getCause());
            Assert.assertArrayEquals(new Throwable[]{closeFailure},
                                     e.getCause().getSuppressed());
        } finally {
            interruptAndJoinThreads(Arrays.asList(task.thread),
                                    TEST_THREAD_JOIN_TIMEOUT);
        }
    }

    @Test
    public void testServiceCleanupFailureFailsTaskWithoutDoubleClose()
            throws Exception {
        ServiceLifecycle lifecycle = new ServiceLifecycle();
        AtomicInteger closeCount = new AtomicInteger();
        CountDownLatch executeStarted = new CountDownLatch(1);
        CountDownLatch allowExecuteReturn = new CountDownLatch(1);
        CountDownLatch closeStarted = new CountDownLatch(1);
        CountDownLatch allowCloseReturn = new CountDownLatch(1);
        RuntimeException cause =
                new IllegalStateException("service close failed");
        ServiceTask task = newServiceTask(
                           Object::new, lifecycle::registerWorker,
                           service -> {
                               executeStarted.countDown();
                               allowExecuteReturn.await();
                           }, service -> {
                               closeCount.incrementAndGet();
                               closeStarted.countDown();
                               try {
                                   allowCloseReturn.await();
                               } catch (InterruptedException e) {
                                   Thread.currentThread().interrupt();
                                   throw new AssertionError(e);
                               }
                               throw cause;
                           });
        CompletableFuture<Throwable> lifecycleResult =
                new CompletableFuture<>();
        Thread lifecycleThread = new Thread(
                () -> lifecycleResult.complete(lifecycle.closeAll()));
        task.thread.start();

        Throwable taskFailure = null;
        try {
            Assert.assertTrue(executeStarted.await(1, TimeUnit.SECONDS));
            lifecycleThread.start();
            Assert.assertTrue(closeStarted.await(1, TimeUnit.SECONDS));
            allowExecuteReturn.countDown();
            task.thread.join(TimeUnit.SECONDS.toMillis(1L));
            Assert.assertFalse("Service future completed before cleanup",
                               task.future.isDone());

            allowCloseReturn.countDown();
            try {
                task.future.get(1, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                taskFailure = e.getCause();
            }
            task.thread.join(TEST_THREAD_JOIN_TIMEOUT);
            lifecycleThread.join(TEST_THREAD_JOIN_TIMEOUT);
            Assert.assertSame(cause, lifecycleResult.get(
                    1, TimeUnit.SECONDS));
        } finally {
            allowExecuteReturn.countDown();
            allowCloseReturn.countDown();
            lifecycle.closeAll();
            interruptAndJoinThreads(Arrays.asList(task.thread,
                                                  lifecycleThread),
                                    TEST_THREAD_JOIN_TIMEOUT);
        }

        Assert.assertSame(cause, taskFailure);
        Assert.assertEquals(1, closeCount.get());
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
        try {
            thread.start();
            Assert.assertTrue(started.await(1, TimeUnit.SECONDS));

            closeServicesAndJoin(lifecycle, Arrays.asList(thread), null);

            Assert.assertTrue(closed.get());
            Assert.assertFalse(thread.isAlive());
        } finally {
            lifecycle.closeAll();
            interruptAndJoinThreads(Arrays.asList(thread),
                                    TEST_THREAD_JOIN_TIMEOUT);
        }
    }

    @Test
    public void testCloseServicesAndJoinUsesOneTimeoutBudget() throws Exception {
        long timeout = TimeUnit.SECONDS.toMillis(1L);
        long maxElapsed = timeout + timeout / 2L;
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
        long elapsed = -1L;
        try {
            closeServicesAndJoin(new ServiceLifecycle(),
                                 Arrays.asList(firstWorker, secondWorker),
                                 master, timeout);
            Assert.fail("Expected service threads to time out");
        } catch (ComputerException ignored) {
            elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        } finally {
            stopping.set(true);
            firstWorker.interrupt();
            secondWorker.interrupt();
            master.interrupt();
            firstWorker.join(TimeUnit.SECONDS.toMillis(1L));
            secondWorker.join(TimeUnit.SECONDS.toMillis(1L));
            master.join(TimeUnit.SECONDS.toMillis(1L));
        }
        Assert.assertTrue("Cleanup exceeded its shared timeout budget: " + elapsed,
                          elapsed < maxElapsed);
    }

    @Test
    public void testCloseBeforeMaster() throws Exception {
        ServiceLifecycle lifecycle = new ServiceLifecycle();
        CountDownLatch workerStarted = new CountDownLatch(1);
        CountDownLatch workerStopped = new CountDownLatch(1);
        CountDownLatch masterStarted = new CountDownLatch(1);
        AtomicBoolean masterInterruptedBeforeWorkerStopped = new AtomicBoolean();
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
        try {
            workerThread.start();
            masterThread.start();
            Assert.assertTrue(workerStarted.await(1, TimeUnit.SECONDS));
            Assert.assertTrue(masterStarted.await(1, TimeUnit.SECONDS));

            closeServicesAndJoin(lifecycle, Arrays.asList(workerThread),
                                 masterThread);

            Assert.assertFalse(masterInterruptedBeforeWorkerStopped.get());
            Assert.assertFalse(workerThread.isAlive());
            Assert.assertFalse(masterThread.isAlive());
        } finally {
            lifecycle.closeAll();
            interruptAndJoinThreads(Arrays.asList(workerThread, masterThread),
                                    TEST_THREAD_JOIN_TIMEOUT);
        }
    }

    @Test
    public void testOneWorker() {
        ServiceLifecycle lifecycle = new ServiceLifecycle();
        String[] masterArgs = commonOptions("local_002", 1)
                              .withBufferThreshold(50)
                              .withBufferCapacity(60)
                              .withRpcServerHost("127.0.0.1")
                              .withRpcServerPort(0)
                              .build();
        String[] workerArgs = commonOptions("local_002", 1)
                              .withBufferThreshold(50)
                              .withBufferCapacity(60)
                              .withTransoprtServerPort(0)
                              .build();
        ServiceTask master = masterTask(lifecycle, masterArgs);
        ServiceTask worker = workerTask(lifecycle, workerArgs,
                                        WorkerService::execute);
        runServices(lifecycle, Arrays.asList(worker), master);
    }

    @Test
    public void testMultiWorkers() {
        int workerCount = 3;
        int partitionCount = 3;
        ServiceLifecycle lifecycle = new ServiceLifecycle();
        String[] masterArgs = commonOptions("local_003", workerCount)
                              .withPartitionCount(partitionCount)
                              .withRpcServerHost("127.0.0.1")
                              .withRpcServerPort(0)
                              .build();
        ServiceTask master = masterTask(lifecycle, masterArgs);
        List<ServiceTask> workers = new ArrayList<>(workerCount);
        for (int i = 1; i <= workerCount; i++) {
            String[] workerArgs = commonOptions("local_003", workerCount)
                                  .withPartitionCount(partitionCount)
                                  .withTransoprtServerPort(0)
                                  .withDataDirs("[jobs-" + i + "]")
                                  .build();
            workers.add(workerTask(lifecycle, workerArgs,
                                   WorkerService::execute));
        }
        runServices(lifecycle, workers, master);
    }

    @Test
    public void testOneWorkerWithBusyClient() {
        ServiceLifecycle lifecycle = new ServiceLifecycle();
        int transoprtServerPort = 8998;
        String[] masterArgs = commonOptions("local_002", 1)
                              .withWriteBufferHighMark(10)
                              .withWriteBufferLowMark(5)
                              .withRpcServerHost("127.0.0.1")
                              .withRpcServerPort(0)
                              .build();
        String[] workerArgs = commonOptions("local_002", 1)
                              .withWriteBufferHighMark(20)
                              .withWriteBufferLowMark(10)
                              .withTransoprtServerPort(transoprtServerPort)
                              .build();
        ServiceTask master = masterTask(lifecycle, masterArgs);
        ServiceTask worker = workerTask(lifecycle, workerArgs, service -> {
            // Let send rate slowly
            this.slowSendFunc(service, transoprtServerPort);
            service.execute();
        });
        runServices(lifecycle, Arrays.asList(worker), master);
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

    private static OptionsBuilder commonOptions(String jobId,
                                                int workerCount) {
        return OptionsBuilder.newInstance()
                             .withJobId(jobId)
                             .withAlgorithm(PageRankParams.class)
                             .withResultName("rank")
                             .withResultClass(DoubleValue.class)
                             .withMessageClass(DoubleValue.class)
                             .withMaxSuperStep(3)
                             .withComputationClass(COMPUTATION)
                             .withWorkerCount(workerCount)
                             .withTestBspTimeouts();
    }

    private ServiceTask masterTask(ServiceLifecycle lifecycle, String[] args) {
        return newServiceTask(() -> this.initMaster(args),
                              lifecycle::registerMaster,
                              MasterService::execute,
                              MasterService::close);
    }

    private ServiceTask workerTask(ServiceLifecycle lifecycle, String[] args,
                                   ServiceExecutor<WorkerService> executor) {
        return newServiceTask(() -> this.initWorker(args),
                              lifecycle::registerWorker, executor,
                              WorkerService::close);
    }

    private static <T> ServiceTask newServiceTask(
            Supplier<T> initializer, Function<Runnable, Boolean> registrar,
            ServiceExecutor<T> executor, Consumer<T> closer) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        Thread thread = new Thread(() -> {
            try {
                T service = initializer.get();
                Runnable closeService = new Runnable() {

                    private boolean closed;
                    private Throwable failure;

                    @Override
                    public synchronized void run() {
                        if (!this.closed) {
                            this.closed = true;
                            try {
                                closer.accept(service);
                            } catch (RuntimeException | Error e) {
                                this.failure = e;
                            }
                        }
                        if (this.failure instanceof RuntimeException) {
                            throw (RuntimeException) this.failure;
                        }
                        if (this.failure != null) {
                            throw (Error) this.failure;
                        }
                    }
                };
                Throwable failure = null;
                try {
                    if (!registrar.apply(closeService)) {
                        future.cancel(false);
                        return;
                    }
                    executor.execute(service);
                } catch (Throwable e) {
                    failure = e;
                    throw e;
                } finally {
                    try {
                        closeService.run();
                    } catch (Throwable closeFailure) {
                        if (failure == null) {
                            throw closeFailure;
                        }
                        if (closeFailure != failure) {
                            failure.addSuppressed(closeFailure);
                        }
                    }
                }
                future.complete(null);
            } catch (Throwable e) {
                LOG.error("Failed to execute service", e);
                future.completeExceptionally(e);
            }
        });
        thread.setDaemon(true);
        return new ServiceTask(thread, future);
    }

    private static void runServices(ServiceLifecycle lifecycle,
                                    List<ServiceTask> workers,
                                    ServiceTask master) {
        master.thread.start();
        List<CompletableFuture<Void>> futures =
                new ArrayList<>(workers.size() + 1);
        List<Thread> workerThreads = new ArrayList<>(workers.size());
        for (ServiceTask worker : workers) {
            worker.thread.start();
            futures.add(worker.future);
            workerThreads.add(worker.thread);
        }
        futures.add(master.future);
        waitForServicesAndClose(lifecycle, futures, workerThreads,
                                master.thread);
    }

    private MasterService initMaster(String[] args) {
        Config config = ComputerContextUtil.initContext(
                        ComputerContextUtil.convertToMap(args));
        MasterService service = new MasterService();
        return initializeService(service, s -> s.init(config),
                                 MasterService::close);
    }

    private WorkerService initWorker(String[] args) {
        Config config = ComputerContextUtil.initContext(
                        ComputerContextUtil.convertToMap(args));
        WorkerService service = new WorkerService();
        return initializeService(service, s -> s.init(config),
                                 WorkerService::close);
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
                         .thenRun(() -> result.complete(null));
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

    @FunctionalInterface
    private interface ServiceExecutor<T> {

        void execute(T service) throws Throwable;
    }

    private static class ServiceTask {

        private final Thread thread;
        private final CompletableFuture<Void> future;

        public ServiceTask(Thread thread, CompletableFuture<Void> future) {
            this.thread = thread;
            this.future = future;
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
            synchronized (this) {
                if (!this.closing) {
                    closers.add(closer);
                    return true;
                }
            }
            closer.run();
            return false;
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
            workerClosers.addAll(masterClosers);
            Throwable failure = closeAll(workerClosers, null);
            return failure;
        }

        private static Throwable closeAll(List<Runnable> closers,
                                          Throwable failure) {
            for (Runnable closer : closers) {
                try {
                    closer.run();
                } catch (Throwable e) {
                    failure = addFailure(failure, e);
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
