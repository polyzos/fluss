/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.utils;

import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

/** Tests for the {@link com.alibaba.fluss.utils.AbstractAutoCloseableRegistry}. */
public abstract class AbstractAutoCloseableRegistryTest<C extends Closeable, E extends C, T> {
    private static final int TEST_TIMEOUT_SECONDS = 10;
    protected ProducerThread[] streamOpenThreads;
    protected AbstractAutoCloseableRegistry<C, E, T, IOException> closeableRegistry;
    protected AtomicInteger unclosedCounter;

    protected abstract void registerCloseable(Closeable closeable) throws IOException;

    protected abstract AbstractAutoCloseableRegistry<C, E, T, IOException> createRegistry();

    protected abstract ProducerThread<C, E, T> createProducerThread(
            AbstractAutoCloseableRegistry<C, E, T, IOException> registry,
            AtomicInteger unclosedCounter,
            int maxStreams);

    public void setup(int maxStreams) {
        this.closeableRegistry = createRegistry();
        this.unclosedCounter = new AtomicInteger(0);
        this.streamOpenThreads = new ProducerThread[10];
        for (int i = 0; i < streamOpenThreads.length; ++i) {
            streamOpenThreads[i] =
                    createProducerThread(closeableRegistry, unclosedCounter, maxStreams);
        }
    }

    protected void startThreads() {
        for (ProducerThread t : streamOpenThreads) {
            t.start();
        }
    }

    protected void joinThreads() throws InterruptedException {
        for (Thread t : streamOpenThreads) {
            t.join();
        }
    }

    @Test
    public void testClose() throws Exception {
        setup(Integer.MAX_VALUE);
        startThreads();
        for (int i = 0; i < 5; ++i) {
            System.gc();
            Thread.sleep(40);
        }
        closeableRegistry.close();
        joinThreads();
        assertThat(unclosedCounter.get()).isEqualTo(0);
        assertThat(closeableRegistry.getNumberOfRegisteredCloseables()).isEqualTo(0);
        final TestCloseable testCloseable = new TestCloseable();
        try {
            registerCloseable(testCloseable);
            fail("Closed registry should not accept closeables!");
        } catch (IOException expected) {
        }
        assertThat(testCloseable.isClosed()).isTrue();
        assertThat(unclosedCounter.get()).isEqualTo(0);
        assertThat(closeableRegistry.getNumberOfRegisteredCloseables()).isEqualTo(0);
    }

    @Test
    void testNonBlockingClose() throws Exception {
        setup(Integer.MAX_VALUE);
        final BlockingTestCloseable blockingCloseable = new BlockingTestCloseable();
        registerCloseable(blockingCloseable);
        assertThat(closeableRegistry.getNumberOfRegisteredCloseables()).isEqualTo(1);
        Thread closer =
                new Thread(
                        () -> {
                            try {
                                closeableRegistry.close();
                            } catch (IOException ignore) {
                            }
                        });
        closer.start();
        blockingCloseable.awaitClose(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        final TestCloseable testCloseable = new TestCloseable();
        try {
            registerCloseable(testCloseable);
            fail("Closed registry should not accept closeables!");
        } catch (IOException ignored) {
        }
        blockingCloseable.unblockClose();
        closer.join();
        assertThat(testCloseable.isClosed()).isTrue();
        assertThat(closeableRegistry.getNumberOfRegisteredCloseables()).isEqualTo(0);
    }

    /** A testing producer. */
    protected abstract static class ProducerThread<C extends Closeable, E extends C, T>
            extends Thread {
        protected final AbstractAutoCloseableRegistry<C, E, T, IOException> registry;
        protected final AtomicInteger refCount;
        protected final int maxStreams;
        protected int numStreams;

        public ProducerThread(
                AbstractAutoCloseableRegistry<C, E, T, IOException> registry,
                AtomicInteger refCount,
                int maxStreams) {
            this.registry = registry;
            this.refCount = refCount;
            this.maxStreams = maxStreams;
            this.numStreams = 0;
        }

        protected abstract void createAndRegisterStream() throws IOException;

        @Override
        public void run() {
            try {
                while (numStreams < maxStreams) {
                    createAndRegisterStream();
                    try {
                        Thread.sleep(0);
                    } catch (InterruptedException ignored) {
                    }
                    if (maxStreams != Integer.MAX_VALUE) {
                        ++numStreams;
                    }
                }
            } catch (Exception ex) {
                // ignored
            }
        }
    }

    /** Testing stream which adds itself to a reference counter while not closed. */
    protected static final class TestStream extends InputStream {

        protected AtomicInteger refCount;

        public TestStream(AtomicInteger refCount) {
            this.refCount = refCount;
            refCount.incrementAndGet();
        }

        @Override
        public int read() throws IOException {
            return 0;
        }

        @Override
        public synchronized void close() throws IOException {
            refCount.decrementAndGet();
        }
    }

    /** A noop {@link Closeable} implementation that blocks inside {@link #close()}. */
    private static class BlockingTestCloseable implements Closeable {
        private final CountDownLatch closeCalledLatch = new CountDownLatch(1);
        private final CountDownLatch blockCloseLatch = new CountDownLatch(1);

        @Override
        public void close() {
            closeCalledLatch.countDown();
            try {
                blockCloseLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        /** Unblocks {@link #close()}. */
        public void unblockClose() {
            blockCloseLatch.countDown();
        }

        /** Causes the current thread to wait until {@link #close()} is called. */
        public void awaitClose(final long timeout, final TimeUnit timeUnit)
                throws InterruptedException {
            assertThat(closeCalledLatch.await(timeout, timeUnit)).isTrue();
        }
    }

    /** A noop {@link Closeable} implementation that tracks whether it was closed. */
    private static class TestCloseable implements Closeable {
        private final AtomicBoolean closed = new AtomicBoolean();

        @Override
        public void close() {
            assertThat(closed.compareAndSet(false, true))
                    .withFailMessage("TestCloseable was already closed")
                    .isTrue();
        }

        public boolean isClosed() {
            return closed.get();
        }
    }
}
