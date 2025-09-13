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

package org.apache.fluss.client.lookup.poller;

import org.apache.fluss.row.InternalRow;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;

/**
 * Periodically polls the complete current state of a small primary-key table
 * using the server-side full-scan RPC, and emits merged snapshots to subscribers.
 *
 * <p>Usage:
 * <pre>{@code
 * FullTablePoller poller = table.newLookup().createFullTableValuesPoller(Duration.ofSeconds(5));
 * poller.subscribe(rows -> render(rows));
 * poller.start();
 * // ...
 * poller.close();
 * }</pre>
 */
public interface FullTablePoller extends Closeable {

    /** Start periodic polling. Idempotent. */
    void start();

    /** Stop periodic polling; can be {@link #start()} again. */
    void stop();

    /**
     * Subscribe to full-table snapshots (values-only rows). The consumer will be invoked
     * once per completed poll with the merged list of rows across all buckets.
     */
    void subscribe(Consumer<List<InternalRow>> listener);

    /** Subscribe to polling errors (e.g., threshold exceeded). */
    void subscribeErrors(Consumer<Throwable> errorListener);

    /** Latest successfully produced snapshot, or null if none yet. */
    @Nullable
    List<InternalRow> latest();

    /** Returns the configured polling period. */
    Duration period();

    @Override
    void close();
}
