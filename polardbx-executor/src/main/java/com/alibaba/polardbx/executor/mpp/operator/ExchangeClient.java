/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.mpp.operator;

import com.alibaba.polardbx.common.exception.MemoryNotEnoughException;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.mpp.execution.SystemMemoryUsageListener;
import com.alibaba.polardbx.executor.mpp.execution.buffer.ChunkCompression;
import com.alibaba.polardbx.executor.mpp.execution.buffer.SerializedChunk;
import com.alibaba.polardbx.executor.mpp.metadata.TaskLocation;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.memory.MemorySetting;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.http.client.HttpClient;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.alibaba.polardbx.executor.mpp.Threads.ENABLE_WISP;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class ExchangeClient implements Closeable, IExchangeClient {

    private static final Logger log = LoggerFactory.getLogger(ExchangeClient.class);

    private static final SerializedChunk
        NO_MORE_PAGES = new SerializedChunk(EMPTY_SLICE, ChunkCompression.UNCOMPRESSED, 0, 0);

    private final long maxBufferedBytes;
    private final DataSize maxResponseSize;
    private final int concurrentRequestMultiplier;
    private final long minErrorDuration;
    private final long maxErrorDuration;
    private final HttpClient httpClient;
    private final ScheduledExecutorService executor;

    private final boolean preferLocalExchange;
    private final boolean supportSpill;

    @GuardedBy("this")
    private final Set<TaskLocation> locations = new HashSet<>();

    @GuardedBy("this")
    private AtomicBoolean noMoreLocations = new AtomicBoolean(false);

    private final ConcurrentMap<TaskLocation, HttpPageBufferClient> allClients = new ConcurrentHashMap<>();

    @GuardedBy("this")
    private final Deque<HttpPageBufferClient> queuedClients = new LinkedList<>();

    private final Set<HttpPageBufferClient> completedClients = newConcurrentHashSet();
    private final LinkedBlockingDeque<SerializedChunk> pageBuffer = new LinkedBlockingDeque<>();

    @GuardedBy("this")
    private final List<SettableFuture<?>> blockedCallers = new ArrayList<>();

    @GuardedBy("this")
    private long bufferBytes;
    @GuardedBy("this")
    private long successfulRequests;
    @GuardedBy("this")
    private long averageBytesPerRequest;

    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

//    private AtomicLong extBytes = new AtomicLong(0);

    private final SystemMemoryUsageListener systemMemoryUsageListener;

    public ExchangeClient(ExecutionContext executionContext,
                          DataSize maxResponseSize,
                          int concurrentRequestMultiplier,
                          long minErrorDuration,
                          long maxErrorDuration,
                          HttpClient httpClient,
                          ScheduledExecutorService executor,
                          SystemMemoryUsageListener systemMemoryUsageListener) {
        this.maxResponseSize = maxResponseSize;
        this.concurrentRequestMultiplier = concurrentRequestMultiplier;
        this.minErrorDuration = minErrorDuration;
        this.maxErrorDuration = maxErrorDuration;
        this.httpClient = httpClient;
        this.executor = executor;
        this.systemMemoryUsageListener = systemMemoryUsageListener;
        this.preferLocalExchange =
            executionContext.getParamManager().getBoolean(ConnectionParams.MPP_RPC_LOCAL_ENABLED);
        this.supportSpill =
            MemorySetting.ENABLE_SPILL && executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_SPILL);
        this.maxBufferedBytes = executionContext.getParamManager().getLong(ConnectionParams.MPP_OUTPUT_MAX_BUFFER_SIZE);
    }

    @Override
    public synchronized void addLocation(TaskLocation location) {
        requireNonNull(location, "location is null");
        if (closed.get()) {
            return;
        }

        if (locations.contains(location)) {
            return;
        }
        if (log.isDebugEnabled()) {
            log.debug("addLocation:" + location);
        }
        checkState(!noMoreLocations.get(), "No more locations already set");
        locations.add(location);
        scheduleRequestIfNecessary();
    }

    @Override
    public synchronized void addLocations(List<TaskLocation> taskLocations) {
        if (closed.get()) {
            return;
        }
        boolean update = false;
        for (TaskLocation location : taskLocations) {
            requireNonNull(location, "location is null");
            if (locations.contains(location)) {
                continue;
            } else {
                update = true;
                checkState(!noMoreLocations.get(), "No more locations already set");
                locations.add(location);
            }
        }
        if (update) {
            scheduleRequestIfNecessary();
        }
    }

    @Override
    public synchronized void noMoreLocations() {
        noMoreLocations.set(true);
        scheduleRequestIfNecessary();
    }

    @Override
    public boolean isNoMoreLocations() {
        return noMoreLocations.get();
    }

    @Override
    @Nullable
    public SerializedChunk pollPage() {
        if (!ENABLE_WISP) {
            checkState(!Thread.holdsLock(this), "Can not get next page while holding a lock on this");
        }

        throwIfFailed();

        if (closed.get()) {
            return null;
        }

        SerializedChunk page = pageBuffer.poll();
        return postProcessPage(page);
    }

    public WorkProcessor<SerializedChunk> pages() {
        return WorkProcessor.create(() -> {
            SerializedChunk page = pollPage();
            if (page == null) {
                if (isFinished()) {
                    return WorkProcessor.ProcessState.finished();
                }

                ListenableFuture<?> blocked = isBlocked();
                if (!blocked.isDone()) {
                    return WorkProcessor.ProcessState.blocked(blocked);
                }

                return WorkProcessor.ProcessState.yield();
            }

            return WorkProcessor.ProcessState.ofResult(page);
        });
    }

    @Override
    @Nullable
    public SerializedChunk getNextPageForDagWithDataDivide(Duration maxWaitTime)
        throws InterruptedException {
        if (!ENABLE_WISP) {
            checkState(!Thread.holdsLock(this), "Can not get next page while holding a lock on this");
        }

        throwIfFailed();

        if (closed.get()) {
            return null;
        }

        scheduleRequestIfNecessaryForDagWithDataDivide();

        SerializedChunk page = pageBuffer.poll();
        // only wait for a page if we have remote clients
        if (page == null && maxWaitTime.toMillis() >= 1 && !allClients.isEmpty()) {
            page = pageBuffer.poll(maxWaitTime.toMillis(), TimeUnit.MILLISECONDS);
        }

        return postProcessPage(page);
    }

    public synchronized void scheduleRequestIfNecessaryForDagWithDataDivide() {
        if (isFinished() || isFailed()) {
            return;
        }

        boolean complete = true;
        for (HttpPageBufferClient client : allClients.values()) {
            if (!client.isCompleted()) {
                complete = false;
                break;
            }
        }

        // if finished, add the end marker
        if (noMoreLocations.get() && complete) {
            if (pageBuffer.peekLast() != NO_MORE_PAGES) {
                checkState(pageBuffer.add(NO_MORE_PAGES), "Could not add no more pages marker");
            }
            if (pageBuffer.peek() == NO_MORE_PAGES) {
                close();
            }
            notifyBlockedCallers();
            return;
        }

        // add clients for new locations
        for (TaskLocation location : locations) {
            if (!allClients.containsKey(location)) {
                HttpPageBufferClient client = new HttpPageBufferClient(
                    httpClient,
                    maxResponseSize,
                    minErrorDuration,
                    maxErrorDuration,
                    location,
                    new ExchangeClientCallback(),
                    executor,
                    preferLocalExchange);
                allClients.put(location, client);
                queuedClients.add(client);
            }
        }

        long neededBytes = maxBufferedBytes - bufferBytes;
        if (neededBytes <= 0) {
            return;
        }

        int clientCount = (int) ((1.0 * neededBytes / averageBytesPerRequest) * concurrentRequestMultiplier);
        clientCount = Math.max(clientCount, 1);

        int pendingClients = allClients.size() - queuedClients.size() - completedClients.size();
        clientCount -= pendingClients;

        for (int i = 0; i < clientCount; i++) {
            HttpPageBufferClient client = queuedClients.poll();
            if (client == null) {
                // no more clients available
                return;
            }
            client.scheduleRequest();
        }
    }

    private SerializedChunk postProcessPage(SerializedChunk page) {
        if (!ENABLE_WISP) {
            checkState(!Thread.holdsLock(this), "Can not get next page while holding a lock on this");
        }

        if (page == null) {
            return null;
        }

        if (page == NO_MORE_PAGES) {
            // mark client closed
            close();

            // add end marker back to queue
            //checkState(pageBuffer.add(NO_MORE_PAGES), "Could not add no more pages marker");
            notifyBlockedCallers();

            // don't return end of stream marker
            return null;
        }

        synchronized (this) {
            if (!closed.get()) {
                int pageRetainedSize = page.getRetainedSizeInBytes();
                bufferBytes -= pageRetainedSize;
                systemMemoryUsageListener.updateSystemMemoryUsage(-pageRetainedSize);
                if (pageBuffer.peek() == NO_MORE_PAGES) {
                    close();
                }
            }
        }
        scheduleRequestIfNecessary();
        return page;
    }

    @Override
    public boolean isFinished() {
        throwIfFailed();
        return isClosed() && completedClients.size() == locations.size();
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public synchronized void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        for (HttpPageBufferClient client : allClients.values()) {
            closeQuietly(client);
        }
        pageBuffer.clear();
        systemMemoryUsageListener.updateSystemMemoryUsage(-bufferBytes);
        bufferBytes = 0;
        if (pageBuffer.peekLast() != NO_MORE_PAGES) {
            checkState(pageBuffer.add(NO_MORE_PAGES), "Could not add no more pages marker");
        }
        notifyBlockedCallers();
    }

    public synchronized void scheduleRequestIfNecessary() {
        if (isFinished() || isFailed()) {
            return;
        }

        // if finished, add the end marker
        if (noMoreLocations.get() && completedClients.size() == locations.size()) {
            if (pageBuffer.peekLast() != NO_MORE_PAGES) {
                checkState(pageBuffer.add(NO_MORE_PAGES), "Could not add no more pages marker");
            }
            if (pageBuffer.peek() == NO_MORE_PAGES) {
                close();
            }
            notifyBlockedCallers();
            return;
        }

        // add clients for new locations
        for (TaskLocation location : locations) {
            if (!allClients.containsKey(location)) {
                HttpPageBufferClient client = new HttpPageBufferClient(
                    httpClient,
                    maxResponseSize,
                    minErrorDuration,
                    maxErrorDuration,
                    location,
                    new ExchangeClientCallback(),
                    executor,
                    preferLocalExchange);
                allClients.put(location, client);
                queuedClients.add(client);
            }
        }

        long neededBytes = maxBufferedBytes - bufferBytes;
        if (neededBytes <= 0) {
            return;
        }

        int clientCount = (int) ((1.0 * neededBytes / averageBytesPerRequest) * concurrentRequestMultiplier);
        clientCount = Math.max(clientCount, 1);

        int pendingClients = allClients.size() - queuedClients.size() - completedClients.size();
        clientCount -= pendingClients;

        for (int i = 0; i < clientCount; i++) {
            HttpPageBufferClient client = queuedClients.poll();
            if (client == null) {
                // no more clients available
                return;
            }
            client.scheduleRequest();
        }
    }

    @Override
    public synchronized ListenableFuture<?> isBlocked() {
        if (isClosed() || isFailed() || pageBuffer.peek() != null) {
            return Futures.immediateFuture(true);
        }
        SettableFuture<?> future = SettableFuture.create();
        blockedCallers.add(future);
        return future;
    }

    private synchronized boolean addPages(List<SerializedChunk> pages) {
        if (isClosed() || isFailed()) {
            return false;
        }

        long memorySize = 0L;
        long responseSize = 0L;
        if (!pages.isEmpty()) {
            for (int i = 0; i < pages.size(); i++) {
                SerializedChunk page = pages.get(i);
                memorySize += page.getRetainedSizeInBytes();
                responseSize += page.getSizeInBytes();
            }

            try {
                systemMemoryUsageListener.updateSystemMemoryUsage(memorySize);
            } catch (MemoryNotEnoughException e) {
                notifyBlockedCallers();
                throw e;
            }
            pageBuffer.addAll(pages);

            // notify all blocked callers
            notifyBlockedCallers();
        }

        bufferBytes += memorySize;
        successfulRequests++;

        // AVG_n = AVG_(n-1) * (n-1)/n + VALUE_n / n
        averageBytesPerRequest = (long) (1.0 * averageBytesPerRequest * (successfulRequests - 1) / successfulRequests
            + responseSize / successfulRequests);
        return true;
    }

    private synchronized void notifyBlockedCallers() {
        for (int i = 0; i < blockedCallers.size(); i++) {
            SettableFuture<?> blockedCaller = blockedCallers.get(i);
            blockedCaller.set(null);
        }
        blockedCallers.clear();
    }

    private synchronized void requestComplete(HttpPageBufferClient client) {
        if (!queuedClients.contains(client)) {
            queuedClients.add(client);
        }
        scheduleRequestIfNecessary();
    }

    private synchronized void clientFinished(HttpPageBufferClient client) {
        requireNonNull(client, "client is null");
        completedClients.add(client);
        scheduleRequestIfNecessary();
    }

    private synchronized void clientFailed(Throwable cause) {
        // TODO: properly handle the failed vs closed state
        // it is important not to treat failures as a successful close
        if (!isClosed()) {
            failure.compareAndSet(null, cause);
            notifyBlockedCallers();
        }
    }

    private boolean isFailed() {
        return failure.get() != null;
    }

    private void throwIfFailed() {
        Throwable t = failure.get();
        if (t != null) {
            throw Throwables.propagate(t);
        }
    }

    private class ExchangeClientCallback
        implements HttpPageBufferClient.ClientCallback {
        @Override
        public boolean addPages(HttpPageBufferClient client, List<SerializedChunk> pages) {
            requireNonNull(client, "client is null");
            requireNonNull(pages, "pages is null");
            return ExchangeClient.this.addPages(pages);
        }

        @Override
        public void requestComplete(HttpPageBufferClient client) {
            requireNonNull(client, "client is null");
            ExchangeClient.this.requestComplete(client);
        }

        @Override
        public void clientFinished(HttpPageBufferClient client) {
            ExchangeClient.this.clientFinished(client);
        }

        @Override
        public void clientFailed(HttpPageBufferClient client, Throwable cause) {
            requireNonNull(client, "client is null");
            requireNonNull(cause, "cause is null");
            ExchangeClient.this.clientFailed(cause);
        }
    }

    private static void closeQuietly(HttpPageBufferClient client) {
        try {
            client.close();
        } catch (RuntimeException e) {
            // ignored
        }
    }
}
