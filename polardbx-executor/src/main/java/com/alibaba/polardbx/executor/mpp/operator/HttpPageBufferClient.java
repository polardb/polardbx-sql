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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.mpp.OutputBuffers;
import com.alibaba.polardbx.executor.mpp.Threads;
import com.alibaba.polardbx.executor.mpp.client.MppMediaTypes;
import com.alibaba.polardbx.executor.mpp.deploy.MppServer;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.mpp.execution.TaskManager;
import com.alibaba.polardbx.executor.mpp.execution.buffer.BufferResult;
import com.alibaba.polardbx.executor.mpp.execution.buffer.SerializedChunk;
import com.alibaba.polardbx.executor.mpp.metadata.TaskLocation;
import com.alibaba.polardbx.executor.mpp.server.TaskResource;
import com.alibaba.polardbx.executor.mpp.server.remotetask.Backoff;
import com.alibaba.polardbx.executor.mpp.util.Failures;
import com.alibaba.polardbx.executor.mpp.util.MillTicker;
import com.alibaba.polardbx.gms.node.HostAddressCache;
import com.google.common.base.Throwables;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.net.MediaType;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.JdkFutureAdapters;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.ResponseTooLargeException;
import io.airlift.http.client.StatusResponseHandler;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.SliceInput;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_REMOTE_BUFFER;
import static com.alibaba.polardbx.executor.mpp.client.MppMediaTypes.MPP_BUFFER_COMPLETE;
import static com.alibaba.polardbx.executor.mpp.client.MppMediaTypes.MPP_MAX_SIZE;
import static com.alibaba.polardbx.executor.mpp.client.MppMediaTypes.MPP_PAGE_NEXT_TOKEN;
import static com.alibaba.polardbx.executor.mpp.client.MppMediaTypes.MPP_PAGE_TOKEN;
import static com.alibaba.polardbx.executor.mpp.client.MppMediaTypes.MPP_TASK_INSTANCE_ID;
import static com.alibaba.polardbx.executor.mpp.execution.buffer.PagesSerdeUtil.readSerializedChunks;
import static com.alibaba.polardbx.executor.mpp.operator.HttpPageBufferClient.PagesResponse.createEmptyPagesResponse;
import static com.alibaba.polardbx.executor.mpp.operator.HttpPageBufferClient.PagesResponse.createPagesResponse;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addTimeout;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.ResponseHandlerUtils.propagate;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@ThreadSafe
public final class HttpPageBufferClient
    implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(HttpPageBufferClient.class);

    /**
     * For each request, the addPage method will be called zero or more times,
     * followed by either requestComplete or clientFinished (if buffer complete).  If the client is
     * closed, requestComplete or bufferFinished may never be called.
     * <p/>
     * <b>NOTE:</b> Implementations of this interface are not allowed to perform
     * blocking operations.
     */
    public interface ClientCallback {
        boolean addPages(HttpPageBufferClient client, List<SerializedChunk> pages);

        void requestComplete(HttpPageBufferClient client);

        void clientFinished(HttpPageBufferClient client);

        void clientFailed(HttpPageBufferClient client, Throwable cause);
    }

    private final HttpClient httpClient;
    private final DataSize maxResponseSize;
    private final TaskLocation location;
    private final ClientCallback clientCallback;
    private final ScheduledExecutorService executor;
    private final Backoff backoff;
    private final boolean preferLocalExchange;

    @GuardedBy("this")
    private boolean closed;
    @GuardedBy("this")
    private ListenableFuture<?> future;
    @GuardedBy("this")
    private DateTime lastUpdate = DateTime.now();
    @GuardedBy("this")
    private long token;
    @GuardedBy("this")
    private boolean scheduled;
    @GuardedBy("this")
    private boolean completed;
    @GuardedBy("this")
    private String taskInstanceId;

    private static final AtomicLongFieldUpdater<HttpPageBufferClient> rowsReceivedUpdater =
        AtomicLongFieldUpdater.newUpdater(HttpPageBufferClient.class, "rowsReceivedLong");
    private static final AtomicIntegerFieldUpdater<HttpPageBufferClient> pagesReceivedUpdater =
        AtomicIntegerFieldUpdater.newUpdater(HttpPageBufferClient.class, "pagesReceivedInt");
    private static final AtomicLongFieldUpdater<HttpPageBufferClient> rowsRejectedUpdater =
        AtomicLongFieldUpdater.newUpdater(HttpPageBufferClient.class, "rowsRejectedLong");
    private static final AtomicIntegerFieldUpdater<HttpPageBufferClient> requestsScheduledUpdater =
        AtomicIntegerFieldUpdater.newUpdater(HttpPageBufferClient.class, "requestsScheduledInt");
    private static final AtomicIntegerFieldUpdater<HttpPageBufferClient> requestsCompletedUpdater =
        AtomicIntegerFieldUpdater.newUpdater(HttpPageBufferClient.class, "requestsCompletedInt");
    private static final AtomicIntegerFieldUpdater<HttpPageBufferClient> requestsFailedUpdater =
        AtomicIntegerFieldUpdater.newUpdater(HttpPageBufferClient.class, "requestsFailedInt");

    private volatile long rowsReceivedLong = 0L;
    private volatile int pagesReceivedInt = 0;
    private volatile long rowsRejectedLong = 0L;
    private volatile int requestsScheduledInt = 0;
    private volatile int requestsCompletedInt = 0;
    private volatile int requestsFailedInt = 0;

    public HttpPageBufferClient(
        HttpClient httpClient,
        DataSize maxResponseSize,
        long minErrorDuration,
        long maxErrorDuration,
        TaskLocation location,
        ClientCallback clientCallback,
        ScheduledExecutorService executor,
        boolean preferLocalExchange) {
        this(httpClient, maxResponseSize,
            minErrorDuration, maxErrorDuration, location,
            clientCallback, executor, preferLocalExchange, MillTicker.systemTicker());
    }

    public HttpPageBufferClient(
        HttpClient httpClient,
        DataSize maxResponseSize,
        long minErrorDuration,
        long maxErrorDuration,
        TaskLocation location,
        ClientCallback clientCallback,
        ScheduledExecutorService executor,
        boolean preferLocalExchange,
        Ticker ticker) {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.maxResponseSize = requireNonNull(maxResponseSize, "maxResponseSize is null");
        this.location = requireNonNull(location, "location is null");
        this.clientCallback = requireNonNull(clientCallback, "clientCallback is null");
        this.executor = requireNonNull(executor, "executor is null");
        requireNonNull(minErrorDuration, "minErrorDuration is null");
        requireNonNull(maxErrorDuration, "maxErrorDuration is null");
        requireNonNull(ticker, "ticker is null");
        this.backoff = new Backoff(
            minErrorDuration,
            maxErrorDuration,
            ticker,
            0, 50, 100, 200);
        this.preferLocalExchange = preferLocalExchange;
    }

    public synchronized PageBufferClientStatus getStatus() {
        String state;
        if (closed) {
            state = "closed";
        } else if (future != null) {
            state = "running";
        } else if (scheduled) {
            state = "scheduled";
        } else if (completed) {
            state = "completed";
        } else {
            state = "queued";
        }
        String httpRequestState = "not scheduled";
        if (future != null) {
            if (future instanceof HttpClient.HttpResponseFuture) {
                httpRequestState = ((HttpClient.HttpResponseFuture) future).getState();
            } else {
                httpRequestState = "not http request";
            }
        }

        long rejectedRows = rowsRejectedUpdater.get(this);
        int rejectedPages = pagesReceivedUpdater.get(this);

        return new PageBufferClientStatus(
            location,
            state,
            lastUpdate,
            rowsReceivedUpdater.get(this),
            pagesReceivedUpdater.get(this),
            rejectedRows == 0 ? OptionalLong.empty() : OptionalLong.of(rejectedRows),
            rejectedPages == 0 ? OptionalInt.empty() : OptionalInt.of(rejectedPages),
            requestsScheduledUpdater.get(this),
            requestsCompletedUpdater.get(this),
            requestsFailedUpdater.get(this),
            httpRequestState);
    }

    public synchronized boolean isRunning() {
        return future != null;
    }

    public synchronized boolean isCompleted() {
        return closed || completed;
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        boolean shouldSendDelete;
        Future<?> future;
        synchronized (this) {
            shouldSendDelete = !closed;

            closed = true;

            future = this.future;

            this.future = null;

            lastUpdate = DateTime.now();
        }

        if (future != null && !future.isDone()) {
            future.cancel(true);
        }

        // abort the output buffer on the remote node; response of delete is ignored
        if (shouldSendDelete) {
            sendDelete();
        }
    }

    public synchronized void scheduleRequest() {
        if (closed || (future != null) || scheduled) {
            return;
        }
        scheduled = true;

        // start before scheduling to include error delay
        backoff.startRequest();

        long delayMills = backoff.getBackoffDelayMills();
        executor.schedule(() -> {
            try {
                initiateRequest();
            } catch (Throwable t) {
                // should not happen, but be safe and fail the operator
                clientCallback.clientFailed(HttpPageBufferClient.this, t);
            }
        }, delayMills, MILLISECONDS);

        lastUpdate = DateTime.now();
        requestsScheduledUpdater.incrementAndGet(this);
    }

    private synchronized void initiateRequest() {
        scheduled = false;
        if (closed || (future != null)) {
            return;
        }

        if (completed) {
            sendDelete();
        } else {
            sendGetResults();
        }

        lastUpdate = DateTime.now();
    }

    private synchronized void sendGetResults() {
        URI uri = HttpUriBuilder.uriBuilderFrom(location.getUri()).appendPath(String.valueOf(token)).build();
        //final HttpClient.HttpResponseFuture<PagesResponse> resultFuture;
        final ListenableFuture<PagesResponse> resultFuture;

        if (log.isDebugEnabled()) {
            log.debug("start to sendGetResults uri[" + uri.toString() + "] , isLocal=" + location.isLocal()
                + ", preferLocalExchange=" + preferLocalExchange);
        }

        if (preferLocalExchange && location.isLocal()) {
            TaskManager taskManager = ((MppServer) ServiceProvider.getInstance().getServer()).getTaskManager();

            ListenableFuture<BufferResult> bufferResultFuture = taskManager
                .getTaskResults(location.getTaskId(), true, new OutputBuffers.OutputBufferId(location.getBufferId()),
                    token, maxResponseSize);
            Duration waitTime = TaskResource.randomizeWaitTime(TaskResource.DEFAULT_MAX_WAIT_TIME);
            bufferResultFuture = addTimeout(
                bufferResultFuture,
                () -> BufferResult.emptyResults(location.getTaskId().toString(), token, false),
                waitTime,
                executor);

            resultFuture = Futures.transform(bufferResultFuture, result -> {
                PagesResponse response = PagesResponse.createPagesResponse(result.getTaskInstanceId(),
                    result.getToken(), result.getNextToken(), result.getSerializedPages(), result.isBufferComplete()
                );
                return response;
            }, directExecutor());

        } else {
            //System.out.println("getRemoteResult:preferLocalExchange="+preferLocalExchange+",location="+location);
            resultFuture = httpClient.executeAsync(
                prepareGet()
                    .setHeader(MPP_MAX_SIZE, maxResponseSize.toString())
                    .setUri(uri).build(),
                new PageResponseHandler());
        }
        future = resultFuture;
        Futures.addCallback(resultFuture, new FutureCallback<PagesResponse>() {
            @Override
            public void onSuccess(PagesResponse result) {
                checkNotHoldsLock();

                backoff.success();

                List<SerializedChunk> pages;
                try {
                    synchronized (HttpPageBufferClient.this) {
                        if (taskInstanceId == null) {
                            taskInstanceId = result.getTaskInstanceId();
                        }

                        if (!isNullOrEmpty(taskInstanceId) && !result.getTaskInstanceId().equals(taskInstanceId)) {
                            // TODO: update error message
                            throw new TddlRuntimeException(ErrorCode.ERR_REMOTE_TASK, String.format("%s (%s)",
                                "remote task instance mismatch", HostAddressCache.fromUri(uri)));
                        }

                        if (result.getToken() == token) {
                            pages = result.getPages();
                            token = result.getNextToken();
                        } else {
                            pages = ImmutableList.of();
                        }
                    }
                } catch (TddlRuntimeException e) {
                    handleFailure(e, resultFuture);
                    return;
                }

                if (log.isDebugEnabled()) {
                    log.debug("get Result callback result[" + result.getTaskInstanceId()
                        + "|" + result.getNextToken()
                        + "|" + result.getToken()
                        + "|" + result.isClientComplete()
                        + "|" + result.getPages().size() + "]");
                }

                try {
                    // add pages
                    if (clientCallback.addPages(HttpPageBufferClient.this, pages)) {
                        pagesReceivedUpdater.addAndGet(HttpPageBufferClient.this, pages.size());
                        rowsReceivedUpdater.addAndGet(HttpPageBufferClient.this, countPagesPositionCount(pages));
                    } else {
                        pagesReceivedUpdater.addAndGet(HttpPageBufferClient.this, pages.size());
                        rowsRejectedUpdater.addAndGet(HttpPageBufferClient.this, countPagesPositionCount(pages));
                    }
                } catch (Throwable t) {
                    // clientCallback.addPages will allocate memory, it maybe throw a exception
                    clientCallback.clientFailed(HttpPageBufferClient.this, t);
                    return;
                }

                synchronized (HttpPageBufferClient.this) {
                    // client is complete, acknowledge it by sending it a delete in the next request
                    if (result.isClientComplete()) {
                        completed = true;
                    }
                    if (future == resultFuture) {
                        future = null;
                    }
                    lastUpdate = DateTime.now();
                }
                requestsCompletedUpdater.incrementAndGet(HttpPageBufferClient.this);
                clientCallback.requestComplete(HttpPageBufferClient.this);
            }

            @Override
            public void onFailure(Throwable t) {
                log.debug(String.format("Request to %s failed %s", uri, t), t);

                checkNotHoldsLock();

                t = rewriteException(t);
                if (!(t instanceof TddlRuntimeException) && backoff.failure()) {
                    log.error("sendGetResults backoff failure, stop retry", t);
                    String message = format("%s (%s - %s failures, time since last success %s)",
                        Failures.WORKER_NODE_ERROR,
                        uri,
                        backoff.getFailureCount(),
                        backoff.getTimeSinceLastSuccess().convertTo(SECONDS));
                    t = new PageTransportTimeoutException(message, t);
                }
                handleFailure(t, resultFuture);
            }
        }, executor);
    }

    private long countPagesPositionCount(List<SerializedChunk> pages) {
        long sum = 0L;
        for (int i = 0; i < pages.size(); i++) {
            sum += pages.get(i).getPositionCount();
        }
        return sum;
    }

    private synchronized void sendDelete() {

        ListenableFuture resultFuture;

        if (log.isDebugEnabled()) {
            log.debug("start to sendDelete uri[" + location.toString() + "]");
        }
        if (preferLocalExchange && location.isLocal()) {
            TaskManager taskManager = ((MppServer) ServiceProvider.getInstance().getServer()).getTaskManager();
            Future<StatusResponseHandler.StatusResponse> taskInfoFuture = executor.submit(
                new Callable<StatusResponseHandler.StatusResponse>() {
                    @Override
                    public StatusResponseHandler.StatusResponse call() {
                        taskManager.abortTaskResults(location.getTaskId(),
                            new OutputBuffers.OutputBufferId(location.getBufferId()));
                        return new StatusResponseHandler.StatusResponse(
                            HttpStatus.OK.code(), ImmutableListMultimap.of());
                    }
                });
            resultFuture = JdkFutureAdapters.listenInPoolThread(taskInfoFuture, executor);

        } else {
            resultFuture = httpClient
                .executeAsync(prepareDelete().setUri(location.getUri()).build(), createStatusResponseHandler());
        }
        future = resultFuture;
        Futures.addCallback(resultFuture, new FutureCallback<StatusResponseHandler.StatusResponse>() {
            @Override
            public void onSuccess(@Nullable StatusResponseHandler.StatusResponse result) {
                checkNotHoldsLock();
                backoff.success();
                if (log.isDebugEnabled()) {
                    log.debug("succ delete " + location.toString());
                }
                synchronized (HttpPageBufferClient.this) {
                    closed = true;
                    if (future == resultFuture) {
                        future = null;
                    }
                    lastUpdate = DateTime.now();
                }
                requestsCompletedUpdater.incrementAndGet(HttpPageBufferClient.this);
                clientCallback.clientFinished(HttpPageBufferClient.this);
            }

            @Override
            public void onFailure(Throwable t) {
                checkNotHoldsLock();

                log.error(String.format("Request to delete %s failed %s", location, t), t);
                if (!(t instanceof TddlRuntimeException) && backoff.failure()) {
                    log.error("sendDelete backoff failure, stop retry", t);
                    String message =
                        format("Error closing remote buffer (%s - %s failures, time since last success %s)",
                            location,
                            backoff.getFailureCount(),
                            backoff.getTimeSinceLastSuccess().convertTo(SECONDS));
                    t = new TddlRuntimeException(ERR_REMOTE_BUFFER, t, message);
                }
                handleFailure(t, resultFuture);
            }
        }, executor);
    }

    private void checkNotHoldsLock() {
        if (!Threads.ENABLE_WISP && Thread.holdsLock(HttpPageBufferClient.this)) {
            log.error("Can not handle callback while holding a lock on this");
        }
    }

    private void handleFailure(Throwable t, ListenableFuture<?> expectedFuture) {
        // Can not delegate to other callback while holding a lock on this
        checkNotHoldsLock();

        requestsFailedUpdater.incrementAndGet(this);
        requestsCompletedUpdater.incrementAndGet(this);

        if (t instanceof TddlRuntimeException) {
            clientCallback.clientFailed(HttpPageBufferClient.this, t);
        }

        synchronized (HttpPageBufferClient.this) {
            if (future == expectedFuture) {
                future = null;
            }
            lastUpdate = DateTime.now();
        }
        clientCallback.requestComplete(HttpPageBufferClient.this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HttpPageBufferClient that = (HttpPageBufferClient) o;

        if (!location.equals(that.location)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return location.hashCode();
    }

    @Override
    public String toString() {
        String state;
        synchronized (this) {
            if (closed) {
                state = "CLOSED";
            } else if (future != null) {
                state = "RUNNING";
            } else {
                state = "QUEUED";
            }
        }
        return toStringHelper(this)
            .add("location", location)
            .addValue(state)
            .toString();
    }

    private static Throwable rewriteException(Throwable t) {
        if (t instanceof ResponseTooLargeException) {
            return new PageTooLargeException();
        }
        return t;
    }

    public static class PageResponseHandler
        implements ResponseHandler<PagesResponse, RuntimeException> {
        @Override
        public PagesResponse handleException(Request request, Exception exception) {
            throw propagate(request, exception);
        }

        @Override
        public PagesResponse handle(Request request, Response response) {
            try {
                // no content means no content was created within the wait period, but query is still ok
                // if job is finished, complete is set in the response
                if (response.getStatusCode() == HttpStatus.NO_CONTENT.code()) {
                    return createEmptyPagesResponse(getTaskInstanceId(response), getToken(response),
                        getNextToken(response), getComplete(response));
                }

                // otherwise we must have gotten an OK response, everything else is considered fatal
                if (response.getStatusCode() != HttpStatus.OK.code()) {
                    StringBuilder body = new StringBuilder();
                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(response.getInputStream()))) {
                        // Get up to 1000 lines for debugging
                        for (int i = 0; i < 1000; i++) {
                            String line = reader.readLine();
                            // Don't output more than 100KB
                            if (line == null || body.length() + line.length() > 100 * 1024) {
                                break;
                            }
                            body.append(line + "\n");
                        }
                    } catch (RuntimeException | IOException e) {
                        // Ignored. Just return whatever message we were able to decode
                    }
                    throw new PageTransportErrorException(
                        format("Expected response code to be 200, but was %s %s:%s", response.getStatusCode(),
                            body.toString()));
                }

                // invalid content type can happen when an error page is returned, but is unlikely given the above 200
                String contentType = response.getHeader(CONTENT_TYPE);
                if (contentType == null) {
                    throw new PageTransportErrorException(format("%s header is not set: %s", CONTENT_TYPE, response));
                }
                if (!mediaTypeMatches(contentType, MppMediaTypes.MPP_PAGES_TYPE)) {
                    throw new PageTransportErrorException(String
                        .format("Expected %s response from server but got %s", MppMediaTypes.MPP_PAGES_TYPE,
                            contentType));
                }

                String taskInstanceId = getTaskInstanceId(response);
                long token = getToken(response);
                long nextToken = getNextToken(response);
                boolean complete = getComplete(response);

                try (SliceInput input = new InputStreamSliceInput(response.getInputStream())) {
                    List<SerializedChunk> pages = ImmutableList.copyOf(readSerializedChunks(input));
                    return createPagesResponse(taskInstanceId, token, nextToken, pages, complete);
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            } catch (PageTransportErrorException e) {
                throw new PageTransportErrorException(
                    String.format("Error fetching %s: %s", request.getUri().toASCIIString(), e.getMessage()), e);
            }
        }

        private static String getTaskInstanceId(Response response) {
            String taskInstanceId = response.getHeader(MPP_TASK_INSTANCE_ID);
            if (taskInstanceId == null) {
                throw new PageTransportErrorException(format("Expected %s header", MPP_TASK_INSTANCE_ID));
            }
            return taskInstanceId;
        }

        private static long getToken(Response response) {
            String tokenHeader = response.getHeader(MPP_PAGE_TOKEN);
            if (tokenHeader == null) {
                throw new PageTransportErrorException(format("Expected %s header", MPP_PAGE_TOKEN));
            }
            return Long.parseLong(tokenHeader);
        }

        private static long getNextToken(Response response) {
            String nextTokenHeader = response.getHeader(MPP_PAGE_NEXT_TOKEN);
            if (nextTokenHeader == null) {
                throw new PageTransportErrorException(format("Expected %s header", MPP_PAGE_NEXT_TOKEN));
            }
            return Long.parseLong(nextTokenHeader);
        }

        private static boolean getComplete(Response response) {
            String bufferComplete = response.getHeader(MPP_BUFFER_COMPLETE);
            if (bufferComplete == null) {
                throw new PageTransportErrorException(format("Expected %s header", MPP_BUFFER_COMPLETE));
            }
            return Boolean.parseBoolean(bufferComplete);
        }

        private static boolean mediaTypeMatches(String value, MediaType range) {
            try {
                return MediaType.parse(value).is(range);
            } catch (IllegalArgumentException | IllegalStateException e) {
                return false;
            }
        }
    }

    public static class PagesResponse {
        public static PagesResponse createPagesResponse(String taskInstanceId, long token, long nextToken,
                                                        Iterable<SerializedChunk> pages, boolean complete) {
            return new PagesResponse(taskInstanceId, token, nextToken, pages, complete);
        }

        public static PagesResponse createEmptyPagesResponse(String taskInstanceId, long token, long nextToken,
                                                             boolean complete) {
            return new PagesResponse(taskInstanceId, token, nextToken, ImmutableList.of(), complete);
        }

        private final String taskInstanceId;
        private final long token;
        private final long nextToken;
        private final List<SerializedChunk> pages;
        private final boolean clientComplete;

        public PagesResponse(String taskInstanceId, long token, long nextToken, Iterable<SerializedChunk> pages,
                             boolean clientComplete) {
            this.taskInstanceId = taskInstanceId;
            this.token = token;
            this.nextToken = nextToken;
            this.pages = ImmutableList.copyOf(pages);
            this.clientComplete = clientComplete;
        }

        public long getToken() {
            return token;
        }

        public long getNextToken() {
            return nextToken;
        }

        public List<SerializedChunk> getPages() {
            return pages;
        }

        public boolean isClientComplete() {
            return clientComplete;
        }

        public String getTaskInstanceId() {
            return taskInstanceId;
        }

        @Override
        public String toString() {
            return toStringHelper(this)
                .add("token", token)
                .add("nextToken", nextToken)
                .add("pagesSize", pages.size())
                .add("clientComplete", clientComplete)
                .toString();
        }
    }
}