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

package com.alibaba.polardbx.executor.mpp.client;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.util.MoreObjects;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import io.airlift.http.client.FullJsonResponseHandler;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import org.apache.calcite.rel.RelNode;

import java.io.Closeable;
import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_DATA_OUTPUT;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public abstract class AbstractStatementClient implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(AbstractStatementClient.class);
    protected static final String USER_AGENT_VALUE = AbstractStatementClient.class.getSimpleName() + "/" +
        MoreObjects.firstNonNull(AbstractStatementClient.class.getPackage().getImplementationVersion(), "unknown");

    protected final String query;
    protected final AtomicReference<QueryResults> currentResults = new AtomicReference<>();
    protected final AtomicBoolean closed = new AtomicBoolean();
    protected final AtomicBoolean gone = new AtomicBoolean();
    protected final AtomicBoolean valid = new AtomicBoolean(true);
    protected final long requestTimeoutMills;
    protected String mppQueryId;
    protected long nextToken = -1;
    protected ExecutionContext clientContext;
    protected RelNode plan;

    public AbstractStatementClient(ExecutionContext executionContext, RelNode plan) {
        this.clientContext = requireNonNull(executionContext, "executionContext is null");
        requireNonNull(executionContext.getOriginSql(), "query is null");
        this.query = executionContext.getOriginSql();
        this.mppQueryId = executionContext.getTraceId();
        this.requestTimeoutMills = executionContext.getSocketTimeout();
        this.plan = plan;
    }

    public abstract void createQuery() throws Exception;

    public String getQuery() {
        return query;
    }

    public boolean isClosed() {
        return closed.get();
    }

    public boolean isGone() {
        return gone.get();
    }

    public boolean isFailed() {
        return currentResults.get().getError() != null;
    }

    public QueryResults current() {
        checkState(isValid(), "current position is not valid (cursor past end)");
        return currentResults.get();
    }

    public QueryResults finalResults() {
        checkState((!isValid()) || isFailed(), "current position is still valid");
        return currentResults.get();
    }

    public boolean isValid() {
        return valid.get() && (!isGone()) && (!isClosed());
    }

    private Request.Builder prepareRequest(Request.Builder builder, URI nextUri) {
        builder.setHeader(MppMediaTypes.MPP_USER, MppMediaTypes.MPP_POLARDBX);
        builder.setHeader(USER_AGENT, USER_AGENT_VALUE).setUri(nextUri);
        return builder;
    }

    public abstract Object getResults(URI nextUri)
        throws Exception;

    public boolean advance() {
        URI nextUri = current().getNextUri();
        if (isClosed() || (nextUri == null)) {
            valid.set(false);
            return false;
        }

        Request request = prepareRequest(prepareGet(), nextUri).build();

        Exception cause = null;
        long start = System.currentTimeMillis();
        long attempts = 0;

        do {
            // back-off on retry
            if (attempts > 0) {
                try {
                    MILLISECONDS.sleep(attempts * 100);
                } catch (InterruptedException e) {
                    try {
                        close();
                    } finally {
                        Thread.currentThread().interrupt();
                    }
                    throw new TddlRuntimeException(ERR_DATA_OUTPUT, "StatementClient thread was interrupted");
                }
            }
            attempts++;

            Object result;
            try {
                result = getResults(nextUri);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                cause = new RuntimeException(e);
                continue;
            }
            if (result instanceof LocalResultResponse) {
                LocalResultResponse r = (LocalResultResponse) result;
                if (r.getException() == null) {
                    processResponse(r);
                    return true;
                } else {
                    gone.set(true);
                    throw new TddlRuntimeException(ERR_DATA_OUTPUT,
                        "Error fetching next at " + request.getUri() + " returned: " + r.getResults() + " [Error: " + r
                            .getException() + "]");
                }
            } else if (result instanceof FullJsonResponseHandler.JsonResponse) {
                FullJsonResponseHandler.JsonResponse<QueryResults> response =
                    (FullJsonResponseHandler.JsonResponse<QueryResults>) result;
                if (response.getStatusCode() == HttpStatus.OK.code() && response.hasValue()) {
                    processResponse(response);
                    return true;
                }
                if (response.getStatusCode() != HttpStatus.SERVICE_UNAVAILABLE.code()) {
                    throw requestFailedException("fetching next", request, response);
                }
            }
        }
        while (((System.currentTimeMillis() - start) < requestTimeoutMills) && !isClosed());

        gone.set(true);
        throw new TddlRuntimeException(ERR_DATA_OUTPUT, cause, "Error fetching next");
    }

    protected void processResponse(LocalResultResponse response) {
        nextToken = response.getToken();
        currentResults.set(response.getResults());
    }

    private void processResponse(FullJsonResponseHandler.JsonResponse<QueryResults> response) {
        currentResults.set(response.getValue());
    }

    private RuntimeException requestFailedException(
        String task, Request request, FullJsonResponseHandler.JsonResponse<QueryResults> response) {
        gone.set(true);
        if (!response.hasValue()) {
            return new RuntimeException(
                format("Error %s at %s returned an invalid response: %s [Error: %s]", task, request.getUri(), response,
                    response.getResponseBody()),
                response.getException());
        }
        return new RuntimeException(
            format("Error %s at %s returned %s", task, request.getUri(), response.getStatusCode()),
            response.getException());
    }

    public String getQueryId() {
        return mppQueryId;
    }

    public abstract void deleteQuery();

    public abstract void failQuery(Throwable t);

    @Override
    public void close() {
        if (!closed.getAndSet(true)) {
            URI uri = currentResults.get().getNextUri();
            if (uri != null) {
                deleteQuery();
            }
        }
    }
}