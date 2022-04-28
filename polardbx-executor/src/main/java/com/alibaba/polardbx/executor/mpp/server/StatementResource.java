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

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.executor.mpp.server;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.exception.code.ErrorType;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.Session;
import com.alibaba.polardbx.executor.mpp.Threads;
import com.alibaba.polardbx.executor.mpp.client.FailureInfo;
import com.alibaba.polardbx.executor.mpp.client.LocalResultRequest;
import com.alibaba.polardbx.executor.mpp.client.LocalResultResponse;
import com.alibaba.polardbx.executor.mpp.client.QueryError;
import com.alibaba.polardbx.executor.mpp.client.QueryResults;
import com.alibaba.polardbx.executor.mpp.execution.QueryExecution;
import com.alibaba.polardbx.executor.mpp.execution.QueryInfo;
import com.alibaba.polardbx.executor.mpp.execution.QueryManager;
import com.alibaba.polardbx.executor.mpp.execution.QueryState;
import com.alibaba.polardbx.executor.mpp.execution.RecordMemSystemListener;
import com.alibaba.polardbx.executor.mpp.execution.StageInfo;
import com.alibaba.polardbx.executor.mpp.execution.StageState;
import com.alibaba.polardbx.executor.mpp.execution.TaskInfo;
import com.alibaba.polardbx.executor.mpp.execution.buffer.OutputBufferInfo;
import com.alibaba.polardbx.executor.mpp.execution.buffer.PagesSerde;
import com.alibaba.polardbx.executor.mpp.execution.buffer.PagesSerdeFactory;
import com.alibaba.polardbx.executor.mpp.execution.buffer.SerializedChunk;
import com.alibaba.polardbx.executor.mpp.metadata.TaskLocation;
import com.alibaba.polardbx.executor.mpp.operator.ExchangeClientSupplier;
import com.alibaba.polardbx.executor.mpp.operator.IExchangeClient;
import com.alibaba.polardbx.executor.mpp.util.Failures;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import io.airlift.jaxrs.testing.MockUriInfo;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.apache.calcite.rel.RelNode;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class StatementResource {

    private static final Logger log = LoggerFactory.getLogger(StatementResource.class);

    private static final long DESIRED_RESULT_BYTES = new DataSize(1, MEGABYTE).toBytes();

    private final QueryManager queryManager;
    private final ExchangeClientSupplier exchangeClientSupplier;
    private final PagesSerdeFactory pagesSerdeFactory;

    private final ConcurrentMap<String, Query> queries = new ConcurrentHashMap<>();
    private final ScheduledExecutorService queryPurger = newSingleThreadScheduledExecutor(
        Threads.threadsNamed("query-purger"));
    private static StatementResource instance = null;

    @Inject
    public StatementResource(
        QueryManager queryManager,
        ExchangeClientSupplier exchangeClientSupplier) {
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.exchangeClientSupplier = requireNonNull(exchangeClientSupplier, "exchangeClientSupplier is null");
        this.pagesSerdeFactory = new PagesSerdeFactory(false);

        queryPurger.scheduleWithFixedDelay(new PurgeQueriesRunnable(queries, queryManager), 200, 200, MILLISECONDS);
        if (instance == null) {
            instance = this;
        }
    }

    public static StatementResource getInstance() {
        return instance;
    }

    public LocalResultResponse createQuery(ExecutionContext clientContext, RelNode physicalPlan, URI serverUri) {
        StatementResource.Query query;
        LocalResultResponse localResultResponse = new LocalResultResponse();
        try {
            Session session = new Session(clientContext.getTraceId(), clientContext);
            query = new StatementResource.Query(exchangeClientSupplier,
                session,
                clientContext.getOriginSql(),
                queryManager,
                pagesSerdeFactory,
                physicalPlan);
            queries.put(query.getQueryId(), query);
            query.init();

            UriInfo uriInfo =
                MockUriInfo.from(uriBuilderFrom(serverUri).replacePath("/v1/statement").build().toString());
            StatementResource
                .getQueryResultsLocal(localResultResponse, Optional.empty(), query, uriInfo, new Duration(1, SECONDS));
        } catch (Exception t) {
            localResultResponse.setException(t);
        }
        return localResultResponse;
    }

    public static LocalResultResponse getQueryResultsLocal(LocalResultResponse result, Optional<Long> token,
                                                           Query query, UriInfo uriInfo, Duration wait)
        throws Exception {
        QueryResults queryResults;
        if (token.isPresent()) {
            queryResults = query.getResults(token.get(), uriInfo, wait);
        } else {
            queryResults = query.getNextResults(uriInfo, wait);
        }

        result.setResults(queryResults);
        result.setToken(queryResults.getToken());
        return result;
    }

    public void cancelQueryLocal(String queryId) {
        Query query = queries.get(queryId);
        if (query != null) {
            query.cancel();
        }
    }

    public void failQueryLocal(String queryId, Throwable t) {
        Query query = queries.get(queryId);
        if (query != null) {
            query.fail(t);
        }
    }

    public LocalResultResponse getResultLocal(LocalResultRequest request) {
        LocalResultResponse queryResult = new LocalResultResponse();
        try {
            UriInfo uriInfo = MockUriInfo.from(request.getUriInfo());
            String queryId = request.getQueryId();
            StatementResource.Query query = queries.get(queryId);
            if (query != null) {
                StatementResource.getQueryResultsLocal(
                    queryResult,
                    Optional.of(request.getToken()),
                    query,
                    uriInfo,
                    new Duration(1, SECONDS)
                );
            } else {
                throw new Exception("queryId:" + queryId + " not found");
            }
        } catch (Exception e) {
            queryResult.setException(e);
        }
        return queryResult;
    }

    @PreDestroy
    public void stop() {
        queryPurger.shutdownNow();
    }

    public Query getQuery(String queryId) {
        return queries.get(queryId);
    }

    @ThreadSafe
    public static class Query {
        private final QueryManager queryManager;
        private final String queryId;
        private IExchangeClient exchangeClient;
        private PagesSerde serde;

        private final AtomicLong resultId = new AtomicLong();
        private Session session;

        @GuardedBy("this")
        private Long updateCount;

        @GuardedBy("this")
        private QueryResults lastResult;

        @GuardedBy("this")
        private String lastResultPath;

        private String query;

        private ExchangeClientSupplier exchangeClientSupplier;

        private AtomicBoolean canPurgeQuerieRecycle = new AtomicBoolean(false);

        private QueryExecution queryExecution = null;

        private final CountDownLatch countDownLatch = new CountDownLatch(1);

        private int waitOutputStageCanAddBuffersTimes = 0;

        private static final Duration afterFristWait = new Duration(0, MILLISECONDS);

        private RelNode physicalPlan;

        private MemoryAllocatorCtx allocator;

        public Query(ExchangeClientSupplier exchangeClientSupplier,
                     Session session,
                     String query,
                     QueryManager queryManager,
                     PagesSerdeFactory pagesSerdeFactory,
                     RelNode physicalPlan) {
            requireNonNull(query, "query is null");
            requireNonNull(queryManager, "queryManager is null");
            this.session = requireNonNull(session, "sessionSupplier is null");
            this.queryId = requireNonNull(session.getQueryId(), "queryId is null");
            this.serde = pagesSerdeFactory.createPagesSerde(CalciteUtils.getTypes(physicalPlan.getRowType()));
            this.exchangeClientSupplier = exchangeClientSupplier;
            this.queryManager = queryManager;
            this.query = query;
            this.physicalPlan = physicalPlan;
            this.allocator = session.getClientContext().getMemoryPool().getMemoryAllocatorCtx();
        }

        public void init() {
            try {
                this.queryExecution = queryManager.createClusterQuery(session, query, this);
                this.exchangeClient = exchangeClientSupplier.get(
                    new RecordMemSystemListener(allocator), session.getClientContext());
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                throw e;
            } finally {
                setCanPurgeQuerieRecycle();
            }
        }

        public void setCanPurgeQuerieRecycle() {
            canPurgeQuerieRecycle.set(true);
        }

        public boolean canPurgeQuerieRecycle() {
            return canPurgeQuerieRecycle.get();
        }

        public void finishWaitOutputStageCanAddBuffers() {
            if (countDownLatch.getCount() > 0) {
                countDownLatch.countDown();
            }
        }

        public void cancel() {
            queryManager.cancelQuery(queryId);
            dispose();
        }

        public void fail(Throwable t) {
            queryManager.failQuery(queryId, t);
            dispose();
        }

        public RelNode getPhysicalPlan() {
            return physicalPlan;
        }

        public void dispose() {
            try {
                if (exchangeClient != null) {
                    exchangeClient.close();
                }
            } catch (IOException e) {
                // ignore
            }
        }

        public String getQueryId() {
            return queryId;
        }

        public synchronized QueryResults getResults(long token, UriInfo uriInfo, Duration maxWaitTime)
            throws Exception {
            // is the a repeated request for the last results?
            String requestedPath = uriInfo.getAbsolutePath().getPath();
            if (lastResultPath != null && requestedPath.equals(lastResultPath)) {
                // tell query manager we are still interested in the query
                queryManager.getQueryInfo(queryId);
                queryManager.recordHeartbeat(queryId);
                return lastResult;
            }

            if (token < resultId.get()) {
                throw new WebApplicationException(Response.Status.GONE);
            }

            // if this is not a request for the next results, return not found
            if (lastResult.getNextUri() == null || !requestedPath.equals(lastResult.getNextUri().getPath())) {
                // unknown token
                throw new WebApplicationException(Response.Status.NOT_FOUND);
            }

            return getNextResults(uriInfo, maxWaitTime);
        }

        public synchronized QueryResults getNextResults(UriInfo uriInfo, Duration maxWaitTime) throws Exception {
            Iterable<Object> data = null;
            try {
                data = getData(maxWaitTime);
            } catch (Throwable t) {
                QueryInfo queryInfo = queryExecution.getQueryInfo();
                QueryError queryError = toQueryError(queryInfo);
                if (queryError != null) {
                    FailureInfo failureInfo = queryError.getFailureInfo();
                    if (failureInfo != null) {
                        throw GeneralUtil.nestedException(failureInfo.toExceptionWithoutType());
                    }
                }
                throw t;
            }

            // get the query info before returning
            // force update if query manager is closed
            QueryInfo queryInfo = queryExecution.getQueryInfo();
            queryManager.recordHeartbeat(queryId);

            // if we have received all of the output data and the query is not marked as done, wait for the query to finish
//            if (!isQueryDagWithDataDivide) {
//                if (exchangeClient.isClosed() && !queryInfo.getState().isDone()) {
//                    queryManager.waitForStateChange(queryId, queryInfo.getState(), maxWaitTime);
//                    queryInfo = queryManager.getQueryInfo(queryId);
//                }
//            }

            // close exchange client if the query has failed
            if (queryInfo.getState().isDone()) {
                if (queryInfo.getState() != QueryState.FINISHED) {
                    exchangeClient.close();
                } else if (!queryInfo.getOutputStage().isPresent()) {
                    // For simple executions (e.g. drop table), there will never be an output stage,
                    // so close the exchange as soon as the query is done.
                    exchangeClient.close();

                    // Return a single value for clients that require a result.
                    data = ImmutableSet.of(ImmutableList.of(true));
                }
            }

            // only return a next if the query is not done or there is more data to send (due to buffering)
            URI nextResultsUri = null;
            long nextToken = resultId.get();
            QueryError rootStageError = null;
            if (!exchangeClient.isClosed()) {
                nextToken = resultId.incrementAndGet();
                nextResultsUri = uriInfo.getBaseUriBuilder().replacePath("/v1/statement").path(queryId.toString())
                    .path(String.valueOf(nextToken)).replaceQuery("").build();
            } else {
                Optional<StageInfo> outputStage = queryInfo.getOutputStage();
                if (outputStage.isPresent() && outputStage.get().getState() == StageState.FAILED) {
                    String errorMsg = "query " + queryId;
                    if (session != null) {
                        errorMsg += " clientInfo " + "";
                    }
                    errorMsg += " statementResource found rootStage failed";
                    if (outputStage.get().getFailureCause() == null) {
                        throw new RuntimeException(errorMsg);
                    }
                    ErrorCode code = outputStage.get().getFailureCause().getErrorCode();
                    FailureInfo info = outputStage.get().getFailureCause().toFailureInfo();
                    rootStageError = toQueryError(queryId, queryInfo.getState(), info, code);
                }
            }

            // first time through, self is null
            QueryError queryError = toQueryError(queryInfo);
            QueryResults queryResults = new QueryResults(
                queryId.toString(),
                nextResultsUri,
                data,
                queryError == null ? rootStageError : toQueryError(queryInfo),
                updateCount,
                nextToken);

            // cache the last results
            if (lastResult != null && lastResult.getNextUri() != null) {
                lastResultPath = lastResult.getNextUri().getPath();
            } else {
                lastResultPath = null;
            }
            lastResult = queryResults;

            return queryResults;
        }

        private synchronized Iterable<Object> getData(Duration maxWait)
            throws InterruptedException {
            // wait for query to start
            QueryInfo queryInfo = queryExecution.getQueryInfo();

            while (maxWait.toMillis() > 1 && !isQueryStarted(queryInfo)) {
                queryManager.recordHeartbeat(queryId);
                maxWait = queryManager.waitForStateChange(queryId, queryInfo.getState(), maxWait);
                queryInfo = queryManager.getQueryInfo(queryId);
            }

            StageInfo outputStage = queryInfo.getOutputStage().orElse(null);
            // if query did not finish starting or does MppConfig.javanot have output, just return
            if (!isQueryStarted(queryInfo) || outputStage == null) {
                return null;
            }

            maxWait = waitOutputStageCanAddBuffers(outputStage, maxWait);
            updateExchangeClient(outputStage);

            ImmutableList.Builder<Chunk> pages = ImmutableList.builder();
            // wait up to max wait for data to arrive; then try to return at least DESIRED_RESULT_BYTES
            long bytes = 0;
            while (bytes < DESIRED_RESULT_BYTES) {
                SerializedChunk serializedPage;

                serializedPage = exchangeClient.getNextPageForDagWithDataDivide(maxWait);

                if (serializedPage == null) {
                    break;
                }

                Chunk chunk = serde.deserialize(serializedPage);
                bytes += chunk.getSizeInBytes();
                pages.add(chunk);
                // only wait on first call
                maxWait = afterFristWait;
            }

            if (bytes == 0) {
                return null;
            }
            return (Iterable) pages.build();
        }

        private synchronized void updateExchangeClient(StageInfo outputStage) {
            // add any additional output locations
            if (exchangeClient.isNoMoreLocations()) {
                return;
            }
            if (!outputStage.getState().isDone()) {
                for (TaskInfo taskInfo : outputStage.getTasks()) {
                    OutputBufferInfo outputBuffers = taskInfo.getOutputBuffers();
                    if (outputBuffers.getState().canAddBuffers()) {
                        // output buffer are still being created
                        continue;
                    }
                    exchangeClient.addLocation(new TaskLocation(taskInfo.getTaskStatus().getSelf().getNodeServer(),
                        taskInfo.getTaskStatus().getSelf().getTaskId(), 0));
                }
            }

            if (allOutputBuffersCreated(outputStage)) {
                exchangeClient.noMoreLocations();
            }
        }

        private static boolean allOutputBuffersCreated(StageInfo outputStage) {
            StageState stageState = outputStage.getState();

            // if the stage is already done, then there will be no more buffers
            if (stageState.isDone()) {
                return true;
            }

            // have all stage tasks been scheduled?
            if (stageState == StageState.PLANNED || stageState == StageState.SCHEDULING) {
                return false;
            }

            // have all tasks finished adding buffers
            return outputStage.getTasks().stream()
                .allMatch(taskInfo -> !taskInfo.getOutputBuffers().getState().canAddBuffers());
        }

        public Duration waitOutputStageCanAddBuffers(StageInfo outputStage, Duration maxWait) {
            if (outputStage == null) {
                return maxWait;
            }

            if (countDownLatch.getCount() <= 0) {
                return maxWait;
            }

            if (waitOutputStageCanAddBuffersTimes > 3) {
                log.warn("how can this happen queryId: " + queryId.toString() +
                    " waitOutputStageCanAddBuffersTimes:" + waitOutputStageCanAddBuffersTimes);
                countDownLatch.countDown();
                return maxWait;
            }

            boolean canAddBuffers = false;
            if (!outputStage.getState().isDone()) {
                for (TaskInfo taskInfo : outputStage.getTasks()) {
                    OutputBufferInfo outputBuffers = taskInfo.getOutputBuffers();
                    if (outputBuffers.getState().canAddBuffers()) {
                        canAddBuffers = true;
                        break;
                    }
                }
            } else {
                return maxWait;
            }

            if (!canAddBuffers) {
                return maxWait;
            }

            if (countDownLatch.getCount() <= 0) {
                return maxWait;
            } else {
                try {
                    long cost = System.currentTimeMillis();
                    long waitMillis = maxWait.toMillis();
                    countDownLatch.await(waitMillis, TimeUnit.MILLISECONDS);
                    waitOutputStageCanAddBuffersTimes++;
                    cost = System.currentTimeMillis() - cost;
                    return new Duration(waitMillis < cost ? 0 : waitMillis - cost, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
            return maxWait;
        }

        public synchronized boolean isDataFinished() {
            if (exchangeClient != null) {
                return exchangeClient.isClosed();
            }
            return true;
        }

        private static boolean isQueryStarted(QueryInfo queryInfo) {
            QueryState state = queryInfo.getState();
            return state != QueryState.QUEUED && queryInfo.getState() != QueryState.PLANNING
                && queryInfo.getState() != QueryState.STARTING;
        }

        private static URI findCancelableLeafStage(QueryInfo queryInfo) {
            // if query is running, find the leaf-most running stage
            return queryInfo.getOutputStage().map(Query::findCancelableLeafStage).orElse(null);
        }

        private static URI findCancelableLeafStage(StageInfo stage) {
            // if this stage is already done, we can't cancel it
            if (stage.getState().isDone()) {
                return null;
            }

            // attempt to find a cancelable sub stage
            // check in reverse order since build side of a join will be later in the list
            for (StageInfo subStage : Lists.reverse(stage.getSubStages())) {
                URI leafStage = findCancelableLeafStage(subStage);
                if (leafStage != null) {
                    return leafStage;
                }
            }

            // no matching sub stage, so return this stage
            return stage.getSelf();
        }

        private static QueryError toQueryError(String queryId, QueryState state, FailureInfo failure,
                                               ErrorCode errorCode) {
            if (errorCode == null) {
                log.warn(String.format("Failed query %s has no error code", queryId));
                errorCode = ErrorCode.ERR_EXECUTE_MPP;
            }
            if (failure == null) {
                log.warn(String.format("Query %s in state %s has no failure info", queryId, state));
                failure = Failures.toFailure(new RuntimeException(format("Query is %s (reason unknown)", state)))
                    .toFailureInfo();
            }
            ErrorType type = errorCode.getType();
            return new QueryError(failure.getMessage(),
                null,
                errorCode.getCode(),
                errorCode.getName(),
                type.toString(),
                failure);
        }

        public Future<QueryInfo> getBlockedQueryInfo() {
            return queryExecution.getFinalQueryInfo();
        }

        public QueryInfo tryGetQueryInfo() {
            return queryExecution.getQueryInfo();
        }

        public static QueryError toQueryError(QueryInfo queryInfo) {
            FailureInfo failure = queryInfo.getFailureInfo();
            if (failure == null) {
                QueryState state = queryInfo.getState();
                if ((!state.isDone()) || (state == QueryState.FINISHED)) {
                    return null;
                }
                log.warn(String.format("Query %s in state %s has no failure info", queryInfo.getQueryId(), state));
                failure = Failures.toFailure(new RuntimeException(format("Query is %s (reason unknown)", state)))
                    .toFailureInfo();
            }

            ErrorCode errorCode;
            if (queryInfo.getErrorCode() != null) {
                errorCode = queryInfo.getErrorCode();
            } else {
                errorCode = ErrorCode.ERR_EXECUTE_MPP;
                log.warn(String.format("Failed query %s has no error code", queryInfo.getQueryId()));
            }
            QueryError queryError = new QueryError(
                failure.getMessage(),
                null,
                errorCode.getCode(),
                errorCode.getName(),
                errorCode.getType().toString(),
                failure);
            return queryError;
        }
    }

    private static class PurgeQueriesRunnable
        implements Runnable {
        private final ConcurrentMap<String, Query> queries;
        private final QueryManager queryManager;

        public PurgeQueriesRunnable(ConcurrentMap<String, Query> queries, QueryManager queryManager) {
            this.queries = queries;
            this.queryManager = queryManager;
        }

        @Override
        public void run() {
            try {
                // Queries are added to the query manager before being recorded in queryIds set.
                // Therefore, we take a snapshot if queryIds before getting the live queries
                // from the query manager.  Then we remove only the queries in the snapshot and
                // not live queries set.  If we did this in the other order, a query could be
                // registered between fetching the live queries and inspecting the queryIds set.
                for (String queryId : ImmutableSet.copyOf(queries.keySet())) {
                    Query query = queries.get(queryId);
                    QueryState state = queryManager.getQueryState(queryId);

                    if (state != null && state == QueryState.FAILED) {
                        if (query.canPurgeQuerieRecycle()) {
                            query.dispose();
                        }
                    }

                    // forget about this query if the query manager is no longer tracking it
                    if (state == null) {
                        if (query.canPurgeQuerieRecycle() && query.isDataFinished()) {
                            queries.remove(queryId);
                        }
                    }
                }
            } catch (Throwable e) {
                log.warn("Error removing old queries", e);
            }
        }
    }
}
