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

package com.alibaba.polardbx.executor.mpp.execution;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.MppConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.executor.mpp.Session;
import com.alibaba.polardbx.executor.mpp.Threads;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.mpp.server.StatementResource;
import com.alibaba.polardbx.executor.mpp.util.ImmutableCollectors;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import io.airlift.units.Duration;
import org.apache.calcite.rel.RelNode;
import org.joda.time.DateTime;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.polardbx.executor.mpp.execution.QueryState.FAILED;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class SqlQueryManager implements QueryManager {

    private static final Logger log = LoggerFactory.getLogger(SqlQueryManager.class);

    private final SqlQueryExecution.SqlQueryExecutionFactory queryExecutionFactory;
    private final SqlQueryLocalExecution.SqlQueryLocalExecutionFactory localExecutionFactory;
    private final ConcurrentMap<String, QueryExecution> queries = new ConcurrentHashMap<>();

    private int maxQueryHistory;
    private long minQueryExpireAge;
    private long maxQueryExpiredReservationAge;
    private long clientTimeout;

    // the expired querys which need keep reservation
    private final Queue<QueryExecution> expiredReservationQueue = new LinkedBlockingQueue<>();
    private final Queue<QueryExecution> expirationQueue = new LinkedBlockingQueue<>();

    private final ScheduledExecutorService queryManagementExecutor;

    private final AtomicLong totalQuery = new AtomicLong(0);

    private class QueryMonitor
        implements Runnable {

        private long timeoutMillis = 10 * 60 * 1000L;

        @Override
        public void run() {
            while (true) {
                try {
                    log.info(
                        "current query count " + queries.size() + " expiration query count " + expirationQueue.size());
                    long now = System.currentTimeMillis();
                    for (QueryExecution queryExecution : queries.values()) {
                        if (!queryExecution.getState().isDone()) {
                            long executeCreateMillis = queryExecution.queryStateMachine().getExecuteCreateMillis();
                            if (executeCreateMillis + timeoutMillis < now) {
                                String queryId = queryExecution.queryStateMachine().getQueryId();
                                String query = queryExecution.queryStateMachine().getQuery();
                                log.info("the blocked queryId is " + queryId + " ,and the sql is  " + query);
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error(e.getMessage());
                }

                try {
                    Thread.sleep(1000 * 120);
                } catch (InterruptedException e) {
                    log.error("sleep exception " + e.getMessage());
                }
            }
        }
    }

    @PreDestroy
    public void stop() {
        boolean queryCancelled = false;
        for (QueryExecution queryExecution : queries.values()) {
            if (queryExecution.getState().isDone()) {
                continue;
            }

            log.info(String.format("Server shutting down. Query %s has been cancelled", queryExecution.getQueryId()));
            queryExecution.fail(new TddlRuntimeException(ErrorCode.ERR_SERVER_SHUTTING_DOWN, "Query " +
                queryExecution.getQueryId() + " has been cancelled"));
            queryCancelled = true;
        }
        if (queryCancelled) {
            try {
                TimeUnit.SECONDS.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        queryManagementExecutor.shutdownNow();
    }

    @Inject
    public SqlQueryManager(SqlQueryExecution.SqlQueryExecutionFactory queryExecutionFactory,
                           SqlQueryLocalExecution.SqlQueryLocalExecutionFactory localExecutionFactory) {
        this.queryExecutionFactory = queryExecutionFactory;
        this.localExecutionFactory = localExecutionFactory;

        this.minQueryExpireAge = MppConfig.getInstance().getMinQueryExpireTime();
        this.maxQueryExpiredReservationAge = MppConfig.getInstance().getMaxQueryExpiredReservationTime();
        this.maxQueryHistory = MppConfig.getInstance().getMaxQueryHistory();
        this.clientTimeout = MppConfig.getInstance().getClientTimeout();

        queryManagementExecutor = new ScheduledThreadPoolExecutor(
            MppConfig.getInstance().getQueryManagerThreadPoolSize(),
            Threads.tpThreadsNamed("query-management"));
        queryManagementExecutor.scheduleWithFixedDelay(() -> {
            try {
                failAbandonedQueries();
            } catch (Throwable e) {
                log.warn("Error cancelling abandoned queries", e);
            }

            try {
                enforceQueryMaxRunTimeLimits();
            } catch (Throwable e) {
                log.warn("Error enforcing query timeout limits", e);
            }

            try {
                removeExpiredQueries();
            } catch (Throwable e) {
                log.warn("Error removing expired queries", e);
            }

            try {
                pruneExpiredQueries();
            } catch (Throwable e) {
                log.warn("Error pruning expired queries", e);
            }
        }, 1, 1, TimeUnit.SECONDS);

        new Thread(new QueryMonitor()).start();
    }

    /**
     * Enforce timeout at the query level
     */
    private void enforceQueryMaxRunTimeLimits() {
        long now = System.currentTimeMillis();
        for (QueryExecution query : queries.values()) {
            if (query.getState().isDone()) {
                continue;
            }
            Long queryMaxRunTime = query.getSession().getClientContext().getParamManager().getLong(
                ConnectionParams.MPP_QUERY_MAX_RUN_TIME);
            long executeCreateMillis = query.queryStateMachine().getExecuteCreateMillis();
            if (executeCreateMillis + queryMaxRunTime < now) {
                query.fail(new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    "Query exceeded maximum time limit of " + queryMaxRunTime));
            }
        }
    }

    private boolean isAbandoned(QueryStateMachine stateMachine) {
        DateTime oldestAllowedHeartbeat = DateTime.now().minus(clientTimeout);
        DateTime lastHeartbeat = stateMachine.getLastHeartbeat();
        return lastHeartbeat != null && lastHeartbeat.isBefore(oldestAllowedHeartbeat);
    }

    private void failAbandonedQueries() {
        for (QueryExecution queryExecution : queries.values()) {
            QueryStateMachine stateMachine = queryExecution.queryStateMachine();
            if (stateMachine.isDone()) {
                continue;
            }

            if (isAbandoned(stateMachine)) {
                log.info(String.format("Failing abandoned query %s", queryExecution.getQueryId()));
                queryExecution.fail(new TddlRuntimeException(ErrorCode.ERR_ABANDONED_QUERY, queryExecution.getQueryId(),
                    stateMachine.getLastHeartbeat().toString(), DateTime.now().toString()));
            }
        }
    }

    /**
     * Remove completed queries after a waiting period
     */
    private void removeExpiredQueries() {
        DateTime timeHorizon = DateTime.now().minus(minQueryExpireAge);

        DateTime reservationTimeHorizon = DateTime.now().minus(maxQueryExpiredReservationAge);

        // remove reservation queries beyond reservationTimeHorizon
        while (expiredReservationQueue.size() > 0) {
            QueryExecution query = expiredReservationQueue.peek();
            // expiredReservationQueue is also FIFO based on query end time. Stop when we see the
            // first query that's too young to expire
            if (query.getQueryEndTime() == null || query.getQueryEndTime().isAfter(reservationTimeHorizon)) {
                break;
            }

            String queryId = query.getQueryId();
            queries.remove(queryId);
            expiredReservationQueue.remove();
        }

        // keep reservation queries fewer than maxQueryHistory, ignore reservationTimeHorizon limit
        while (expiredReservationQueue.size() > maxQueryHistory) {
            QueryExecution query = expiredReservationQueue.peek();

            String queryId = query.getQueryId();
            queries.remove(queryId);
            expiredReservationQueue.remove();
        }

        // calculate remain queries in expirationQueue
        int maxRemain = maxQueryHistory - expiredReservationQueue.size();
        maxRemain = maxRemain < 0 ? 0 : maxRemain;
        maxRemain = maxRemain > maxQueryHistory ? maxQueryHistory : maxRemain;

        // we're willing to keep queries beyond timeHorizon as long as we have fewer than maxQueryHistory
        while (expirationQueue.size() > maxRemain) {
            QueryExecution query = expirationQueue.peek();

            // expirationQueue is FIFO based on query end time. Stop when we see the
            // first query that's too young to expire
            if (query.getQueryEndTime() == null || query.getQueryEndTime().isAfter(timeHorizon)) {
                break;
            }

            // only expire them if they are older than minQueryExpireAge. We need to keep them
            // around for a while in case clients come back asking for status
            String queryId = query.getQueryId();

            if (query.getState() == FAILED || query.isNeedReserveAfterExpired()) {
                // record the need keep reservation's querys.
                //两种情况需要保留：1. 失败的查询 2. 开启最高级别采样（explain analyze table 也会开启高级别采样）
                expiredReservationQueue.add(query);
                expirationQueue.remove();
            } else {
                queries.remove(queryId);
                expirationQueue.remove();
            }
            logQueryPlan(query);
        }

        // clean queries in expiredReservationQueue again
        // expiredReservationQueue may grow larger after expirationQueue remove
        while (expiredReservationQueue.size() > maxQueryHistory) {
            QueryExecution query = expiredReservationQueue.peek();

            String queryId = query.getQueryId();
            queries.remove(queryId);
            expiredReservationQueue.remove();
        }
    }

    private void logQueryPlan(QueryExecution query) {
        boolean isPrintElapsedLongQueryEnabled = query.getSession().getClientContext().getParamManager()
            .getBoolean(ConnectionParams.MPP_PRINT_ELAPSED_LONG_QUERY_ENABLED);
        if (isPrintElapsedLongQueryEnabled) {
            QueryStateMachine queryStateMachine = query.queryStateMachine();
            Duration elapsedTime = queryStateMachine.getElapsedTime();
            long elapsedQueryThresholdMills = query.getSession().getClientContext().getParamManager()
                .getLong(ConnectionParams.MPP_ELAPSED_QUERY_THRESHOLD_MILLS);
            if (elapsedTime.toMillis() > elapsedQueryThresholdMills) {
                log.warn(
                    "elapsed time too long warn, " + "query : " + queryStateMachine.getQuery());
            }
        }
    }

    /**
     * Prune extraneous info from old queries
     */
    private void pruneExpiredQueries() {
        if (expirationQueue.size() <= maxQueryHistory) {
            return;
        }

        int count = 0;
        // we're willing to keep full info for up to maxQueryHistory queries
        for (QueryExecution query : expirationQueue) {
            if (expirationQueue.size() - count <= maxQueryHistory) {
                break;
            }
            query.pruneInfo();
            count++;
        }
    }

    @Override
    public List<QueryInfo> getAllQueryInfo() {
        return queries.values().stream()
            .map(queryExecution -> {
                try {
                    return queryExecution.getQueryInfo();
                } catch (RuntimeException ignored) {
                    return null;
                }
            })
            .filter(Objects::nonNull)
            .collect(ImmutableCollectors.toImmutableList());
    }

    @Override
    public List<TaskContext> getAllLocalQueryContext() {

        //TODO filter
        return queries.values().stream()
            .map(queryExecution -> {
                ExecutionContext ec = queryExecution.getSession().getClientContext();
                if (!ExecUtils.isMppMode(ec) && ec.getMemoryPool() != null) {
                    return queryExecution.getTaskContext();
                } else {
                    return null;
                }
            })
            .filter(Objects::nonNull)
            .collect(ImmutableCollectors.toImmutableList());
    }

    @Override
    public Duration waitForStateChange(String queryId, QueryState currentState,
                                       Duration maxWait) throws InterruptedException {
        requireNonNull(queryId, "queryId is null");
        requireNonNull(maxWait, "maxWait is null");

        QueryExecution query = queries.get(queryId);
        if (query == null) {
            return maxWait;
        }

        return query.waitForStateChange(currentState, maxWait);
    }

    @Override
    public QueryInfo getQueryInfo(String queryId) {
        requireNonNull(queryId, "queryId is null");

        QueryExecution query = queries.get(queryId);
        if (query == null) {
            throw new NoSuchElementException();
        }

        return query.getQueryInfo();
    }

    @Override
    public QueryState getQueryState(String queryId) {
        requireNonNull(queryId, "queryId is null");
        QueryExecution queryExecution = queries.get(queryId);
        if (queryExecution == null) {
            return null;
        }
        return queryExecution.getState();
    }

    @Override
    public void recordHeartbeat(String queryId) {
        requireNonNull(queryId, "queryId is null");

        QueryExecution query = queries.get(queryId);
        if (query == null) {
            return;
        }

        query.recordHeartbeat();
    }

    @Override
    public QueryExecution createClusterQuery(Session session, String query, StatementResource.Query sQuery) {
        QueryExecution queryExecution =
            submitQuery(session, query, sQuery.getPhysicalPlan(), queryExecutionFactory);
        queryExecution.addStateChangeListener(newState -> {
            if (newState.isDone()) {
                expirationQueue.add(queryExecution);
            }
        });
        queries.put(session.getQueryId(), queryExecution);
        queryExecution.start();
        if (sQuery != null) {
            sQuery.setCanPurgeQuerieRecycle();
        }
        return queryExecution;
    }

    @Override
    public QueryExecution createLocalQuery(Session session, String query, RelNode relNode) {
        QueryExecution queryExecution = submitQuery(session, query, relNode, localExecutionFactory);
        queries.put(session.getQueryId(), queryExecution);

        MDC.put(MDC.MDC_KEY_APP, session.getSchema().toLowerCase());
        MDC.put(MDC.MDC_KEY_CON, session.getClientContext().getMdcConnString());
        queryExecution.start();

        return queryExecution;
    }

    private QueryExecution submitQuery(Session session, String query, RelNode relNode,
                                       QueryExecution.QueryExecutionFactory<? extends QueryExecution> queryExecutionFactory) {
        requireNonNull(session, "sessionFactory is null");
        requireNonNull(query, "query is null");
        checkArgument(!query.isEmpty(), "query must not be empty string");

        if (log.isDebugEnabled()) {
            log.debug(String.format("start queryId: %s, schema=%s", query, session.getSchema()));
        }
        QueryExecution queryExecution = queryExecutionFactory.createQueryExecution(this, query, relNode, session);
        totalQuery.incrementAndGet();
        return queryExecution;
    }

    @Override
    public void removeLocalQuery(String queryId, boolean needReserved) {
        if (needReserved) {
            QueryExecution query = queries.get(queryId);
            if (query != null) {
                expirationQueue.add(query);
            }
        } else {
            queries.remove(queryId);
        }
    }

    @Override
    public void cancelQuery(String queryId) {
        requireNonNull(queryId, "queryId is null");
        if (log.isDebugEnabled()) {
            log.debug(String.format("Cancel query %s", queryId));
        }
        QueryExecution query = queries.get(queryId);
        if (query != null) {
            query.cancelQuery();
        }
    }

    @Override
    public void cancelStage(StageId stageId) {
        requireNonNull(stageId, "stageId is null");
        if (log.isDebugEnabled()) {
            log.debug(String.format("Cancel stage %s", stageId));
        }
        QueryExecution query = queries.get(stageId.getQueryId());
        if (query != null) {
            query.cancelStage(stageId);
        }
    }

    @Override
    public QueryExecution getQueryExecution(String queryId) {
        return queries.get(queryId);
    }

    @Override
    public void failQuery(String queryId, Throwable cause) {
        requireNonNull(cause, "cause is null");
        QueryExecution query = queries.get(queryId);
        if (query != null) {
            query.fail(cause);
        }
    }

    @Override
    public void reloadConfig() {
        this.minQueryExpireAge = MppConfig.getInstance().getMinQueryExpireTime();
        this.maxQueryExpiredReservationAge = MppConfig.getInstance().getMaxQueryExpiredReservationTime();
        this.maxQueryHistory = MppConfig.getInstance().getMaxQueryHistory();
        this.clientTimeout = MppConfig.getInstance().getClientTimeout();
        if (ServiceProvider.getInstance().clusterMode() && this.queryExecutionFactory != null) {
            //动态调整cluster模式配置
            queryExecutionFactory.setThreadPoolExecutor(MppConfig.getInstance().getQueryExecutionThreadPoolSize());
        }
    }

    @Override
    public long getTotalQueries() {
        return totalQuery.get();
    }
}
