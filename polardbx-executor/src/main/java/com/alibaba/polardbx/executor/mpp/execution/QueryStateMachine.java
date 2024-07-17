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
package com.alibaba.polardbx.executor.mpp.execution;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.mpp.Session;
import com.alibaba.polardbx.executor.mpp.client.FailureInfo;
import com.alibaba.polardbx.executor.mpp.operator.BlockedReason;
import com.alibaba.polardbx.executor.mpp.operator.OperatorStats;
import com.alibaba.polardbx.executor.mpp.util.Failures;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.joda.time.DateTime;

import javax.annotation.concurrent.ThreadSafe;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.alibaba.polardbx.executor.mpp.execution.QueryState.FAILED;
import static com.alibaba.polardbx.executor.mpp.execution.QueryState.FINISHED;
import static com.alibaba.polardbx.executor.mpp.execution.QueryState.FINISHING;
import static com.alibaba.polardbx.executor.mpp.execution.QueryState.PLANNING;
import static com.alibaba.polardbx.executor.mpp.execution.QueryState.QUEUED;
import static com.alibaba.polardbx.executor.mpp.execution.QueryState.RUNNING;
import static com.alibaba.polardbx.executor.mpp.execution.QueryState.STARTING;
import static com.alibaba.polardbx.executor.mpp.execution.QueryState.TERMINAL_QUERY_STATES;
import static com.alibaba.polardbx.executor.mpp.execution.StageInfo.getAllStages;
import static io.airlift.units.DataSize.succinctBytes;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
public class QueryStateMachine implements StateMachineBase<QueryState> {
    private static final Logger log = LoggerFactory.getLogger(QueryStateMachine.class);

    private final String queryId;
    private final String query;
    private final Session session;
    private final boolean autoCommit;
    private final QueryStateTimer queryStateTimer;

    private final AtomicLong peakMemory = new AtomicLong();
    private final AtomicLong currentMemory = new AtomicLong();

    private final StateMachine<QueryState> queryState;

    private final AtomicReference<ExecutionFailureInfo> failureCause = new AtomicReference<>();

    private final StateMachine<Optional<QueryInfo>> finalQueryInfo;

    private final SettableFuture<QueryInfo> blocked = SettableFuture.create();

    private QueryStateMachine(String query, Session session, boolean autoCommit,
                              Executor executor, boolean needStats, Ticker ticker) {
        this.query = query;
        this.session = requireNonNull(session, "session is null");
        this.queryId = requireNonNull(session.getQueryId(), "queryId is null");
        this.autoCommit = autoCommit;
        this.queryStateTimer = new QueryStateTimer(ticker, session.getStartTime(), needStats);

        this.queryState = new StateMachine<>("query " + query, executor, QUEUED, TERMINAL_QUERY_STATES);
        this.finalQueryInfo = new StateMachine<>("finalQueryInfo-" + queryId, executor, Optional.empty());
    }

    public String getQuery() {
        return query;
    }

    /**
     * Created QueryStateMachines must be transitioned to terminal states to clean up resources.
     */
    public static QueryStateMachine begin(
        String query,
        Session session,
        Executor executor,
        boolean needStats) {
        return beginWithTicker(query, session, executor, needStats, Ticker.systemTicker());
    }

    static QueryStateMachine beginWithTicker(
        String query,
        Session session,
        Executor executor,
        boolean needStats,
        Ticker ticker) {

        boolean autoCommit = false;
        QueryStateMachine queryStateMachine = new QueryStateMachine(query, session, autoCommit,
            executor, needStats, ticker);
        if (log.isDebugEnabled()) {
            queryStateMachine
                .addStateChangeListener(
                    newState -> log.debug(String.format("Query %s is %s", session.getQueryId(), newState)));
        }
        return queryStateMachine;
    }

    /**
     * Create a QueryStateMachine that is already in a failed state.
     */
    public static QueryStateMachine failed(String query, Session session, URI self,
                                           Executor executor, Throwable throwable) {
        return failedWithTicker(query, session, executor, Ticker.systemTicker(), throwable);
    }

    static QueryStateMachine failedWithTicker(
        String query,
        Session session,
        Executor executor,
        Ticker ticker,
        Throwable throwable) {
        QueryStateMachine
            queryStateMachine = new QueryStateMachine(query, session, false, executor, false, ticker);
        queryStateMachine.transitionToFailed(throwable);
        return queryStateMachine;
    }

    public String getQueryId() {
        return queryId;
    }

    public Session getSession() {
        return session;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public long getPeakMemoryInBytes() {
        return peakMemory.get();
    }

    public void updateMemoryUsage(long deltaMemoryInBytes) {
        long currentMemoryValue = currentMemory.addAndGet(deltaMemoryInBytes);
        if (currentMemoryValue > peakMemory.get()) {
            peakMemory.updateAndGet(x -> currentMemoryValue > x ? currentMemoryValue : x);
        }
    }

    public QueryInfo getQueryInfo(Optional<StageInfo> rootStage, URI querySelf) {
        // Query state must be captured first in order to provide a
        // correct view of the query.  For example, building this
        // information, the query could finish, and the task states would
        // never be visible.
        QueryState state = queryState.get();

        // don't report failure info is query is marked as success
        FailureInfo failureInfo = null;
        ErrorCode errorCode = null;
        if (state == FAILED) {
            ExecutionFailureInfo failureCause = this.failureCause.get();
            if (failureCause != null) {
                failureInfo = failureCause.toFailureInfo();
                errorCode = failureCause.getErrorCode();
            }
        }

        int totalTasks = 0;
        int runningTasks = 0;
        int completedTasks = 0;

        int totalPipelineExecs = 0;
        int queuedPipelineExecs = 0;
        int runningPipelineExecs = 0;
        int completedPipelineExecs = 0;

        long cumulativeMemory = 0;
        long totalMemoryReservation = 0;
        long peakMemoryReservation = 0;

        long totalScheduledTime = 0;
        long totalCpuTime = 0;
        long totalUserTime = 0;
        long totalBlockedTime = 0;

        long processedInputDataSize = 0;
        long processedInputPositions = 0;

        long outputDataSize = 0;
        long outputPositions = 0;

        boolean fullyBlocked = rootStage.isPresent();
        Set<BlockedReason> blockedReasons = new HashSet<>();

        boolean completeInfo = true;

        List<OperatorStats> operatorStatsList = new ArrayList<>();
        List<StageInfo> stageInfos = getAllStages(rootStage);
        for (StageInfo stageInfo : stageInfos) {
            StageStats stageStats = stageInfo.getStageStats();
            totalTasks += stageStats.getTotalTasks();
            runningTasks += stageStats.getRunningTasks();
            completedTasks += stageStats.getCompletedTasks();

            totalPipelineExecs += stageStats.getTotalPipelineExecs();
            queuedPipelineExecs += stageStats.getQueuedPipelineExecs();
            runningPipelineExecs += stageStats.getRunningPipelineExecs();
            completedPipelineExecs += stageStats.getCompletedPipelineExecs();

            cumulativeMemory += stageStats.getCumulativeMemory();
            totalMemoryReservation += stageStats.getTotalMemoryReservation().toBytes();
            peakMemoryReservation = getPeakMemoryInBytes();

            totalScheduledTime += stageStats.getTotalScheduledTimeNanos();
            totalCpuTime += stageStats.getTotalCpuTimeNanos();
            totalUserTime += stageStats.getTotalUserTimeNanos();
            totalBlockedTime += stageStats.getTotalBlockedTimeNanos();
            if (!stageInfo.getState().isDone()) {
                fullyBlocked &= stageStats.isFullyBlocked();
                blockedReasons.addAll(stageStats.getBlockedReasons());
            }

            PlanInfo plan = stageInfo.getPlan();
            if (plan != null && plan.getPartition()) {
                processedInputDataSize += stageStats.getProcessedInputDataSize().toBytes();
                processedInputPositions += stageStats.getProcessedInputPositions();
            }
            completeInfo = completeInfo && stageInfo.isCompleteInfo();
            operatorStatsList.addAll(stageStats.getOperatorSummaries());
        }

        if (rootStage.isPresent()) {
            StageStats outputStageStats = rootStage.get().getStageStats();
            outputDataSize += outputStageStats.getOutputDataSize().toBytes();
            outputPositions += outputStageStats.getOutputPositions();
        }

        QueryStats queryStats = new QueryStats(
            queryStateTimer.getCreateTime(),
            queryStateTimer.getExecutionStartNanos(),
            queryStateTimer.getLastHeartbeat(),
            queryStateTimer.getEndTime(),

            queryStateTimer.getElapsedTime(),
            queryStateTimer.getQueuedTime(),
            queryStateTimer.getDistributedPlanningTime(),
            queryStateTimer.getTotalPlanningTime(),
            queryStateTimer.getFinishingTime(),

            totalTasks,
            runningTasks,
            completedTasks,

            totalPipelineExecs,
            queuedPipelineExecs,
            runningPipelineExecs,
            completedPipelineExecs,

            cumulativeMemory,
            totalMemoryReservation > 0 ? succinctBytes(totalMemoryReservation) : succinctBytes(0),
            peakMemoryReservation > 0 ? succinctBytes(peakMemoryReservation) : succinctBytes(0),
            new Duration(totalScheduledTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
            new Duration(totalCpuTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
            new Duration(totalUserTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
            new Duration(totalBlockedTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
            fullyBlocked,
            blockedReasons,
            processedInputDataSize > 0 ? succinctBytes(processedInputDataSize) : succinctBytes(0),
            processedInputPositions,
            outputDataSize > 0 ? succinctBytes(outputDataSize) : succinctBytes(0),
            outputPositions,
            operatorStatsList);

        return new QueryInfo(queryId,
            new SessionInfo(session.getSchema(), session.getUser(), session.getServerVariables(),
                session.getUserDefVariables()),
            state,
            isScheduled(rootStage),
            querySelf,
            query,
            queryStats,
            rootStage,
            failureInfo,
            errorCode,
            completeInfo);
    }

    public Duration getElapsedTime() {
        return this.queryStateTimer.getElapsedTime();
    }

    public QueryState getQueryState() {
        return queryState.get();
    }

    @Override
    public boolean isDone() {
        return queryState.get().isDone();
    }

    public boolean transitionToPlanning() {
        this.queryStateTimer.beginPlanning();
        return queryState.compareAndSet(QUEUED, PLANNING);
    }

    public boolean transitionToStarting() {
        this.queryStateTimer.beginStarting();
        return queryState.setIf(STARTING, currentState -> currentState == QUEUED || currentState == PLANNING);
    }

    public boolean transitionToRunning() {
        this.queryStateTimer.beginRunning();
        return queryState.setIf(RUNNING,
            currentState -> currentState != RUNNING && currentState != FINISHING && !currentState.isDone());
    }

    public boolean transitionToFinishing() {
        this.queryStateTimer.beginFinishing();
        if (!queryState.setIf(FINISHING, currentState -> currentState != FINISHING && !currentState.isDone())) {
            return false;
        }
        transitionToFinished();
        return true;
    }

    private boolean transitionToFinished() {
        this.queryStateTimer.endQuery();
        return queryState.setIf(FINISHED, currentState -> !currentState.isDone());
    }

    @Override
    public void failed(Throwable cause) {
        transitionToFailed(cause);
    }

    public boolean transitionToFailed(Throwable throwable) {
        requireNonNull(throwable, "throwable is null");
        this.queryStateTimer.endQuery();

        // NOTE: The failure cause must be set before triggering the state change, so
        // listeners can observe the exception. This is safe because the failure cause
        // can only be observed if the transition to FAILED is successful.
        failureCause.compareAndSet(null, Failures.toFailure(throwable));

        boolean failed = queryState.setIf(FAILED, currentState -> !currentState.isDone());
        if (failed) {
            log.error(queryId + ": the sql is " + query + " , and it has error: " +
                ExceptionUtils.getFullStackTrace(throwable));
        }
        return failed;
    }

    public boolean transitionToCanceled() {
        this.queryStateTimer.endQuery();

        // NOTE: The failure cause must be set before triggering the state change, so
        // listeners can observe the exception. This is safe because the failure cause
        // can only be observed if the transition to FAILED is successful.
        failureCause.compareAndSet(null,
            Failures.toFailure(new TddlRuntimeException(ErrorCode.ERR_USER_CANCELED, "Query was canceled")));

        boolean canceled = queryState.setIf(FAILED, currentState -> !currentState.isDone());
        return canceled;
    }

    @Override
    public void addStateChangeListener(StateMachine.StateChangeListener<QueryState> stateChangeListener) {
        queryState.addStateChangeListener(stateChangeListener);
    }

    public void removeStateChangeListener(StateMachine.StateChangeListener<QueryState> stateChangeListener) {
        queryState.removeStateChangeListener(stateChangeListener);
    }

    public Duration waitForStateChange(QueryState currentState, Duration maxWait)
        throws InterruptedException {
        return queryState.waitForStateChange(currentState, maxWait);
    }

    public void recordHeartbeat() {
        queryStateTimer.recordHeartbeat();
    }

    public void recordDistributedPlanningTime(long distributedPlanningStart) {
        queryStateTimer.recordDistributedPlanningTime(distributedPlanningStart);
    }

    private static boolean isScheduled(Optional<StageInfo> rootStage) {
        if (!rootStage.isPresent()) {
            return false;
        }
        return getAllStages(rootStage).stream()
            .map(StageInfo::getState)
            .allMatch(state -> (state == StageState.RUNNING || state == StageState.FLUSHING) || state.isDone());
    }

    public Optional<QueryInfo> getFinalQueryInfo() {
        return finalQueryInfo.get();
    }

    public QueryInfo updateQueryInfo(Optional<StageInfo> stageInfo, URI querySelf) {
        QueryInfo queryInfo = getQueryInfo(stageInfo, querySelf);
        if (queryInfo.isFinalQueryInfo()) {
            blocked.set(queryInfo);
            finalQueryInfo.compareAndSet(Optional.empty(), Optional.of(queryInfo));
        }
        return queryInfo;
    }

    public SettableFuture<QueryInfo> getBlockedFinalQueryInfo() {
        return blocked;
    }

    public void pruneQueryInfo() {
        Optional<QueryInfo> finalInfo = finalQueryInfo.get();
        if (!finalInfo.isPresent() || !finalInfo.get().getOutputStage().isPresent()) {
            return;
        }

        QueryInfo queryInfo = finalInfo.get();
        StageInfo outputStage = queryInfo.getOutputStage().get();
        StageInfo prunedOutputStage = new StageInfo(
            outputStage.getStageId(),
            outputStage.getState(),
            outputStage.getSelf(),
            null, // Remove the plan
            outputStage.getTypes(),
            outputStage.getStageStats(),
            ImmutableList.of(), // Remove the tasks
            ImmutableList.of(), // Remove the substages
            outputStage.getFailureCause()
        );

        QueryInfo prunedQueryInfo = new QueryInfo(
            queryInfo.getQueryId(),
            queryInfo.getSession(),
            queryInfo.getState(),
            queryInfo.isScheduled(),
            queryInfo.getSelf(),
            queryInfo.getQuery(),
            queryInfo.getQueryStats(),
            Optional.of(prunedOutputStage),
            queryInfo.getFailureInfo(),
            queryInfo.getErrorCode(),
            queryInfo.isCompleteInfo());
        finalQueryInfo.compareAndSet(finalInfo, Optional.of(prunedQueryInfo));
    }

    public DateTime getQueryEndTime() {
        return queryStateTimer.getEndTime();
    }

    public long getExecuteCreateMillis() {
        return queryStateTimer.getCreateMillis();
    }

    public Throwable toFailException() {
        ExecutionFailureInfo failureInfo = failureCause.get();
        if (failureInfo != null) {
            throw GeneralUtil.nestedException(failureInfo.toException());
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Query failed(reason unknown)");
        }
    }

    public DateTime getLastHeartbeat() {
        return queryStateTimer.getLastHeartbeat();
    }
}
