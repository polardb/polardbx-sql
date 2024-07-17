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

import com.alibaba.polardbx.common.utils.bloomfilter.BloomFilterInfo;
import com.alibaba.polardbx.executor.mpp.Session;
import io.airlift.units.Duration;
import org.apache.calcite.rel.RelNode;
import org.joda.time.DateTime;

import java.util.List;
import java.util.concurrent.Future;

import static java.util.Objects.requireNonNull;

public abstract class QueryExecution {

    protected QueryStateMachine stateMachine;

    public String getQueryId() {
        return stateMachine.getQueryId();
    }

    public Future<QueryInfo> getFinalQueryInfo() {
        return stateMachine.getBlockedFinalQueryInfo();
    }

    public DateTime getQueryEndTime() {
        return stateMachine.getQueryEndTime();
    }

    public QueryState getState() {
        return stateMachine.getQueryState();
    }

    public Duration waitForStateChange(QueryState currentState, Duration maxWait) throws InterruptedException {
        return stateMachine.waitForStateChange(currentState, maxWait);
    }

    public Session getSession() {
        return stateMachine.getSession();
    }

    public QueryStateMachine queryStateMachine() {
        return this.stateMachine;
    }

    public void fail(Throwable cause) {
        requireNonNull(cause, "cause is null");
        stateMachine.transitionToFailed(cause);
    }

    public void cancelQuery() {
        stateMachine.transitionToCanceled();
    }

    public void recordHeartbeat() {
        stateMachine.recordHeartbeat();
    }

    // XXX: This should be removed when the client protocol is improved, so that we don't need to hold onto so much query history
    public void pruneInfo() {
        stateMachine.pruneQueryInfo();
    }

    public void addStateChangeListener(StateMachine.StateChangeListener<QueryState> stateChangeListener) {
        stateMachine.addStateChangeListener(stateChangeListener);
    }

    public void removeStateChangeListener(StateMachine.StateChangeListener<QueryState> stateChangeListener) {
        stateMachine.removeStateChangeListener(stateChangeListener);
    }

    public interface QueryExecutionFactory<T extends QueryExecution> {
        T createQueryExecution(QueryManager queryManager, String query, RelNode physicalPlan, Session session);

        void setThreadPoolExecutor(int poolSize);
    }

    public abstract boolean isNeedReserveAfterExpired();

    public abstract QueryInfo getQueryInfo();

    public abstract long getTotalMemoryReservation();

    public abstract Duration getTotalCpuTime();

    public abstract TaskContext getTaskContext();

    public abstract void start();

    public abstract void cancelStage(StageId stageId);

    public abstract void close(Throwable throwable);

    public abstract void mergeBloomFilter(List<BloomFilterInfo> filterInfos);
}
