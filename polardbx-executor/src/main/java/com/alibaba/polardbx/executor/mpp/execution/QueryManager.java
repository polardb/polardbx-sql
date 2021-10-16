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

import com.alibaba.polardbx.executor.mpp.Session;
import com.alibaba.polardbx.executor.mpp.server.StatementResource;
import io.airlift.units.Duration;
import org.apache.calcite.rel.RelNode;

import java.util.List;

public interface QueryManager {
    List<QueryInfo> getAllQueryInfo();

    List<TaskContext> getAllLocalQueryContext();

    Duration waitForStateChange(String queryId, QueryState currentState, Duration maxWait)
        throws InterruptedException;

    QueryInfo getQueryInfo(String queryId);

    QueryState getQueryState(String queryId);

    void recordHeartbeat(String queryId);

    QueryExecution createClusterQuery(Session session, String query, StatementResource.Query sQuery);

    QueryExecution createLocalQuery(Session session, String query, RelNode relNode);

    void removeLocalQuery(String queryId, boolean needReserved);

    void cancelQuery(String queryId);

    void cancelStage(StageId stageId);

    QueryExecution getQueryExecution(String queryId);

    void failQuery(String queryId, Throwable cause);

    /**
     * 支持重新加载系统级配置项
     */
    void reloadConfig();

    long getTotalQueries();
}
