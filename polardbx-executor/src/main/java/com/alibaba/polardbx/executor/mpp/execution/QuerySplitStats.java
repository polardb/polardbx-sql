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

import com.alibaba.polardbx.executor.mpp.operator.DriverStats;
import com.alibaba.polardbx.util.MoreObjects;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static java.util.Objects.requireNonNull;

@Immutable
public class QuerySplitStats {
    private final String queryId;
    private final SessionInfo session;
    private final QueryState state;
    private final String query;
    private final List<DriverStats> driverStats;

    @JsonCreator
    public QuerySplitStats(
        @JsonProperty("queryId")
        String queryId,
        @JsonProperty("session")
        SessionInfo session,
        @JsonProperty("state")
        QueryState state,
        @JsonProperty("query")
        String query,
        @JsonProperty("driverStats")
        List<DriverStats> driverStats
    ) {
        requireNonNull(queryId, "queryId is null");
        requireNonNull(session, "session is null");
        requireNonNull(state, "state is null");
        requireNonNull(query, "query is null");
        this.queryId = queryId;
        this.session = session;
        this.state = state;
        this.query = query;
        this.driverStats = driverStats;
    }

    @JsonProperty
    public String getQueryId() {
        return queryId;
    }

    @JsonProperty
    public SessionInfo getSession() {
        return session;
    }

    @JsonProperty
    public QueryState getState() {
        return state;
    }

    @JsonProperty
    public String getQuery() {
        return query;
    }

    @JsonProperty
    public List<DriverStats> getDriverStats() {
        return driverStats;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("queryId", queryId).add("state", state).toString();
    }

    public static QuerySplitStats from(QueryStatsInfo queryInfo) {
        List<DriverStats> driverStats = new ArrayList<>();
        QueryStatsInfo.StageStatsInfo outputStage = queryInfo.getOutputStage();
        addStageSplits(outputStage, driverStats);

        driverStats.sort(Comparator.comparing(DriverStats::getDriverId));
        return new QuerySplitStats(queryInfo.getQueryId(), queryInfo.getSession(), queryInfo.getState(),
            queryInfo.getQuery(),
            driverStats
        );
    }

    private static void addStageSplits(QueryStatsInfo.StageStatsInfo outputStage,
                                       List<DriverStats> driverStats) {
        if (outputStage.getTaskStats() != null) {
            for (QueryStatsInfo.TaskStatsInfo taskStat : outputStage.getTaskStats()) {
                driverStats.addAll(taskStat.getDetailedStats().getDriverStats());
            }
        }

        if (outputStage.getSubStages() != null) {
            for (QueryStatsInfo.StageStatsInfo subStage : outputStage.getSubStages()) {
                addStageSplits(subStage, driverStats);
            }
        }

    }
}
