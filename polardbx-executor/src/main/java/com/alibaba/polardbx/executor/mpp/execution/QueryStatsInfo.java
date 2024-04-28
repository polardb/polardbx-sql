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

import com.alibaba.polardbx.executor.mpp.operator.TaskStats;
import com.alibaba.polardbx.util.MoreObjects;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

@Immutable
public class QueryStatsInfo {
    private final String queryId;
    private final SessionInfo session;
    private final QueryState state;
    private final String query;
    private final QueryStats queryStats;
    private final StageStatsInfo outputStage;

    @JsonCreator
    public QueryStatsInfo(
        @JsonProperty("queryId")
        String queryId,
        @JsonProperty("session")
        SessionInfo session,
        @JsonProperty("state")
        QueryState state,
        @JsonProperty("query")
        String query,
        @JsonProperty("queryStats")
        QueryStats queryStats,
        @JsonProperty("outputStage")
        StageStatsInfo outputStage
    ) {
        requireNonNull(queryId, "queryId is null");
        requireNonNull(session, "session is null");
        requireNonNull(state, "state is null");
        requireNonNull(query, "query is null");
        requireNonNull(outputStage, "outputStage is null");
        this.queryId = queryId;
        this.session = session;
        this.state = state;
        this.query = query;
        this.queryStats = queryStats;
        this.outputStage = outputStage;
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
    public QueryStats getQueryStats() {
        return queryStats;
    }

    @JsonProperty
    public StageStatsInfo getOutputStage() {
        return outputStage;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("queryId", queryId).add("state", state).toString();
    }

    public static QueryStatsInfo from(QueryInfo queryInfo) {
        return new QueryStatsInfo(queryInfo.getQueryId(), queryInfo.getSession(), queryInfo.getState(),
            queryInfo.getQuery(),
            queryInfo.getQueryStats(),
            StageStatsInfo.from(queryInfo.getOutputStage())
        );
    }

    public static class StageStatsInfo {

        private final StageId stageId;
        private final StageState state;
        private final PlanInfo plan;
        private final StageStats stageStats;
        private final List<TaskStatsInfo> taskStats;
        private final List<StageStatsInfo> subStages;

        public static StageStatsInfo from(Optional<StageInfo> stage) {
            if (!stage.isPresent()) {
                return null;
            }
            StageInfo stageInfo = stage.get();
            List<TaskStatsInfo> tasks = Optional.ofNullable(stageInfo.getTasks())
                .map(taskInfos ->
                    taskInfos.stream().map(TaskStatsInfo::from)
                        .collect(Collectors.toList()))
                .orElse(null);
            List<StageStatsInfo> subStages = Optional.ofNullable(stageInfo.getSubStages())
                .map(subStage ->
                    subStage.stream()
                        .map(subStageInfo -> StageStatsInfo.from(Optional.of(subStageInfo)))
                        .collect(Collectors.toList()))
                .orElse(null);
            return new StageStatsInfo(stageInfo.getStageId(), stageInfo.getState(), stageInfo.getPlan(),
                stageInfo.getStageStats(), tasks, subStages);
        }

        @JsonCreator
        public StageStatsInfo(
            @JsonProperty("stageId")
            StageId stageId,
            @JsonProperty("state")
            StageState state,
            @JsonProperty("plan")
            PlanInfo plan,
            @JsonProperty("stageStats")
            StageStats stageStats,
            @JsonProperty("tasks")
            List<TaskStatsInfo> taskStats,
            @JsonProperty("subStages")
            List<StageStatsInfo> subStages) {
            this.stageId = stageId;
            this.state = state;
            this.plan = plan;
            this.stageStats = stageStats;
            this.taskStats = taskStats;
            this.subStages = subStages;
        }

        @JsonProperty
        public StageId getStageId() {
            return stageId;
        }

        @JsonProperty
        public StageState getState() {
            return state;
        }

        @JsonProperty
        public PlanInfo getPlan() {
            return plan;
        }

        @JsonProperty
        public StageStats getStageStats() {
            return stageStats;
        }

        @JsonProperty
        public List<TaskStatsInfo> getTaskStats() {
            return taskStats;
        }

        @JsonProperty
        public List<StageStatsInfo> getSubStages() {
            return subStages;
        }
    }

    public static class TaskStatsInfo {

        private final TaskStats detailedStats;
        private final TaskStatus taskStatus;
        private final boolean complete;

        private final int completedPipelineExecs;
        private final int totalPipelineExecs;
        private final long elapsedTimeMillis;
        private final long totalCpuTime;
        private final long processTimeMillis;
        private final long processWall;
        private final long pullDataTimeMillis;
        private final long deliveryTimeMillis;

        @JsonCreator
        public TaskStatsInfo(
            @JsonProperty("detailedStats")
            TaskStats detailedStats,
            @JsonProperty("taskStatus")
            TaskStatus taskStatus,
            @JsonProperty("complete")
            boolean complete,
            @JsonProperty("completedPipelineExecs")
            int completedPipelineExecs,
            @JsonProperty("totalPipelineExecs")
            int totalPipelineExecs,
            @JsonProperty("elapsedTime")
            long elapsedTimeMillis,
            @JsonProperty("totalCpuTime")
            long totalCpuTime,
            @JsonProperty("processTime")
            long processTimeMillis,
            @JsonProperty("processWall")
            long processWall,
            @JsonProperty("pullDataTimeMillis")
            long pullDataTimeMillis,
            @JsonProperty("deliveryTimeMillis")
            long deliveryTimeMillis) {
            this.detailedStats = detailedStats;
            this.taskStatus = taskStatus;
            this.complete = complete;
            this.completedPipelineExecs = completedPipelineExecs;
            this.totalPipelineExecs = totalPipelineExecs;
            this.elapsedTimeMillis = elapsedTimeMillis;
            this.totalCpuTime = totalCpuTime;
            this.processTimeMillis = processTimeMillis;
            this.processWall = processWall;
            this.pullDataTimeMillis = pullDataTimeMillis;
            this.deliveryTimeMillis = deliveryTimeMillis;
        }

        public static TaskStatsInfo from(TaskInfo taskInfo) {
            if (taskInfo == null) {
                return null;
            }
            return new TaskStatsInfo(taskInfo.getTaskStats(), taskInfo.getTaskStatus(), taskInfo.isComplete(),
                taskInfo.getCompletedPipelineExecs(), taskInfo.getTotalPipelineExecs(),
                taskInfo.getElapsedTimeMillis(), taskInfo.getTotalCpuTime(), taskInfo.getProcessTimeMillis(),
                taskInfo.getProcessWall(), taskInfo.getPullDataTimeMillis(), taskInfo.getDeliveryTimeMillis());
        }

        @JsonProperty
        public TaskStats getDetailedStats() {
            return detailedStats;
        }

        @JsonProperty
        public TaskStatus getTaskStatus() {
            return taskStatus;
        }

        @JsonProperty
        public boolean isComplete() {
            return complete;
        }

        @JsonProperty
        public int getCompletedPipelineExecs() {
            return completedPipelineExecs;
        }

        @JsonProperty
        public int getTotalPipelineExecs() {
            return totalPipelineExecs;
        }

        @JsonProperty
        public long getElapsedTimeMillis() {
            return elapsedTimeMillis;
        }

        @JsonProperty
        public long getTotalCpuTime() {
            return totalCpuTime;
        }

        @JsonProperty
        public long getProcessTimeMillis() {
            return processTimeMillis;
        }

        @JsonProperty
        public long getProcessWall() {
            return processWall;
        }

        @JsonProperty
        public long getPullDataTimeMillis() {
            return pullDataTimeMillis;
        }

        @JsonProperty
        public long getDeliveryTimeMillis() {
            return deliveryTimeMillis;
        }
    }
}
