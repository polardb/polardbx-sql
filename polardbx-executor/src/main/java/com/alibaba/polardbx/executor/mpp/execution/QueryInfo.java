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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.exception.code.ErrorType;
import com.alibaba.polardbx.executor.mpp.client.FailureInfo;
import com.alibaba.polardbx.util.MoreObjects;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.net.URI;
import java.util.Optional;

import static com.alibaba.polardbx.executor.mpp.execution.StageInfo.getAllStages;
import static java.util.Objects.requireNonNull;

@Immutable
public class QueryInfo {
    private final String queryId;
    private final SessionInfo session;
    private final QueryState state;
    private final boolean scheduled;
    private final URI self;
    private final String query;
    private final QueryStats queryStats;
    private final Optional<StageInfo> outputStage;
    private final FailureInfo failureInfo;
    private final ErrorType errorType;
    private final ErrorCode errorCode;
    private final boolean completeInfo;

    @JsonCreator
    public QueryInfo(
        @JsonProperty("queryId")
            String queryId,
        @JsonProperty("session")
            SessionInfo session,
        @JsonProperty("state")
            QueryState state,
        @JsonProperty("scheduled")
            boolean scheduled,
        @JsonProperty("self")
            URI self,
        @JsonProperty("query")
            String query,
        @JsonProperty("queryStats")
            QueryStats queryStats,
        @JsonProperty("outputStage")
            Optional<StageInfo> outputStage,
        @JsonProperty("failureInfo")
            FailureInfo failureInfo,
        @JsonProperty("errorCode")
            ErrorCode errorCode,
        @JsonProperty("completeInfo")
            boolean completeInfo
    ) {
        requireNonNull(queryId, "queryId is null");
        requireNonNull(session, "session is null");
        requireNonNull(state, "state is null");
        requireNonNull(self, "self is null");
        requireNonNull(query, "query is null");
        requireNonNull(outputStage, "outputStage is null");
        this.queryId = queryId;
        this.session = session;
        this.state = state;
        this.scheduled = scheduled;
        this.self = self;
        this.query = query;
        this.queryStats = queryStats;
        this.outputStage = outputStage;
        this.failureInfo = failureInfo;
        this.errorType = errorCode == null ? null : errorCode.getType();
        this.errorCode = errorCode;
        this.completeInfo = completeInfo;
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
    public boolean isScheduled() {
        return scheduled;
    }

    @JsonProperty
    public URI getSelf() {
        return self;
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
    public Optional<StageInfo> getOutputStage() {
        return outputStage;
    }

    @Nullable
    @JsonProperty
    public FailureInfo getFailureInfo() {
        return failureInfo;
    }

    @Nullable
    @JsonProperty
    public ErrorType getErrorType() {
        return errorType;
    }

    @Nullable
    @JsonProperty
    public ErrorCode getErrorCode() {
        return errorCode;
    }

    @JsonProperty
    public boolean isFinalQueryInfo() {
        return state.isDone() && getAllStages(outputStage).stream().allMatch(StageInfo::isFinalStageInfo);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("queryId", queryId).add("state", state).toString();
    }

    public boolean isCompleteInfo() {
        return completeInfo;
    }

    public String toPlanString() {
        MoreObjects.ToStringHelper toStringHelper =
            MoreObjects.toStringHelper(this).add("queryId", queryId).add("state", state)
                .add("queryStats", queryStats.toPlanString());
        if (outputStage.isPresent()) {
            toStringHelper.add("stage", outputStage.get().toPlanString());
        }
        return toStringHelper.toString();
    }

    public QueryInfo summary() {
        if (!this.outputStage.isPresent()) {
            return this;
        }

        if (this.outputStage.get().getTasks().isEmpty()) {
            return this;
        }

        if (this.outputStage.get().getTasks().get(0).getStats() == null) {
            return this;
        }

        return new QueryInfo(this.queryId, this.session, this.state, this.scheduled, this.self, this.query,
            this.queryStats, Optional.of(outputStage.get().summary()), failureInfo, errorCode, completeInfo
        );
    }
}
