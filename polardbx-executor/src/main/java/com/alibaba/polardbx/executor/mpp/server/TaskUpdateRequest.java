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

package com.alibaba.polardbx.executor.mpp.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.executor.mpp.OutputBuffers;
import com.alibaba.polardbx.executor.mpp.execution.SessionRepresentation;
import com.alibaba.polardbx.executor.mpp.execution.TaskSource;
import com.alibaba.polardbx.executor.mpp.planner.PlanFragment;
import com.alibaba.polardbx.common.utils.bloomfilter.BloomFilterInfo;

import java.net.URI;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class TaskUpdateRequest {
    private final Optional<PlanFragment> fragment;
    private final List<TaskSource> sources;
    private final OutputBuffers outputIds;
    private final SessionRepresentation session;
    private final Optional<List<BloomFilterInfo>> bloomfilters;
    private final URI runtimeFilterUpdateUri;

    @JsonCreator
    public TaskUpdateRequest(
        @JsonProperty("fragment") Optional<PlanFragment> fragment,
        @JsonProperty("sources") List<TaskSource> sources,
        @JsonProperty("outputIds") OutputBuffers outputIds,
        @JsonProperty("session") SessionRepresentation session,
        @JsonProperty("bloomfilters") Optional<List<BloomFilterInfo>> bloomfilters,
        @JsonProperty("runtimeFilterUpdateUri") URI runtimeFilterUpdateUri) {
        requireNonNull(fragment, "fragment is null");
        requireNonNull(sources, "sources is null");
        requireNonNull(outputIds, "outputIds is null");
        requireNonNull(session, "session is null");
        this.bloomfilters = bloomfilters;
        this.fragment = fragment;
        this.sources = ImmutableList.copyOf(sources);
        this.outputIds = outputIds;
        this.session = session;
        this.runtimeFilterUpdateUri = runtimeFilterUpdateUri;
    }

    @JsonProperty
    public Optional<List<BloomFilterInfo>> getBloomfilters() {
        return bloomfilters;
    }

    @JsonProperty
    public Optional<PlanFragment> getFragment() {
        return fragment;
    }

    @JsonProperty
    public List<TaskSource> getSources() {
        return sources;
    }

    @JsonProperty
    public OutputBuffers getOutputIds() {
        return outputIds;
    }

    @JsonProperty
    public SessionRepresentation getSession() {
        return session;
    }

    @JsonProperty
    public URI getRuntimeFilterUpdateUri() {
        return runtimeFilterUpdateUri;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
            .add("fragment", fragment)
            .add("sources", sources)
            .add("outputIds", outputIds)
            .add("session", session)
            .add("runtimeFilterUpdateURI", runtimeFilterUpdateUri)
            .toString();
    }
}
