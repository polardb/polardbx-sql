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
import com.google.common.collect.Sets;

import java.util.LinkedHashSet;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class TaskSource {
    private final int planNodeId;
    private final LinkedHashSet<ScheduledSplit> splits;
    private final boolean noMoreSplits;
    private final boolean expand;

    @JsonCreator
    public TaskSource(
        @JsonProperty("planNodeId") int planNodeId,
        @JsonProperty("splits") Set<ScheduledSplit> splits,
        @JsonProperty("noMoreSplits") boolean noMoreSplits,
        @JsonProperty("expand") boolean expand) {
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.splits = new LinkedHashSet<>(requireNonNull(splits, "splits is null"));
        this.noMoreSplits = noMoreSplits;
        this.expand = expand;
    }

    @JsonProperty
    public int getPlanNodeId() {
        return planNodeId;
    }

    public Integer getIntegerId() {
        return planNodeId;
    }

    @JsonProperty
    public LinkedHashSet<ScheduledSplit> getSplits() {
        return splits;
    }

    public void addSplit(ScheduledSplit split) {
        this.splits.add(split);
    }

    @JsonProperty
    public boolean isNoMoreSplits() {
        return noMoreSplits;
    }

    public TaskSource update(TaskSource source) {
        if (planNodeId != source.getPlanNodeId()) {
            checkArgument(planNodeId == source.getPlanNodeId(), "Expected source %s, but got source %s", planNodeId,
                source.getPlanNodeId());
        }

        if (isNewer(source)) {
            // assure the new source is properly formed
            // we know that either the new source one has new splits and/or it is marking the source as closed
            checkArgument(!noMoreSplits || source.isNoMoreSplits(),
                "Source %s has new splits, but no more splits already set", planNodeId);

            LinkedHashSet<ScheduledSplit> newSplits = Sets.newLinkedHashSet();
            newSplits.addAll(splits);
            newSplits.addAll(source.getSplits());

            return new TaskSource(planNodeId, newSplits, source.isNoMoreSplits(), expand);
        } else {
            // the specified source is older than this one
            return this;
        }
    }

    private boolean isNewer(TaskSource source) {
        // the specified source is newer if it changes the no more
        // splits flag or if it contains new splits
        return (!noMoreSplits && source.isNoMoreSplits()) || (!splits.containsAll(source.getSplits()));
    }

    @JsonProperty
    public boolean isExpand() {
        return expand;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
            .add("planNodeId", planNodeId)
            .add("splits", splits)
            .add("noMoreSplits", noMoreSplits)
            .add("expand", expand)
            .toString();
    }
}
