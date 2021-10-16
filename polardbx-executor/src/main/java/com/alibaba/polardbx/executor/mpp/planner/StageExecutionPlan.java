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
package com.alibaba.polardbx.executor.mpp.planner;

import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.executor.mpp.split.SplitInfo;
import com.alibaba.polardbx.util.MoreObjects;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class StageExecutionPlan {
    private final PlanFragment fragment;
    private final SplitInfo splitInfo;
    private final List<SplitInfo> expandSplitInfos;
    private final List<StageExecutionPlan> subStages;

    public StageExecutionPlan(
        PlanFragment fragment,
        SplitInfo splitInfo,
        List<SplitInfo> expandSplitInfos,
        List<StageExecutionPlan> subStages) {
        this.fragment = requireNonNull(fragment, "fragment is null");
        this.splitInfo = splitInfo;
        this.expandSplitInfos = expandSplitInfos;
        this.subStages = ImmutableList.copyOf(requireNonNull(subStages, "dependencies is null"));
    }

    public PlanFragment getFragment() {
        return fragment;
    }

    public SplitInfo getSplitInfo() {
        return splitInfo;
    }

    public List<SplitInfo> getExpandInfo() {
        return expandSplitInfos;
    }

    public List<StageExecutionPlan> getSubStages() {
        return subStages;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("fragment", fragment)
            .add("subStages", subStages)
            .toString();
    }
}
