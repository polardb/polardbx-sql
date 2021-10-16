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
package com.alibaba.polardbx.executor.mpp.execution.scheduler;

import com.alibaba.polardbx.executor.mpp.execution.SqlStageExecution;
import com.alibaba.polardbx.executor.mpp.execution.StageState;
import com.alibaba.polardbx.executor.mpp.util.ImmutableCollectors;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.mpp.execution.StageState.FLUSHING;
import static com.alibaba.polardbx.executor.mpp.execution.StageState.RUNNING;
import static com.alibaba.polardbx.executor.mpp.execution.StageState.SCHEDULED;

public class AllAtOnceExecutionSchedule
    implements ExecutionSchedule {
    private final List<SqlStageExecution> schedulingStages;

    public AllAtOnceExecutionSchedule(Collection<SqlStageExecution> stages) {
        List<Set<Integer>> phases =
            PhasedExecutionSchedule
                .extractPhases(
                    stages.stream().map(SqlStageExecution::getFragment).collect(ImmutableCollectors.toImmutableList()));

        Map<Integer, SqlStageExecution> stagesByFragmentId = stages.stream().collect(
            ImmutableCollectors.toImmutableMap(stage -> stage.getFragment().getId()));

        // create a mutable list of mutable sets of stages, so we can remove completed stages
        schedulingStages = new ArrayList<>();
        for (Set<Integer> phase : phases) {
            schedulingStages.addAll(phase.stream()
                .map(stagesByFragmentId::get).collect(Collectors.toList()));
        }
    }

    @Override
    public List<SqlStageExecution> getStagesToSchedule() {
        for (Iterator<SqlStageExecution> iterator = schedulingStages.iterator(); iterator.hasNext(); ) {
            StageState state = iterator.next().getState();
            if (state == SCHEDULED || state == RUNNING || state == FLUSHING || state.isDone()) {
                iterator.remove();
            }
        }
        return schedulingStages;
    }

    @Override
    public boolean isFinished() {
        return schedulingStages.isEmpty();
    }
}
