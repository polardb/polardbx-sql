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

package com.alibaba.polardbx.executor.balancer.action;

import com.alibaba.polardbx.executor.ddl.job.task.shared.EmptyTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.scaleout.ScaleOutUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * Action that move multiple groups concurrently
 *
 * @author moyi
 * @since 2021/10
 */
public class ActionMoveGroups implements BalanceAction {

    private final String schema;
    private final List<ActionMoveGroup> actions;

    public ActionMoveGroups(String schema, List<ActionMoveGroup> actions) {
        this.schema = schema;
        this.actions = actions;
    }

    @Override
    public String getSchema() {
        return schema;
    }

    @Override
    public String getName() {
        return "MoveGroups";
    }

    @Override
    public String getStep() {
        return StringUtils.join(this.actions, ",");
    }

    /*
     * Convert it to concurrent move-database jobs like:
     *
     * Empty
     * /   \
     * MoveDatabase | MoveDatabase | MoveDatabase
     * \                   /           /
     *  Empty
     *
     * @param ec
     * @return
     */
    @Override
    public ExecutableDdlJob toDdlJob(ExecutionContext ec) {
        ExecutableDdlJob job = new ExecutableDdlJob();
        job.setMaxParallelism(ScaleOutUtils.getScaleoutTaskParallelism(ec));

        EmptyTask head = new EmptyTask(schema);
        job.addTask(head);
        job.labelAsHead(head);
        EmptyTask tail = new EmptyTask(schema);
        job.addTask(tail);
        job.labelAsTail(tail);

        for (ActionMoveGroup move : actions) {
            ExecutableDdlJob subJob = move.toDdlJob(ec);
            job.appendJobAfter(head, subJob);
            job.addTaskRelationship(subJob.getTail(), tail);
        }

        return job;
    }
}
