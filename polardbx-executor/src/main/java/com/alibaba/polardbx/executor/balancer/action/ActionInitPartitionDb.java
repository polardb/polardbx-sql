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

import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

/**
 * Initialize physical database for partition-table
 * 1. Create physical database
 * 2. Copy existed broadcast tables
 *
 * @author moyi
 * @since 2021/10
 */
public class ActionInitPartitionDb implements BalanceAction {

    private String schema;

    public ActionInitPartitionDb(String schema) {
        this.schema = schema;
    }

    public static String getActionName() {
        return "ActionInitPartitionDb";
    }

    @Override
    public String getSchema() {
        return schema;
    }

    @Override
    public String getName() {
        return getActionName();
    }

    @Override
    public String getStep() {
        return "refresh topology";
    }

    @Override
    public ExecutableDdlJob toDdlJob(ExecutionContext ec) {
        final String sqlRefreshTopology = "refresh topology";
        return ActionUtils.convertToDelegatorJob(schema, sqlRefreshTopology);
    }
}
