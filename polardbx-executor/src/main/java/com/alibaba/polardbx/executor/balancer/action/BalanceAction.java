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

import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

/**
 * @author moyi
 * @since 202103
 */
public interface BalanceAction {

    /**
     * On which schema this action applies
     */
    String getSchema();

    /**
     * The name of this action
     */
    @JSONField(deserialize = false, serialize = false)
    String getName();

    /**
     * Execution steps of this action
     */
    @JSONField(deserialize = false, serialize = false)
    String getStep();

    /**
     * Convert to a ddl job
     * Which means this action could not be executed directly, but execute as a ddl-job
     */
    ExecutableDdlJob toDdlJob(ExecutionContext ec);

    @JSONField(deserialize = false, serialize = false)
    default Long getBackfillRows() {
        return 0L;
    }

    @JSONField(deserialize = false, serialize = false)
    default Long getDiskSize() {
        return 0L;
    }

    @JSONField(deserialize = false, serialize = false)
    default double getLogicalTableCount() {
        return 0;
    }
}
