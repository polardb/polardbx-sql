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

package com.alibaba.polardbx.optimizer.optimizeralert;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

public interface OptimizerAlertLogger {

    /**
     * increase alert count by one
     */
    void inc();

    /**
     * log the alert when it's not too frequent
     *
     * @param ec ExecutionContext of the query
     * @return return if log success
     */
    boolean logDetail(ExecutionContext ec);

    /**
     * collect alert count since last schedule job
     *
     * @return pair of OptimizerAlertType, alert count since last schedule job
     */
    Pair<OptimizerAlertType, Long> collectByScheduleJob();

    /**
     * collect total alert count
     *
     * @return pair of OptimizerAlertType, total alert count
     */
    Pair<OptimizerAlertType, Long> collectByView();
}