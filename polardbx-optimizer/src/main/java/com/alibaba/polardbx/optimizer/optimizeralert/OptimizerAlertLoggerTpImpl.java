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

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.Pair;

public class OptimizerAlertLoggerTpImpl extends OptimizerAlertLoggerBaseImpl {

    public OptimizerAlertLoggerTpImpl() {
        super();
        this.optimizerAlertType = OptimizerAlertType.TP_SLOW;
    }

    @Override
    public Pair<OptimizerAlertType, Long> collectByScheduleJob() {
        Pair<OptimizerAlertType, Long> collect = super.collectByScheduleJob();

        if (collect != null && collect.getValue() < DynamicConfig.getInstance().getTpSlowAlertThreshold()) {
            return null;
        }
        return collect;
    }
}
