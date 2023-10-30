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

package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertManager;
import com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import java.util.List;

public class OptimizerAlertViewSyncAction implements ISyncAction {

    public OptimizerAlertViewSyncAction() {
    }

    @Override
    public ResultCursor sync() {
        ArrayResultCursor result = new ArrayResultCursor("OptimizerAlertView");
        result.addColumn("COMPUTE_NODE", DataTypes.StringType);
        result.addColumn("ALERT_TYPE", DataTypes.StringType);
        result.addColumn("ALERT_COUNT", DataTypes.LongType);

        result.initMeta();
        List<Pair<OptimizerAlertType, Long>> collects = OptimizerAlertManager.getInstance().collectByView();
        for (Pair<OptimizerAlertType, Long> collect : collects) {
            result.addRow(new Object[] {
                TddlNode.getHost() + ":" + TddlNode.getPort(),
                collect.getKey().name(),
                collect.getValue()
            });
        }
        return result;
    }
}

