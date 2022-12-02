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

package com.alibaba.polardbx.transaction.sync;

import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.transaction.TransactionManager;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.TddlConstants.INFORMATION_SCHEMA;

/**
 * @author wuzhe
 */
public class FetchTimerTaskInfoSyncAction implements ISyncAction {
    private String schemaName;

    public FetchTimerTaskInfoSyncAction() {

    }

    public FetchTimerTaskInfoSyncAction(String schemaName) {
        this.schemaName = schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    @Override
    public ResultCursor sync() {
        final ArrayResultCursor result = new ArrayResultCursor("TIMER TASK INFO");
        result.addColumn("Variable_name", DataTypes.StringType);
        result.addColumn("Value", DataTypes.StringType);
        // Only fetch information from leader.
        if (ExecUtils.hasLeadership(schemaName)) {
            // Get all current running parameters.
            final Set<String> schemaNames = OptimizerContext.getActiveSchemaNames().stream()
                .filter(schema -> !INFORMATION_SCHEMA.equalsIgnoreCase(schema))
                .collect(Collectors.toSet());
            for (String schemaName : schemaNames) {
                final TransactionManager transactionManager = TransactionManager.getInstance(schemaName);
                if (null != transactionManager) {
                    final Map<String, String> allActualParams = transactionManager.getCurrentTimerTaskParams();
                    for (Map.Entry<String, String> nameAndVal : allActualParams.entrySet()) {
                        final String paramName = nameAndVal.getKey();
                        final String paramVal = nameAndVal.getValue();
                        result.addRow(new Object[] {paramName + "_" + schemaName, paramVal});
                    }
                }
            }
        }
        return result;
    }
}
