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

package com.alibaba.polardbx.optimizer.core.rel.util;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

public class PartitionPlanExplainUtil {

    public static boolean checkIfNewPartDbTbl(String schemaName,
                                          List<String> tableNames,
                                          SqlNode selectedPartitions,
                                          ExecutionContext executionContext) {
        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        if (isNewPartDb) {
            PartitionInfoManager partInfoMgr = executionContext.getSchemaManager(schemaName)
                    .getTddlRuleManager()
                    .getPartitionInfoManager();
            for (int i = 0; i < tableNames.size(); i++) {
                String tb = tableNames.get(i);
                PartitionInfo partInfo = partInfoMgr.getPartitionInfo(tb);
                validateSelectedPartitions(selectedPartitions, true, partInfo);
            }
            return true;
        } else {
            validateSelectedPartitions(selectedPartitions,false, null);
            return false;
        }
    }

    private static void validateSelectedPartitions(SqlNode selectedPartitions, boolean isNewPartDb, PartitionInfo partInfo) {
        if (selectedPartitions != null) {
            if (!isNewPartDb) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Do not support table with mysql partition.");
            } else {
                boolean isPartTbl = partInfo.isPartitionedTable();
                if (!isPartTbl) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        "PARTITION () clause on non partitioned table");
                }
            }

        }
    }
}
