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

package com.alibaba.polardbx.executor.ddl.job.validator;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.metadb.limit.LimitValidator;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupInfoRecord;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupUtils;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.util.DbNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;

public class JoinGroupValidator {

    public static void validateJoinGroupInfo(String schemaName, String joinGroupName,
                                             ParamManager paramManager) {
        validateJoinGroupName(joinGroupName);
    }

    public static void validateJoinGroupName(String joinGroupName) {
        validateJoinGroupNameLength(joinGroupName);
        for (int i = 0; i < joinGroupName.length(); i++) {
            if (!DbNameUtil.isWord(joinGroupName.charAt(i))) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                    String.format(
                        "Failed to execute this command because the joinGroupName[%s] contains some invalid characters",
                        joinGroupName));
            }
        }
    }

    public static void validateJoinGroupNameLength(String joinGroupName) {
        LimitValidator.validateJoinGroupNameLength(joinGroupName);
    }

    public static void validateJoinGroupExistence(String schemaName, String joinGroupName) {
        JoinGroupInfoRecord
            joinGroupInfoRecord = JoinGroupUtils.getJoinGroupInfoByJoinGroupName(schemaName, joinGroupName);
        if (joinGroupInfoRecord == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_JOIN_GROUP_NOT_EXISTS,
                String.format(
                    "Failed to execute this command because the joinGroupName[%s] is not exists",
                    joinGroupName));
        }
    }

    public static void validateJoinGroupInfo(String schemaName, String tableGroupName, String joinGroupName, String errMsg, ExecutionContext ec, Connection metaDbConn) {
        TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
        TableGroupConfig tableGroupConfig =
            tableGroupInfoManager.getTableGroupConfigByName(tableGroupName);
        if (tableGroupConfig != null && GeneralUtil.isNotEmpty(tableGroupConfig.getTables())) {
            TablePartRecordInfoContext tablePartRecordInfoContext = tableGroupConfig.getTables().get(0);
            String tableName = tablePartRecordInfoContext.getTableName();
            TableMeta tableMeta = ec.getSchemaManager(schemaName).getTable(tableName);
            if (tableMeta.isGsi()) {
                tableName = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
            }
            JoinGroupInfoRecord
                record = JoinGroupUtils.getJoinGroupInfoByTable(schemaName, tableName, metaDbConn);
            if (record==null && StringUtils.isEmpty(joinGroupName)) {
                return;
            }
            if ((record == null && StringUtils.isNotEmpty(joinGroupName)) || (record != null && StringUtils.isEmpty(joinGroupName))) {
                throw new TddlRuntimeException(ErrorCode.ERR_JOIN_GROUP_NOT_MATCH, errMsg);
            }
            boolean isValid = joinGroupName.equalsIgnoreCase(record.joinGroupName);
            if (!isValid) {
                throw new TddlRuntimeException(ErrorCode.ERR_JOIN_GROUP_NOT_MATCH, errMsg);
            }
        }
    }
}
