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

package com.alibaba.polardbx.executor.ddl.job.task.tablegroup;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.job.validator.JoinGroupValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableGroupValidator;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupInfoAccessor;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupInfoRecord;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupUtils;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

@Getter
@TaskName(name = "JoinGroupValidateTask")
public class JoinGroupValidateTask extends BaseValidateTask {

    private List<String> tableGroups;
    private String tableName;
    private boolean onlyCompareTableGroup;

    @JSONCreator
    public JoinGroupValidateTask(String schemaName, List<String> tableGroups, String tableName, boolean onlyCompareTableGroup) {
        super(schemaName);
        this.tableGroups = tableGroups;
        this.tableName=tableName;
        this.onlyCompareTableGroup = onlyCompareTableGroup;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        if (GeneralUtil.isEmpty(tableGroups)) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,"the tableGroup list can't be empty");
        }
        String targetTableGroup = tableGroups.get(0);
        TableGroupValidator.validateTableGroupInfo(schemaName, targetTableGroup, true, executionContext.getParamManager());
        TableGroupInfoManager tableGroupInfoManager = OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
        TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(targetTableGroup);
        String targetJoinGroup = "";
        if (GeneralUtil.isNotEmpty(tableGroupConfig.getTables())) {
            TablePartRecordInfoContext tablePartRecordInfoContext = tableGroupConfig.getTables().get(0);
            String tbName = tablePartRecordInfoContext.getTableName();
            TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tbName);
            if (tableMeta.isGsi()) {
                tbName = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
            }
            JoinGroupInfoRecord record = JoinGroupUtils.getJoinGroupInfoByTable(schemaName, tbName, metaDbConnection);
            targetJoinGroup = record == null ? "" : record.joinGroupName;
        }
        if(onlyCompareTableGroup) {
            if (tableGroups.size() < 2) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,"the tableGroup list should great than 2");
            }
            for (int i=1; i< tableGroups.size();i++) {
                String sourceTableGroup = tableGroups.get(i);
                String errMsg = String.format(
                    "The joinGroup of tableGroup:[%s] is not match with the joinGroup of tableGroup[%s]",
                    sourceTableGroup, targetTableGroup);
                JoinGroupValidator.validateJoinGroupInfo(schemaName, sourceTableGroup, targetJoinGroup, errMsg,
                    executionContext, metaDbConnection);
            }

        } else {
            TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);
            String tbName = tableName;
            if (tableMeta.isGsi()) {
                tbName = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
            }
            if(GeneralUtil.isNotEmpty(tableGroupConfig.getTables())) {
                JoinGroupInfoRecord joinGroupInfoRecord = JoinGroupUtils.getJoinGroupInfoByTable(schemaName, tbName, metaDbConnection);
                String sourceJoinGroup = joinGroupInfoRecord == null ? "" : joinGroupInfoRecord.joinGroupName;
                boolean isValid = targetJoinGroup.equalsIgnoreCase(sourceJoinGroup);
                if (!isValid) {
                    String errMsg = String.format(
                        "The joinGroup of table:[%s] is not match with the joinGroup of tableGroup[%s]",
                        tableName, targetTableGroup);
                    throw new TddlRuntimeException(ErrorCode.ERR_JOIN_GROUP_NOT_MATCH, errMsg);
                }
            }
        }
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {

    }
}
