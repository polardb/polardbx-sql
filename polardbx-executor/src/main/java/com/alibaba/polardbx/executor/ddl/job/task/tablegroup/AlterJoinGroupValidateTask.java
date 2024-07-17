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
import com.alibaba.polardbx.gms.tablegroup.JoinGroupInfoAccessor;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupInfoRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

@Getter
@TaskName(name = "AlterJoinGroupValidateTask")
public class AlterJoinGroupValidateTask extends BaseValidateTask {

    private String joinGroupName;
    private Map<String, Long> tablesVersion;
    /**
     * key:tableGroupName
     * value:
     * key:tables be altered
     * value:all tables in current tableGroup
     */
    private Map<String, Pair<Set<String>, Set<String>>> tableGroupInfos = new TreeMap<>();

    @JSONCreator
    public AlterJoinGroupValidateTask(String schemaName, String joinGroupName, Map<String, Long> tablesVersion,
                                      Map<String, Pair<Set<String>, Set<String>>> tableGroupInfos) {
        super(schemaName);
        this.joinGroupName = joinGroupName;
        this.tablesVersion = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        this.tablesVersion.putAll(tablesVersion);
        for (Map.Entry<String, Pair<Set<String>, Set<String>>> entry : tableGroupInfos.entrySet()) {
            Pair<Set<String>, Set<String>> tables =
                new Pair<>(new TreeSet<>(String::compareToIgnoreCase),
                    new TreeSet<>(String::compareToIgnoreCase));
            tables.getKey().addAll(entry.getValue().getKey());
            tables.getValue().addAll(entry.getValue().getValue());
            this.tableGroupInfos.put(entry.getKey(), tables);
        }
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        JoinGroupInfoAccessor joinGroupInfoAccessor = new JoinGroupInfoAccessor();
        joinGroupInfoAccessor.setConnection(metaDbConnection);
        JoinGroupInfoRecord joinGroupInfoRecord =
            joinGroupInfoAccessor.getJoinGroupInfoByName(schemaName, joinGroupName, false);
        if (joinGroupInfoRecord == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_JOIN_GROUP_NOT_EXISTS,
                String.format(
                    "the metadata of joinGroup[%s] is not exists",
                    joinGroupName));
        }
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        JoinGroupValidator.validateJoinGroupInfo(schemaName, joinGroupName, executionContext.getParamManager());
        if (GeneralUtil.isNotEmpty(tablesVersion)) {
            for (Map.Entry<String, Long> tableVersionInfo : tablesVersion.entrySet()) {
                Long newTableVersion =
                    executionContext.getSchemaManager(schemaName).getTable(tableVersionInfo.getKey()).getVersion();
                if (newTableVersion.longValue() != tableVersionInfo.getValue().longValue()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_TABLEGROUP_META_TOO_OLD,
                        String.format(
                            "the metadata of tableGroup[%s].[%s] is too old, version:[%d<->%d], please retry this command",
                            joinGroupName, tableVersionInfo.getKey(), tableVersionInfo.getValue(), newTableVersion));
                }
            }
        }
        for (Map.Entry<String, Pair<Set<String>, Set<String>>> entry : tableGroupInfos.entrySet()) {
            String tableGroupName = entry.getKey();
            Set<String> originTables = entry.getValue().getValue();
            TableGroupConfig tableGroupConfig =
                OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
                    .getTableGroupConfigByName(tableGroupName);
            Set<String> curTables = new TreeSet<>(String::compareToIgnoreCase);
            curTables.addAll(
                tableGroupConfig.getAllTables().stream().collect(Collectors.toSet()));
            if (!originTables.equals(curTables)) {
                throw new TddlRuntimeException(ErrorCode.ERR_TABLEGROUP_META_TOO_OLD,
                    String.format(
                        "the metadata of tableGroup[%s] is too old, tables change from[%s] to [%s], please retry this command",
                        tableGroupName, String.join(",", originTables), String.join(",", curTables)));

            }
        }

    }

    @Override
    protected String remark() {
        return "|tableGroupName: " + joinGroupName;
    }
}
