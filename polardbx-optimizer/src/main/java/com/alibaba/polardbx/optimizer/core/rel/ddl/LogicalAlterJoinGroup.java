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

package com.alibaba.polardbx.optimizer.core.rel.ddl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupTableDetailRecord;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupUtils;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterJoinGroupPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterJoinGroup;
import org.apache.calcite.sql.SqlAlterJoinGroup;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class LogicalAlterJoinGroup extends BaseDdlOperation {
    private AlterJoinGroupPreparedData preparedData;

    public LogicalAlterJoinGroup(DDL ddl) {
        super(ddl);
    }

    @Override
    public boolean isSupportedByFileStorage() {
        return true;
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        return true;
    }

    public void preparedData(ExecutionContext executionContext) {
        String schemaName = getSchemaName();
        AlterJoinGroup alterJoinGroup = (AlterJoinGroup) relDdl;
        SqlAlterJoinGroup sqlAlterJoinGroup = (SqlAlterJoinGroup) alterJoinGroup.sqlNode;
        Map<String, Long> tablesToChange = new TreeMap<>(String::compareToIgnoreCase);
        Map<String, Long> gsiTablesToChange = new TreeMap<>(String::compareToIgnoreCase);
        SchemaManager schemaManager = executionContext.getSchemaManager(schemaName);
        TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
        for (String table : sqlAlterJoinGroup.getTableNames()) {
            TableMeta tableMeta = schemaManager.getTable(table);
            PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
            if (tableMeta.isGsi()) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "can not change the GSI's joinGroup directly");
            }
            if (partitionInfo.isBroadcastTable() || partitionInfo.isSingleTable()) {
                //ignore broadcast table and single table
                continue;
            }
            tablesToChange.put(table, tableMeta.getVersion());
        }
        Set<String> tablesInCurJoinGroup = new TreeSet<>(String::compareToIgnoreCase);
        try (Connection connection = MetaDbUtil.getConnection()) {
            List<JoinGroupTableDetailRecord> records =
                JoinGroupUtils.getJoinGroupDetailByName(schemaName, sqlAlterJoinGroup.getJoinGroupName(),
                    connection);
            for (JoinGroupTableDetailRecord record : GeneralUtil.emptyIfNull(records)) {
                if (tablesToChange.containsKey(record.tableName)) {
                    tablesInCurJoinGroup.add(record.tableName);
                }
            }

        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
        Map<String, Long> tablesVersion = new TreeMap<>(String::compareToIgnoreCase);
        if (sqlAlterJoinGroup.isAdd()) {
            for (String tableName : tablesInCurJoinGroup) {
                tablesToChange.remove(tableName);
            }
            tablesVersion.putAll(tablesToChange);
        } else {
            for (String tableName : tablesInCurJoinGroup) {
                tablesVersion.put(tableName, tablesToChange.get(tableName));
            }
        }
        Map<String, Pair<Set<String>, Set<String>>> tableGroupMap = new TreeMap<>(String::compareToIgnoreCase);
        for (Map.Entry<String, Long> entry : tablesVersion.entrySet()) {
            TableMeta tableMeta = schemaManager.getTable(entry.getKey());
            PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
            TableGroupConfig tableGroupConfig =
                tableGroupInfoManager.getTableGroupConfigById(partitionInfo.getTableGroupId());
            String tableGroupName = tableGroupConfig.getTableGroupRecord().getTg_name();
            if (!tableGroupMap.containsKey(tableGroupName)) {
                Pair<Set<String>, Set<String>> tables =
                    new Pair<>(new TreeSet<>(String::compareToIgnoreCase), new TreeSet<>(String::compareToIgnoreCase));
                tables.getKey().add(tableMeta.getTableName());
                tables.getValue().addAll(tableGroupConfig.getTables().stream().map(o -> o.getTableName()).collect(
                    Collectors.toList()));
                tableGroupMap.put(tableGroupName, tables);
            } else {
                tableGroupMap.get(tableGroupName).getKey().add(tableMeta.getTableName());
            }
            if (tableMeta.withGsi()) {
                List<TableMeta> indexTableMetas =
                    GlobalIndexMeta.getIndex(tableMeta.getTableName(), tableMeta.getSchemaName(), executionContext);
                indexTableMetas.forEach(o -> {
                    TableGroupConfig gsiTableGroupConfig =
                        tableGroupInfoManager.getTableGroupConfigById(o.getPartitionInfo().getTableGroupId());
                    gsiTablesToChange.put(o.getTableName(), o.getVersion());
                    String gsiTableGroupName = gsiTableGroupConfig.getTableGroupRecord().tg_name;
                    if (!tableGroupMap.containsKey(gsiTableGroupName)) {
                        Pair<Set<String>, Set<String>> tables =
                            new Pair<>(new TreeSet<>(String::compareToIgnoreCase),
                                new TreeSet<>(String::compareToIgnoreCase));
                        tables.getKey().add(o.getTableName());
                        tables.getValue()
                            .addAll(gsiTableGroupConfig.getTables().stream().map(o1 -> o1.getTableName()).collect(
                                Collectors.toList()));
                    } else {
                        tableGroupMap.get(gsiTableGroupName).getKey().add(o.getTableName());
                    }
                });
            }
        }
        preparedData =
            new AlterJoinGroupPreparedData(schemaName, sqlAlterJoinGroup.getJoinGroupName(),
                sqlAlterJoinGroup.isAdd(), tablesVersion, tableGroupMap);
    }

    public AlterJoinGroupPreparedData getPreparedData() {
        return preparedData;
    }

    public static LogicalAlterJoinGroup create(DDL ddl) {
        return new LogicalAlterJoinGroup(ddl);
    }
}
