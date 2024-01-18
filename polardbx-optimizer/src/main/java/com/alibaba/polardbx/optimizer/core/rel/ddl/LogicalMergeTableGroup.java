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
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupLocation;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.MergeTableGroupPreparedData;
import com.alibaba.polardbx.optimizer.archive.CheckOSSArchiveUtil;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.MergeTableGroup;
import org.apache.calcite.sql.SqlMergeTableGroup;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class LogicalMergeTableGroup extends BaseDdlOperation {
    private MergeTableGroupPreparedData preparedData;

    public LogicalMergeTableGroup(DDL ddl) {
        super(ddl);
    }

    @Override
    public boolean isSupportedByFileStorage() {
        return false;
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        MergeTableGroup mergeTableGroup = (MergeTableGroup) relDdl;
        SqlMergeTableGroup sqlMergeTableGroup = (SqlMergeTableGroup) mergeTableGroup.sqlNode;

        for (String tableGroup : sqlMergeTableGroup.getSourceTableGroups()) {
            if (!CheckOSSArchiveUtil.checkTableGroupWithoutOSS(schemaName, tableGroup)) {
                throw new TddlRuntimeException(ErrorCode.ERR_UNARCHIVE_FIRST, "unarchive tablegroup " + tableGroup);
            }
        }
        return true;
    }

    public void preparedData(ExecutionContext executionContext) {
        String schemaName = getSchemaName();
        MergeTableGroup mergeTableGroup = (MergeTableGroup) relDdl;
        SqlMergeTableGroup sqlMergeTableGroup = (SqlMergeTableGroup) mergeTableGroup.sqlNode;
        String targetTableGroup = sqlMergeTableGroup.getTargetTableGroup();
        Set<String> sourceTableGroups = new TreeSet<>(String::compareToIgnoreCase);
        sourceTableGroups.addAll(sqlMergeTableGroup.getSourceTableGroups());
        Map<String, Map<String, Long>> groupTablesVersion = new HashMap<>();
        Map<String, TableGroupConfig> tableGroupConfigMap = new TreeMap<>(String::compareToIgnoreCase);

        SchemaManager schemaManager = executionContext.getSchemaManager(schemaName);
        TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
        TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(targetTableGroup);

        if (tableGroupConfig == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_TABLE_GROUP_NOT_EXISTS, targetTableGroup);
        }

        tableGroupConfigMap.put(targetTableGroup, tableGroupConfig);

        Set<String> physicalGroups = new TreeSet<>(String::compareToIgnoreCase);

        if (GeneralUtil.isEmpty(tableGroupConfig.getAllTables())) {
            throw new TddlRuntimeException(ErrorCode.ERR_TABLE_GROUP_IS_EMPTY,
                "it not allow to merge tables into empty tablegroup");
        } else {
            TablePartRecordInfoContext tableInfo = tableGroupConfig.getAllTables().get(0);
            String primaryTableName = tableInfo.getTableName();
            TableMeta tableMeta = schemaManager.getTable(primaryTableName);
            if (tableMeta.isGsi()) {
                //all the gsi table version change will be behavior by primary table
                assert
                    tableMeta.getGsiTableMetaBean() != null && tableMeta.getGsiTableMetaBean().gsiMetaBean != null;
                primaryTableName = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
                tableMeta = schemaManager.getTable(primaryTableName);
            }
            //only need to add the first tables
            Map<String, Long> tableVersions = new TreeMap<>(String::compareToIgnoreCase);
            tableVersions.put(tableMeta.getTableName(), tableMeta.getVersion());
            groupTablesVersion.put(targetTableGroup, tableVersions);

            for (PartitionGroupRecord record : GeneralUtil.emptyIfNull(tableGroupConfig.getPartitionGroupRecords())) {
                physicalGroups.add(GroupInfoUtil.buildGroupNameFromPhysicalDb(record.getPhy_db()));
            }
        }
        for (String sourceGroup : sourceTableGroups) {
            tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(sourceGroup);
            tableGroupConfigMap.put(sourceGroup, tableGroupConfig);
            if (tableGroupConfig == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_TABLE_GROUP_NOT_EXISTS,
                    "tablegroup:[" + sourceGroup + "] is not exists");
            }
            Map<String, Long> tableVersions = new TreeMap<>(String::compareToIgnoreCase);
            for (TablePartRecordInfoContext tableInfo : GeneralUtil.emptyIfNull(tableGroupConfig.getAllTables())) {
                String primaryTableName = tableInfo.getTableName();
                TableMeta tableMeta = schemaManager.getTable(primaryTableName);
                if (tableMeta.isGsi()) {
                    //all the gsi table version change will be behavior by primary table
                    assert
                        tableMeta.getGsiTableMetaBean() != null && tableMeta.getGsiTableMetaBean().gsiMetaBean != null;
                    primaryTableName = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
                    tableMeta = schemaManager.getTable(primaryTableName);
                }
                tableVersions.put(tableMeta.getTableName(), tableMeta.getVersion());
            }
            groupTablesVersion.put(sourceGroup, tableVersions);
        }
        List<GroupDetailInfoExRecord> detailInfoExRecords = TableGroupLocation.getOrderedGroupList(schemaName);
        Map<String, String> dbInstMap = new TreeMap<>(String::compareToIgnoreCase);
        detailInfoExRecords.stream().forEach(o -> dbInstMap.put(o.phyDbName, o.storageInstId));

        preparedData =
            new MergeTableGroupPreparedData(schemaName, targetTableGroup, sourceTableGroups, tableGroupConfigMap,
                groupTablesVersion, physicalGroups, dbInstMap,
                mergeTableGroup.isForce());

    }

    public MergeTableGroupPreparedData getPreparedData() {
        return preparedData;
    }

    public static LogicalMergeTableGroup create(DDL ddl) {
        return new LogicalMergeTableGroup(ddl);
    }

    @Override
    public boolean checkIfFileStorage(ExecutionContext executionContext) {
        MergeTableGroup mergeTableGroup = (MergeTableGroup) relDdl;
        SqlMergeTableGroup sqlMergeTableGroup = (SqlMergeTableGroup) mergeTableGroup.sqlNode;

        for (String tableGroup : sqlMergeTableGroup.getSourceTableGroups()) {
            if (TableGroupNameUtil.isOssTg(tableGroup)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean checkIfBindFileStorage(ExecutionContext executionContext) {
        MergeTableGroup mergeTableGroup = (MergeTableGroup) relDdl;
        SqlMergeTableGroup sqlMergeTableGroup = (SqlMergeTableGroup) mergeTableGroup.sqlNode;

        for (String tableGroup : sqlMergeTableGroup.getSourceTableGroups()) {
            if (!CheckOSSArchiveUtil.checkTableGroupWithoutOSS(schemaName, tableGroup)) {
                return true;
            }
        }
        return false;
    }
}
