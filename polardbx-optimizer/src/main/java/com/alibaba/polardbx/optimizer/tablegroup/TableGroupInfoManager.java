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

package com.alibaba.polardbx.optimizer.tablegroup;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupUtils;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.PartitionStrategy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manage the tablegroup and partition group
 *
 * @author luoyanxin
 */
public class TableGroupInfoManager extends AbstractLifecycle {

    /**
     * Each schema has only one broadcast table group
     */
    public static final String BROADCAT_TG_NAME = "tg_broadcast";

    /**
     * the schema of tableGroup infos
     */
    final protected String schemaName;

    /**
     * key: tableGroup id
     * value: TableGroupConfig
     */
    final protected Map<Long, TableGroupConfig> tableGroupConfigInfoCache;

    /**
     * The tgId For broadcast table group
     */
    protected Long broadcastTgId = null;

    public TableGroupInfoManager(String schemaName) {
        this.schemaName = schemaName;
        tableGroupConfigInfoCache = new ConcurrentHashMap<>();
    }

    @Override
    protected void doInit() {
        super.doInit();

        loadTableGroupInfo();
    }

    @Override
    protected void doDestroy() {
        synchronized (tableGroupConfigInfoCache) {
            tableGroupConfigInfoCache.clear();
        }
    }

    protected void loadTableGroupInfo() {

        List<TableGroupConfig> tableGroupConfigs = TableGroupUtils.getAllTableGroupInfoByDb(this.schemaName);

        synchronized (tableGroupConfigInfoCache) {
            tableGroupConfigInfoCache.clear();
            for (TableGroupConfig conf : tableGroupConfigs) {
                Long tgId = conf.getTableGroupRecord().id;
                if (conf.getTableGroupRecord().tg_type == TableGroupRecord.TG_TYPE_BROADCAST_TBL_TG) {
                    broadcastTgId = tgId;
                }
                this.tableGroupConfigInfoCache.put(tgId, conf);
            }
        }
    }

    public void reloadTableGroupByGroupId(Long Id) {
        TableGroupConfig tableGroupConfig = TableGroupUtils.getTableGroupInfoByGroupId(Id);
        synchronized (tableGroupConfigInfoCache) {
            if (tableGroupConfig != null) {
                if (tableGroupConfig.getTableGroupRecord().tg_type == TableGroupRecord.TG_TYPE_BROADCAST_TBL_TG) {
                    broadcastTgId = tableGroupConfig.getTableGroupRecord().id;
                }
                this.tableGroupConfigInfoCache.put(Id, tableGroupConfig);
            } else {
                this.tableGroupConfigInfoCache.remove(Id);
            }
        }
    }

    public void reloadTableGroupByGroupIdAndTableName(Long id, String dbName, String tableName) {
        TableGroupConfig tableGroupConfig = tableGroupConfigInfoCache.get(id);
        if (tableGroupConfig != null) {
            TablePartRecordInfoContext tablePartRecordInfoContext = null;
            if (GeneralUtil.isNotEmpty(tableGroupConfig.getAllTables())) {
                tablePartRecordInfoContext = tableGroupConfig.getAllTables().stream()
                    .filter(o -> tableName.equalsIgnoreCase(o.getLogTbRec().getTableName())).findFirst().orElse(null);
                if (tablePartRecordInfoContext != null) {
                    //do nothing
                    return;
                } else {
                    tablePartRecordInfoContext =
                        TableGroupUtils.getTablePartRecordInfoContextsByDbNameAndTableName(dbName, tableName);
                }
            } else {
                tableGroupConfig.setTables(new ArrayList<>());
            }

            if (tablePartRecordInfoContext != null) {
                synchronized (tableGroupConfig.getAllTables()) {
                    tableGroupConfig.getAllTables().add(tablePartRecordInfoContext);
                }
            }
        } else {
            reloadTableGroupByGroupId(id);
        }
    }

    public TableGroupConfig reloadTableGroupByGroupName(String schemaName, String tableGroupName) {
        TableGroupConfig tableGroupConfig = TableGroupUtils.getTableGroupInfoByGroupName(schemaName, tableGroupName);
        synchronized (tableGroupConfigInfoCache) {
            if (tableGroupConfig != null) {
                if (tableGroupConfig.getTableGroupRecord().tg_type == TableGroupRecord.TG_TYPE_BROADCAST_TBL_TG) {
                    broadcastTgId = tableGroupConfig.getTableGroupRecord().id;
                }
                this.tableGroupConfigInfoCache.put(tableGroupConfig.getTableGroupRecord().id, tableGroupConfig);
            } else {
                Map.Entry<Long, TableGroupConfig> entry = tableGroupConfigInfoCache.entrySet().stream()
                    .filter(o -> o.getValue().getTableGroupRecord().tg_name.equalsIgnoreCase(tableGroupName))
                    .findFirst()
                    .orElse(null);
                if (entry != null) {
                    tableGroupConfigInfoCache.remove(entry.getKey());
                }
            }
        }
        return tableGroupConfig;
    }

    public TableGroupConfig getBroadcastTableGroupConfig() {
        if (broadcastTgId == null) {
            return null;
        }
        return tableGroupConfigInfoCache.get(broadcastTgId);
    }

    public TableGroupConfig getTableGroupConfigById(Long tableGroupId) {
        return tableGroupConfigInfoCache.get(tableGroupId);
    }

    public TableGroupConfig getTableGroupConfigByName(String tableGroupName) {
        Map.Entry<Long, TableGroupConfig> entry = tableGroupConfigInfoCache.entrySet().stream()
            .filter(o -> o.getValue().getTableGroupRecord().tg_name.equalsIgnoreCase(tableGroupName)).findFirst()
            .orElse(null);
        if (entry == null) {
            return null;
        } else {
            return entry.getValue();
        }
    }

    public PartitionInfo getTableGroupFirstTablePartitionInfo(
        String dbName,
        String tableGroupName,
        ExecutionContext executionContext) {

        TableGroupConfig tableGroupConfig = this.getTableGroupConfigByName(tableGroupName);
        if (tableGroupConfig == null) {
            return null;
        }

        String tableInCurrentGroup = tableGroupConfig.getTables().get(0).getLogTbRec().tableName;
        SchemaManager schemaManager = null;
        if (executionContext != null) {
            schemaManager = executionContext.getSchemaManager(dbName);
        } else {
            schemaManager = OptimizerContext.getContext(dbName).getLatestSchemaManager();
        }

        TableMeta tableMeta = schemaManager.getTable(tableInCurrentGroup);
        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
        return partitionInfo;
    }

    public PartitionGroupRecord getPartitionGroupByPartName(String dbName,
                                                            String tableGroupName,
                                                            String targetPartGroupName,
                                                            ExecutionContext executionContext) {
        TableGroupConfig tableGroupConfig = this.getTableGroupConfigByName(tableGroupName);
        if (tableGroupConfig == null) {
            return null;
        }
        PartitionInfo firstTblPartInfo = getTableGroupFirstTablePartitionInfo(dbName, tableGroupName, executionContext);
        PartitionSpec targetPartSpce = firstTblPartInfo.getPartitionBy().getPartitionByPartName(targetPartGroupName);
        Long pgId = targetPartSpce.getLocation().getPartitionGroupId();
        return tableGroupConfig.getPartitionGroup(pgId);
    }

    @Deprecated
    public PartitionGroupRecord getNextNeighborPartitionGroupsByPartNames(String dbName,
                                                                          String tableGroupName,
                                                                          String dropPartGroupName,
                                                                          ExecutionContext executionContext) {
        return null;
    }

    public Map<Long, TableGroupConfig> getTableGroupConfigInfoCache() {
        return tableGroupConfigInfoCache;
    }

    public Map<Long, TableGroupConfig> copyTableGroupConfigInfoFromCache(Integer tgType) {

        Map<Long, TableGroupConfig> copyTableGroupInfo = new HashMap<>();
        synchronized (this.tableGroupConfigInfoCache) {
            if (tgType == null) {
                copyTableGroupInfo.putAll(this.tableGroupConfigInfoCache);
            } else {
                this.tableGroupConfigInfoCache.entrySet().stream()
                    .filter(tg -> tg.getValue().getTableGroupRecord().tg_type == tgType)
                    .forEach(newTg -> copyTableGroupInfo.put(newTg.getKey(), newTg.getValue()));
            }
        }
        return copyTableGroupInfo;
    }

    public void putMockEntry(PartitionInfo partitionInfo) {
        if ((ConfigDataMode.isFastMock() || ConfigDataMode.isMock())) {
            PartitionInfoManager partitionInfoManager =
                OptimizerContext.getContext(schemaName).getPartitionInfoManager();
            Long maxExistGroupId = 0L;

            TablePartitionRecord logTableRec = PartitionInfoUtil.prepareRecordForLogicalTable(partitionInfo);
            List<TablePartitionRecord> partRecList = PartitionInfoUtil.prepareRecordForAllPartitions(partitionInfo);
            Map<String, List<TablePartitionRecord>> subPartRecInfos = PartitionInfoUtil
                .prepareRecordForAllSubpartitions(partRecList, partitionInfo,
                    partitionInfo.getPartitionBy().getPartitions());

            TableGroupRecord tableGroupRecord = null;
            List<PartitionGroupRecord> partitionGroupRecords = null;

            boolean found = false;
            for (Map.Entry<Long, TableGroupConfig> entry : tableGroupConfigInfoCache.entrySet()) {
                TablePartRecordInfoContext tablePartRecordInfoContext =
                    entry.getValue().getTables().get(0);
                PartitionInfo part = partitionInfoManager
                    .getPartitionInfo(tablePartRecordInfoContext.getLogTbRec().tableName);
                if (entry.getKey() > maxExistGroupId) {
                    maxExistGroupId = entry.getKey();
                }
                if (partitionInfo.equals(part)) {
                    partitionInfo.setTableGroupId(entry.getKey());
                    logTableRec.groupId = entry.getKey();
                    partitionGroupRecords = entry.getValue().getPartitionGroupRecords();
                    found = true;
                    break;
                }
            }
            // need to create a new table group and related partition groups
            if (!found) {
                Long maxPartGroupId = 0L;
                if (tableGroupConfigInfoCache.get(maxExistGroupId) != null) {
                    for (PartitionGroupRecord partitionGroupRecord : tableGroupConfigInfoCache.get(maxExistGroupId)
                        .getPartitionGroupRecords()) {
                        if (partitionGroupRecord.id > maxPartGroupId) {
                            maxPartGroupId = partitionGroupRecord.id;
                        }
                    }
                }
                maxExistGroupId = maxExistGroupId + 1;
                tableGroupRecord = PartitionInfoUtil.prepareRecordForTableGroup(partitionInfo);
                tableGroupRecord.id = maxExistGroupId;
                partitionInfo.setTableGroupId(maxExistGroupId);
                partitionGroupRecords =
                    PartitionInfoUtil
                        .prepareRecordForPartitionGroups(partitionInfo.getPartitionBy().getPartitions());
                for (PartitionGroupRecord partitionGroupRecord : partitionGroupRecords) {
                    partitionGroupRecord.id = maxPartGroupId + 1;
                    maxPartGroupId = maxPartGroupId + 1;
                    TablePartitionRecord tablePartitionRecord = partRecList.stream()
                        .filter(o -> o.partName.equalsIgnoreCase(partitionGroupRecord.partition_name)).findFirst()
                        .orElse(null);
                    assert tablePartitionRecord != null;
                    tablePartitionRecord.groupId = partitionGroupRecord.id;
                }
                TablePartRecordInfoContext newTablePartRecordInfoContext = new TablePartRecordInfoContext();
                newTablePartRecordInfoContext.setLogTbRec(logTableRec);
                newTablePartRecordInfoContext.setPartitionRecList(partRecList);
                newTablePartRecordInfoContext.setSubPartitionRecMap(subPartRecInfos);
                List<TablePartRecordInfoContext> newTablePartRecordsInfoContext = new ArrayList<>();
                newTablePartRecordsInfoContext.add(newTablePartRecordInfoContext);

                TableGroupConfig newTableGroupConfig =
                    new TableGroupConfig(
                        tableGroupRecord,
                        partitionGroupRecords,
                        newTablePartRecordsInfoContext);
                tableGroupConfigInfoCache.put(tableGroupRecord.id, newTableGroupConfig);
            } else {
                TablePartRecordInfoContext existsTablePart =
                    tableGroupConfigInfoCache.get(logTableRec.groupId).getTables().stream()
                        .filter(o -> o.getLogTbRec().tableName.equalsIgnoreCase(partitionInfo.getTableName()))
                        .findFirst().orElse(null);
                if (existsTablePart != null) {
                    return;
                }
                for (PartitionGroupRecord partitionGroupRecord : partitionGroupRecords) {
                    TablePartitionRecord tablePartitionRecord = partRecList.stream()
                        .filter(o -> o.partName.equalsIgnoreCase(partitionGroupRecord.partition_name)).findFirst()
                        .orElse(null);
                    assert tablePartitionRecord != null;
                    tablePartitionRecord.groupId = partitionGroupRecord.id;
                }
                TablePartRecordInfoContext newTablePartRecordInfoContext = new TablePartRecordInfoContext();
                newTablePartRecordInfoContext.setLogTbRec(logTableRec);
                newTablePartRecordInfoContext.setPartitionRecList(partRecList);
                newTablePartRecordInfoContext.setSubPartitionRecMap(subPartRecInfos);
                tableGroupConfigInfoCache.get(logTableRec.groupId).getTables()
                    .add(newTablePartRecordInfoContext);
            }

        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "it's not allow to set the partitionInfo here for non-mock mode");
        }
    }

    public static void reload(String schemaName, Long tableGroupId) {
        OptimizerContext.getContext(schemaName).getTableGroupInfoManager().reloadTableGroupByGroupId(tableGroupId);
    }

    public String getSchemaName() {
        return schemaName;
    }

}
