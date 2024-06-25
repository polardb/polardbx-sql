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
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.partition.TablePartitionConfigUtil;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupUtils;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.GroupStorageInfoManager;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manage the tablegroup and partition group
 *
 * @author luoyanxin
 */
public class TableGroupInfoManager extends AbstractLifecycle {

    /**
     * the schema of tableGroup infos
     */
    final protected String schemaName;

    /**
     * key: tableGroup id
     * value: TableGroupConfig
     */
    final protected Map<Long, TableGroupConfig> tableGroupConfigInfoCache;

    final private GroupStorageInfoManager groupStorageInfoManager;

    /**
     * The tgId For broadcast table group
     */
    protected Long broadcastTgId = null;

    public TableGroupInfoManager(String schemaName) {
        this.schemaName = schemaName;
        tableGroupConfigInfoCache = new ConcurrentHashMap<>();
        groupStorageInfoManager = new GroupStorageInfoManager(schemaName);
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
            groupStorageInfoManager.clear();
        }
    }

    protected void loadTableGroupInfo() {

        List<TableGroupConfig> tableGroupConfigs = TableGroupUtils.getAllTableGroupInfoByDb(this.schemaName);

        synchronized (tableGroupConfigInfoCache) {
            tableGroupConfigInfoCache.clear();
            groupStorageInfoManager.clear();
            for (TableGroupConfig conf : tableGroupConfigs) {
                Long tgId = conf.getTableGroupRecord().id;
                if (conf.getTableGroupRecord().tg_type == TableGroupRecord.TG_TYPE_BROADCAST_TBL_TG) {
                    broadcastTgId = tgId;
                }
                this.tableGroupConfigInfoCache.put(tgId, conf);
            }

            groupStorageInfoManager.registerTableGroupId(tableGroupConfigInfoCache.values());
        }
    }

    public void reloadTableGroupByGroupId(Long id) {
        reloadTableGroupByGroupId(null, id);
    }

    public void reloadTableGroupByGroupId(Connection conn, Long Id) {
        TableGroupConfig tableGroupConfig = TableGroupUtils.getTableGroupInfoByGroupId(conn, Id);
        synchronized (tableGroupConfigInfoCache) {
            if (tableGroupConfig != null) {
                if (tableGroupConfig.getTableGroupRecord().tg_type == TableGroupRecord.TG_TYPE_BROADCAST_TBL_TG) {
                    broadcastTgId = tableGroupConfig.getTableGroupRecord().id;
                }
                this.tableGroupConfigInfoCache.put(Id, tableGroupConfig);
                this.groupStorageInfoManager.registerTableGroupId(ImmutableList.of(tableGroupConfig));
            } else {
                this.tableGroupConfigInfoCache.remove(Id);
                this.groupStorageInfoManager.unregisterTableGroupId(Id);
            }
        }
    }

    public void reloadTableGroupByGroupIdAndTableName(Long id, String dbName, String tableName) {
        reloadTableGroupByGroupIdAndTableName(null, id, dbName, tableName);
    }

    public void reloadTableGroupByGroupIdAndTableName(Connection conn, Long id, String dbName, String tableName) {
        TableGroupConfig tableGroupConfig = tableGroupConfigInfoCache.get(id);
        if (tableGroupConfig != null) {
            String logicTableName = null;
            if (GeneralUtil.isNotEmpty(tableGroupConfig.getAllTables())) {
                logicTableName = tableGroupConfig.getAllTables().stream()
                    .filter(o -> tableName.equalsIgnoreCase(o)).findFirst().orElse(null);
                if (logicTableName != null) {
                    //do nothing
                    return;
                } else {
                    TablePartRecordInfoContext tablePartRecordInfoContext =
                        TableGroupUtils.getTablePartRecordInfoContextsByDbNameAndTableName(conn, dbName, tableName);
                    if (tablePartRecordInfoContext != null && tablePartRecordInfoContext.getLogTbRec().partStatus
                        == TablePartitionRecord.PARTITION_STATUS_LOGICAL_TABLE_PUBLIC) {
                        synchronized (tableGroupConfig.getAllTables()) {
                            tableGroupConfig.getAllTables().add(tablePartRecordInfoContext.getTableName());
                        }
                    }
                }
            } else {
                reloadTableGroupByGroupId(conn, id);
            }
        } else {
            reloadTableGroupByGroupId(conn, id);
        }
    }

    /**
     * invalidate tables in table group by id
     */
    public void invalidate(Long id, String tableName) {
        TableGroupConfig tableGroupConfig = tableGroupConfigInfoCache.get(id);
        if (tableGroupConfig != null) {
            synchronized (tableGroupConfig.getAllTables()) {
                tableGroupConfig.getAllTables().removeIf(
                    o -> tableName.equalsIgnoreCase(o));
            }
            TableGroupRecord tableGroupRecord = tableGroupConfig.getTableGroupRecord();
            if (tableGroupConfig.getAllTables().isEmpty()) {
                if (tableGroupRecord.manual_create == 0) {
                    synchronized (tableGroupConfigInfoCache) {
                        tableGroupConfigInfoCache.remove(id);
                        groupStorageInfoManager.unregisterTableGroupId(id);
                    }
                } else {
                    synchronized (tableGroupConfig.getTableGroupRecord()) {
                        tableGroupConfig.setPartitionGroupRecords(new ArrayList<>());
                    }
                }
            }
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
                groupStorageInfoManager.registerTableGroupId(ImmutableList.of(tableGroupConfig));
            } else {
                Map.Entry<Long, TableGroupConfig> entry = tableGroupConfigInfoCache.entrySet().stream()
                    .filter(o -> o.getValue().getTableGroupRecord().tg_name.equalsIgnoreCase(tableGroupName))
                    .findFirst()
                    .orElse(null);
                if (entry != null) {
                    tableGroupConfigInfoCache.remove(entry.getKey());
                    groupStorageInfoManager.unregisterTableGroupId(entry.getKey());
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

    public Map<Long, String> getPartitionDNs(Long tableGroupId) {
        return groupStorageInfoManager.getPartitionDNs(tableGroupId);
    }

    public String getPartitionDN(Long tableGroupId, Long parId) {
        return groupStorageInfoManager.getPartitionDN(tableGroupId, parId);
    }

    public TableGroupConfig getTableGroupConfigByName(String tableGroupName) {
        if (StringUtils.isEmpty(tableGroupName)) {
            return null;
        }
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

        String tableInCurrentGroup = tableGroupConfig.getTables().get(0);
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
            List<TablePartitionRecord> allPhyPartRecInfos = new ArrayList<>();
            if (partitionInfo.getPartitionBy().getSubPartitionBy() != null) {
                for (int i = 0; i < partRecList.size(); i++) {
                    allPhyPartRecInfos.addAll(subPartRecInfos.get(partRecList.get(i).getPartName()));
                }
            } else {
                allPhyPartRecInfos = partRecList;
            }

            TableGroupRecord tableGroupRecord = null;
            List<PartitionGroupRecord> partitionGroupRecords = null;

            boolean found = false;
            for (Map.Entry<Long, TableGroupConfig> entry : tableGroupConfigInfoCache.entrySet()) {
                String tableName =
                    entry.getValue().getTables().get(0);
                PartitionInfo part = partitionInfoManager
                    .getPartitionInfo(tableName);
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
                        .prepareRecordForPartitionGroups(partitionInfo.getPartitionBy().getPhysicalPartitions());
                for (PartitionGroupRecord partitionGroupRecord : partitionGroupRecords) {
                    partitionGroupRecord.id = maxPartGroupId + 1;
                    maxPartGroupId = maxPartGroupId + 1;
                    TablePartitionRecord tablePartitionRecord = allPhyPartRecInfos.stream()
                        .filter(o -> o.partName.equalsIgnoreCase(partitionGroupRecord.partition_name)).findFirst()
                        .orElse(null);
                    assert tablePartitionRecord != null;
                    tablePartitionRecord.groupId = partitionGroupRecord.id;
                }
                List<String> tables = new ArrayList<>();
                tables.add(logTableRec.getTableName());
                TableGroupConfig newTableGroupConfig =
                    new TableGroupConfig(
                        tableGroupRecord,
                        partitionGroupRecords,
                        tables,
                        tableGroupRecord.getLocality());
                tableGroupConfigInfoCache.put(tableGroupRecord.id, newTableGroupConfig);
            } else {
                String existsTablePart =
                    tableGroupConfigInfoCache.get(logTableRec.groupId).getTables().stream()
                        .filter(o -> o.equalsIgnoreCase(partitionInfo.getTableName()))
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
                tableGroupConfigInfoCache.get(logTableRec.groupId).getTables().add(logTableRec.getTableName());
            }

        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "it's not allow to set the partitionInfo here for non-mock mode");
        }
    }

    public static void reload(String schemaName, Long tableGroupId) {
        OptimizerContext.getContext(schemaName).getTableGroupInfoManager().reloadTableGroupByGroupId(tableGroupId);
    }

    private static Long getTableGroupIdFromMetaDb(String schemaName, String logicalTableName, String gsiName,
                                                  boolean fromDelta) {
        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        if (!isNewPartDb) {
            return null;
        }
        if (StringUtils.isEmpty(gsiName)) {
            TablePartitionRecord tpr =
                TablePartitionConfigUtil.getTablePartitionByDbTb(schemaName, logicalTableName, fromDelta);
            if (tpr == null) {
                return null;
            }
            return tpr.getGroupId();
        } else {
            Map<String, String> gsiNames = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            GsiMetaManager.GsiMetaBean gsiMetaBean = OptimizerContext.getContext(schemaName).getLatestSchemaManager()
                .getGsi(logicalTableName, IndexStatus.ALL);
            GsiMetaManager.GsiTableMetaBean gsiTableMetaBean = gsiMetaBean.getTableMeta().get(logicalTableName);
            if (gsiTableMetaBean == null) {
                return null;
            }
            gsiTableMetaBean.indexMap.forEach((key, value) -> {
                gsiNames.put(TddlSqlToRelConverter.unwrapGsiName(key), key);
            });
            String fullGsiName = gsiNames.get(gsiName);
            if (StringUtils.isEmpty(fullGsiName)) {
                return null;
            }
            TablePartitionRecord tpr =
                TablePartitionConfigUtil.getTablePartitionByDbTb(schemaName, fullGsiName, fromDelta);
            if (tpr == null) {
                return null;
            }
            return tpr.getGroupId();
        }
    }

    private static Long getTableGroupIdFromMem(String schemaName, String logicalTableName, String gsiName) {
        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        if (!isNewPartDb) {
            return null;
        }
        SchemaManager sm = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        TableGroupInfoManager tgInfoManager = OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
        TableMeta tableMeta = sm.getTable(logicalTableName);
        if (StringUtils.isEmpty(gsiName)) {
            long tableGroupId = tableMeta.getPartitionInfo().getTableGroupId();
            TableGroupConfig tableGroupConfig = tgInfoManager.getTableGroupConfigById(tableGroupId);
            return tableGroupConfig.getTableGroupRecord().id;
        } else {
            Map<String, String> gsiNames = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            tableMeta.getGsiTableMetaBean().indexMap.forEach((key, value) -> {
                gsiNames.put(TddlSqlToRelConverter.unwrapGsiName(key), key);
            });
            String fullGsiName = gsiNames.get(gsiName);
            if (StringUtils.isEmpty(fullGsiName)) {
                return null;
            }
            tableMeta = sm.getTable(fullGsiName);
            long tableGroupId = tableMeta.getPartitionInfo().getTableGroupId();
            TableGroupConfig tableGroupConfig = tgInfoManager.getTableGroupConfigById(tableGroupId);
            return tableGroupConfig.getTableGroupRecord().id;
        }
    }

    public static Long getTableGroupId(String schemaName, String logicalTableName, String gsiName,
                                       boolean fromMetadb, boolean fromDelta) {
        if (fromMetadb) {
            return getTableGroupIdFromMetaDb(schemaName, logicalTableName, gsiName, fromDelta);
        } else {
            return getTableGroupIdFromMem(schemaName, logicalTableName, gsiName);
        }
    }

    public static Long getTableGroupId(String schemaName, String logicalTableName, String gsiName) {
        return getTableGroupIdFromMem(schemaName, logicalTableName, gsiName);
    }

    public String getSchemaName() {
        return schemaName;
    }

}
