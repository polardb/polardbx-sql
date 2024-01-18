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

package com.alibaba.polardbx.executor.balancer.policy;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.locality.LocalityDetailInfoRecord;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.metadb.table.TablesRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupUtils;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoAccessor;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoRecord;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.locality.LocalityManager;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author moyi
 * @since 2021/08
 */
public class PolicyUtils {

    public static Map<String, GroupDetailInfoRecord> getGroupDetails(String schema) {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            GroupDetailInfoAccessor accessor = new GroupDetailInfoAccessor();
            accessor.setConnection(conn);
            List<GroupDetailInfoRecord> records =
                accessor.getGroupDetailInfoByInstIdAndDbName(InstIdUtil.getInstId(), schema);
            return records.stream().collect(Collectors.toMap(GroupDetailInfoRecord::getGroupName, x -> x));
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public static Map<String, GroupDetailInfoRecord> getGroupDetails(String schema, List<String> storageInsts) {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            GroupDetailInfoAccessor accessor = new GroupDetailInfoAccessor();
            accessor.setConnection(conn);
            List<GroupDetailInfoRecord> records =
                accessor.getGroupDetailInfoByInstIdAndDbName(InstIdUtil.getInstId(), schema)
                    .stream().filter(o -> storageInsts.contains(o.storageInstId)).collect(Collectors.toList());
            return records.stream().collect(Collectors.toMap(GroupDetailInfoRecord::getGroupName, x -> x));
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public static List<LocalityDetailInfoRecord> getLocalityDetails(String schema) {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            List<TableGroupConfig> tableGroupConfigList = TableGroupUtils.getAllTableGroupInfoByDb(schema);
            PartitionInfoManager partitionInfoManager = OptimizerContext.getContext(schema).getPartitionInfoManager();
            TableInfoManager tableInfoManager = new TableInfoManager();
            DbInfoManager dbInfoManager = DbInfoManager.getInstance();
            tableInfoManager.setConnection(conn);
            List<TablesRecord> tableInfoList = tableInfoManager.queryTables(schema);

            List<LocalityDetailInfoRecord> localityDetailInfoRecords = new ArrayList<>();

            int rowNum = 0;

            Long objectId;
            String objectName, locality;
            objectId = dbInfoManager.getDbInfo(schema).id;
            objectName = schema;
            locality = LocalityManager.getInstance().getLocalityOfDb(objectId).getLocality();
            localityDetailInfoRecords.add(new LocalityDetailInfoRecord(rowNum++,
                LocalityDetailInfoRecord.LOCALITY_TYPE_DATABASE,
                objectId,
                objectName,
                locality));

            for (TablesRecord tableInfo : tableInfoList) {
                objectId = tableInfo.id;
                objectName = tableInfo.tableName;
                locality =
                    partitionInfoManager.getPartitionInfo(objectName).getLocality();
                localityDetailInfoRecords.add(new LocalityDetailInfoRecord(rowNum++,
                    LocalityDetailInfoRecord.LOCALITY_TYPE_TABLE,
                    objectId,
                    objectName,
                    locality));
            }

            for (TableGroupConfig tableGroupConfig : tableGroupConfigList) {
                objectId = tableGroupConfig.getTableGroupRecord().getId();
                objectName = tableGroupConfig.getTableGroupRecord().getTg_name();
                locality = tableGroupConfig.getLocalityDesc().toString();
                localityDetailInfoRecords.add(new LocalityDetailInfoRecord(rowNum++,
                    LocalityDetailInfoRecord.LOCALITY_TYPE_TABLEGROUP,
                    objectId,
                    objectName,
                    locality));
                List<PartitionGroupRecord> partitionGroupList = tableGroupConfig.getPartitionGroupRecords();
                for (PartitionGroupRecord partitionGroupRecord : partitionGroupList) {
                    locality = partitionGroupRecord.getLocality();
                    localityDetailInfoRecords.add(new LocalityDetailInfoRecord(rowNum++,
                        LocalityDetailInfoRecord.LOCALITY_TYPE_PARTITIONGROUP,
                        partitionGroupRecord.id,
                        partitionGroupRecord.partition_name,
                        locality));
                }

            }
            return localityDetailInfoRecords;
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public static List<LocalityDetailInfoRecord> getLocalityDetails(String schema, String tableGroup,
                                                                    String partitionGroup) {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            TableGroupConfig tableGroupConfig = TableGroupUtils.getTableGroupInfoByGroupName(schema, tableGroup);
            List<LocalityDetailInfoRecord> localityDetailInfoRecords = new ArrayList<>();
            int rowNum = 0;
            PartitionGroupRecord partitionGroupRecord = tableGroupConfig.getPartitionGroupByName(partitionGroup);
            String locality = (partitionGroupRecord == null)?"":partitionGroupRecord.getLocality();
            localityDetailInfoRecords.add(new LocalityDetailInfoRecord(rowNum++,
                LocalityDetailInfoRecord.LOCALITY_TYPE_PARTITIONGROUP,
                partitionGroupRecord.id,
                partitionGroupRecord.partition_name,
                locality));
            return localityDetailInfoRecords;
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public static List<LocalityDetailInfoRecord> getLocalityDetails(String schema, String tableGroup) {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            TableGroupConfig tableGroupConfig = TableGroupUtils.getTableGroupInfoByGroupName(schema, tableGroup);
            TableInfoManager tableInfoManager = new TableInfoManager();
            List<String> tableNames =
                tableGroupConfig.getTables().stream().map(table -> table.getTableName()).collect(Collectors.toList());
            PartitionInfoManager partitionInfoManager = OptimizerContext.getContext(schema).getPartitionInfoManager();
            tableInfoManager.setConnection(conn);
            List<TablesRecord> tableInfoList = tableInfoManager.queryTables(schema);

            List<LocalityDetailInfoRecord> localityDetailInfoRecords = new ArrayList<>();

            Long objectId;
            String objectName, locality;

            int rowNum = 0;
            for (TablesRecord tableInfo : tableInfoList) {
                objectId = tableInfo.id;
                objectName = tableInfo.tableName;
                String finalObjectName = objectName;
                if (!tableNames.stream().anyMatch(tableName -> tableName.equalsIgnoreCase(finalObjectName))) {
                    continue;
                }
                locality = partitionInfoManager.getPartitionInfo(objectName).getLocality();
                localityDetailInfoRecords.add(new LocalityDetailInfoRecord(rowNum++,
                    LocalityDetailInfoRecord.LOCALITY_TYPE_TABLE,
                    objectId,
                    objectName,
                    locality));
            }

            objectId = tableGroupConfig.getTableGroupRecord().getId();
            objectName = tableGroupConfig.getTableGroupRecord().getTg_name();
            locality = tableGroupConfig.getLocalityDesc().toString();
            localityDetailInfoRecords.add(new LocalityDetailInfoRecord(rowNum++,
                LocalityDetailInfoRecord.LOCALITY_TYPE_TABLEGROUP,
                objectId,
                objectName,
                locality));
            List<PartitionGroupRecord> partitionGroupList = tableGroupConfig.getPartitionGroupRecords();
            for (PartitionGroupRecord partitionGroupRecord : partitionGroupList) {
                locality = (partitionGroupRecord == null)?"":partitionGroupRecord.getLocality();
                localityDetailInfoRecords.add(new LocalityDetailInfoRecord(rowNum++,
                    LocalityDetailInfoRecord.LOCALITY_TYPE_PARTITIONGROUP,
                    partitionGroupRecord.id,
                    partitionGroupRecord.partition_name,
                    locality));
            }
            return localityDetailInfoRecords;
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }
}
