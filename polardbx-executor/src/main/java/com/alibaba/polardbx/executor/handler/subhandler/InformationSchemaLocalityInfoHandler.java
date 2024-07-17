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

package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.privilege.DbInfo;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.gms.locality.LocalityInfoAccessor;
import com.alibaba.polardbx.gms.locality.LocalityInfoRecord;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.metadb.table.TablesRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.DbInfoRecord;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.locality.LocalityManager;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.alibaba.polardbx.optimizer.view.InformationSchemaLocalityInfo;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author moyi
 * @since 2021/01
 */
public class InformationSchemaLocalityInfoHandler extends BaseVirtualViewSubClassHandler {
    public InformationSchemaLocalityInfoHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaLocalityInfo;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {

        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            DbInfoManager dbInfoManager = DbInfoManager.getInstance();
            for (DbInfoRecord dbInfoRecord : dbInfoManager.getDbInfoList()) {
                String schemaName = dbInfoRecord.dbName;
                Boolean isNewPartition = dbInfoRecord.isPartition();
                if (isNewPartition) {
                    handleNewPartitionDatabase(executionContext, schemaName, cursor);
                } else {
//                    handleDrdsDatabase(executionContext, schemaName, dbInfoRecord.id, cursor);
                }
            }
        } catch (SQLException e) {
            MetaDbLogUtil.META_DB_LOG.error(e);
            throw GeneralUtil.nestedException(e);
        }

        return cursor;
    }

    private void handleDrdsDatabase(ExecutionContext executionContext, String schemaName, Long id,
                                    ArrayResultCursor result) {
        result.addRow(new Object[] {schemaName, "database", schemaName, id, "", "", "", ""});
    }

    private void handleNewPartitionDatabase(ExecutionContext executionContext, String schemaName,
                                            ArrayResultCursor result) {
        TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
        PartitionInfoManager partitionInfoManager = OptimizerContext.getContext(schemaName).getPartitionInfoManager();
        LocalityManager localityManager = LocalityManager.getInstance();

        try (Connection connection = MetaDbUtil.getConnection()) {
            List<TableGroupConfig> tableGroupConfigList =
                tableGroupInfoManager.getTableGroupConfigInfoCache().values().stream().collect(
                    Collectors.toList());
            DbInfoManager dbInfoManager = DbInfoManager.getInstance();
            TableInfoManager tableInfoManager = new TableInfoManager();
            tableInfoManager.setConnection(connection);
            List<TablesRecord> tableInfoList = tableInfoManager.queryTables(schemaName);
            List<PartitionGroupRecord> partitionGroupRecordList = new ArrayList<>();

            //database
            Long objectId;
            String objectName, locality;
            objectId = dbInfoManager.getDbInfo(schemaName).id;
            objectName = schemaName;
            locality = localityManager.getLocalityOfDb(objectId).getLocality();
            result.addRow(new Object[] {schemaName, "database", objectName, objectId, "", locality, "", ""});

            for (TablesRecord tableInfo : tableInfoList) {
                objectId = tableInfo.id;
                objectName = tableInfo.tableName;
                locality = partitionInfoManager.getPartitionInfo(objectName).getLocality();
                result.addRow(new Object[] {schemaName, "table", objectName, objectId, "", locality, "", ""});
//                result.addRow(new Object[]{objectId, objectName, "table", locality, ""});
            }

            for (TableGroupConfig tableGroupConfig : tableGroupConfigList) {
                objectId = tableGroupConfig.getTableGroupRecord().getId();
                partitionGroupRecordList = tableGroupConfig.getPartitionGroupRecords();
                objectName = tableGroupConfig.getTableGroupRecord().getTg_name();
                locality = tableGroupConfig.getLocalityDesc().toString();
                String tableListString = String.join(",", tableGroupConfig.getAllTables());
                result.addRow(
                    new Object[] {schemaName, "tablegroup", objectName, objectId, "", locality, tableListString, ""});
                for (PartitionGroupRecord partitionGroupRecord : partitionGroupRecordList) {
                    Long partitionGroupId = partitionGroupRecord.id;
                    String partitionGroupName = partitionGroupRecord.partition_name;
                    locality = partitionGroupRecord.getLocality();
                    String physicalDb = partitionGroupRecord.getPhy_db();
                    if (!StringUtils.isEmpty(locality)) {
                        result.addRow(new Object[] {
                            schemaName, "partitiongroup", objectName, objectId, "", locality, "", physicalDb});
//                        result.addRow(new Object[]{partitionGroupId, objectName + "." + partitionGroupName, "partitiongroup", locality, ""});
                    }
                }
            }
        } catch (SQLException e) {
            String errMsg = String.format(
                "error occurs while show locality: %s, the error is %s", schemaName, e);
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, errMsg);
        } finally {
        }
    }

    private String objectName(ExecutionContext ec, int objectType, long objectId) {
        switch (objectType) {
        case LocalityInfoRecord.LOCALITY_TYPE_DEFAULT:
            return "default";
        case LocalityInfoRecord.LOCALITY_TYPE_DATABASE:
            return queryDatabaseName(objectId);
        case LocalityInfoRecord.LOCALITY_TYPE_TABLE:
            return queryTableName(ec, objectId);
        case LocalityInfoRecord.LOCALITY_TYPE_TABLEGROUP:
            return queryTableGroupName(ec, objectId);
        case LocalityInfoRecord.LOCALITY_TYPE_PARTITIONGROUP:
            return String.valueOf(objectId);
        default:
            return "unknown";
        }
    }

    private String queryDatabaseName(long objectId) {
        final DbInfoManager dm = DbInfoManager.getInstance();
        DbInfoRecord dbInfo = dm.getDbInfo(objectId);
        if (dbInfo == null) {
            return null;
        } else {
            return dbInfo.dbName;
        }
    }

    private String queryTableName(ExecutionContext ec, long objectId) {
        final TableInfoManager tm = ec.getTableInfoManager();
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            tm.setConnection(metaDbConn);
            TablesRecord record = tm.queryTable(objectId);
            if (record != null) {
                return record.tableName;
            }
            return null;
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        } finally {
            tm.setConnection(null);
        }
    }

    private String queryTableGroupName(ExecutionContext ec, long tgId) {
        TableGroupInfoManager tm = OptimizerContext.getContext(ec.getSchemaName()).getTableGroupInfoManager();
        TableGroupConfig tg = tm.getTableGroupConfigById(tgId);
        return tg == null ? "null" : tg.getTableGroupRecord().getTg_name();
    }

}

