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

package com.alibaba.polardbx.executor.utils;

import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager;
import com.alibaba.polardbx.executor.gsi.CheckerManager;
import com.alibaba.polardbx.executor.gsi.GsiBackfillManager;
import com.alibaba.polardbx.executor.statistic.entity.PolarDbXSystemTableColumnStatistic;
import com.alibaba.polardbx.executor.statistic.entity.PolarDbXSystemTableLogicalTableStatistic;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.misc.SchemaInfoCleaner;
import com.alibaba.polardbx.gms.metadb.table.BaselineInfoAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableStatus;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.metadb.table.TablesRecord;
import com.alibaba.polardbx.gms.scheduler.DdlPlanAccessor;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupUtils;
import com.alibaba.polardbx.gms.tablegroup.TableGroupUtils;
import com.alibaba.polardbx.gms.topology.SchemaMetaCleaner;
import com.alibaba.polardbx.gms.ttl.TtlInfoAccessor;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.optimizer.view.PolarDbXSystemTableView;

import java.sql.Connection;
import java.util.List;

import static com.alibaba.polardbx.common.cdc.ICdcManager.DEFAULT_DDL_VERSION_ID;
import static com.alibaba.polardbx.common.properties.ConnectionParams.ENABLE_HLL;

/**
 * @author chenghui.lch
 */
public class SchemaMetaUtil {

    public static class PolarDbXSchemaMetaCleaner implements SchemaMetaCleaner {

        @Override
        public void clearSchemaMeta(String schemaName, Connection metaDbConn, long versionId) {
            SchemaMetaUtil.cleanupSchemaMeta(schemaName, metaDbConn, versionId);
        }
    }

    public static void cleanupSchemaMeta(String schemaName, Connection metaDbConn, long versionId) {

        TableInfoManager tableInfoManager = new TableInfoManager();
        SchemaInfoCleaner schemaInfoCleaner = new SchemaInfoCleaner();
        DdlPlanAccessor ddlPlanAccessor = new DdlPlanAccessor();
        TtlInfoAccessor ttlInfoAccessor = new TtlInfoAccessor();

        try {
            assert metaDbConn != null;

            tableInfoManager.setConnection(metaDbConn);
            schemaInfoCleaner.setConnection(metaDbConn);
            ddlPlanAccessor.setConnection(metaDbConn);
            ttlInfoAccessor.setConnection(metaDbConn);

            // If the schema has been dropped, then we have to do some cleanup.
            String tableListDataId = MetaDbDataIdBuilder.getTableListDataId(schemaName);
            MetaDbConfigManager.getInstance().unregister(tableListDataId, metaDbConn);
            String columnarTableListDataId = MetaDbDataIdBuilder.getColumnarTableListDataId(schemaName);
            MetaDbConfigManager.getInstance().unregister(columnarTableListDataId, metaDbConn);

            List<TablesRecord> records = tableInfoManager.queryTables(schemaName);
            for (TablesRecord record : records) {
                String tableDataId = MetaDbDataIdBuilder.getTableDataId(schemaName, record.tableName);
                MetaDbConfigManager.getInstance().unbindListener(tableDataId);
                MetaDbConfigManager.getInstance().unregister(tableDataId, metaDbConn);
            }

            if (versionId != DEFAULT_DDL_VERSION_ID) {
                tableInfoManager.updateColumnarTableStatusAndVersionIDBySchema(schemaName, versionId,
                    ColumnarTableStatus.DROP.name());
            }

            tableInfoManager.removeAll(schemaName);

            schemaInfoCleaner.removeAll(schemaName);
            DdlJobManager.cleanUpArchiveSchema(schemaName);

            PolarDbXSystemTableView.deleteAll(schemaName, metaDbConn);

            new PolarDbXSystemTableLogicalTableStatistic().deleteAll(schemaName, metaDbConn);
            new PolarDbXSystemTableColumnStatistic().deleteAll(schemaName, metaDbConn);

            GsiBackfillManager.deleteAll(schemaName, metaDbConn);
            CheckerManager.deleteAll(schemaName, metaDbConn);
            ddlPlanAccessor.deleteAll(schemaName);

            TableGroupUtils.deleteTableGroupInfoBySchema(schemaName, metaDbConn);
            JoinGroupUtils.deleteJoinGroupInfoBySchema(schemaName, metaDbConn);

        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG.error(e);
        } finally {
            tableInfoManager.setConnection(null);
            schemaInfoCleaner.setConnection(null);
        }
        deleteBaselineInformation(schemaName, new BaselineInfoAccessor(false));
    }

    public static boolean checkSupportHll(String schemaName) {
        if (schemaName == null || schemaName.isEmpty()) {
            throw new IllegalArgumentException("checkSupportHll with empty schema name");
        }
        if (!InstConfUtil.getBool(ENABLE_HLL)) {
            return false;
        }

        ExecutorContext executorContext = ExecutorContext.getContext(schemaName);

        // should not happen, ExecutorContext should be inited when schema inited
        if (executorContext == null) {
            return false;
        }
        return executorContext.getStorageInfoManager().supportsHyperLogLog();
    }

    /**
     * Deletes baseline information for a specified schema separately,
     * due to potential size issues with removing the entire schema in a single transaction.
     *
     * @param schemaName Name of the schema whose baseline information should be deleted.
     */
    public static void deleteBaselineInformation(String schemaName, BaselineInfoAccessor baselineInfoAccessor) {
        try (Connection metaDatabaseConnection = MetaDbDataSource.getInstance().getConnection()) {
            // Set up a new connection to the meta database specifically
            baselineInfoAccessor.setConnection(metaDatabaseConnection);

            // Delete baseline information associated with the specified schema
            baselineInfoAccessor.deleteBySchema(schemaName);
        } catch (Exception exception) {
            MetaDbLogUtil.META_DB_LOG.error("Failed to delete baseline information for schema: " + schemaName,
                exception);
        } finally {
            // Clear the connection reference to ensure proper resource cleanup.
            baselineInfoAccessor.setConnection(null);
        }
    }

}
