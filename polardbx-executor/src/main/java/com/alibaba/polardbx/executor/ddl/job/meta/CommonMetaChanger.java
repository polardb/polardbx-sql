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

package com.alibaba.polardbx.executor.ddl.job.meta;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.sync.ClearFileSystemCacheSyncAction;
import com.alibaba.polardbx.executor.sync.DeleteOssFileSyncAction;
import com.alibaba.polardbx.executor.sync.InvalidateBufferPoolSyncAction;
import com.alibaba.polardbx.executor.sync.RemoveColumnStatisticSyncAction;
import com.alibaba.polardbx.executor.sync.RemoveTableStatisticSyncAction;
import com.alibaba.polardbx.executor.sync.RenameStatisticSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.utils.DdlUtils;
import com.alibaba.polardbx.gms.listener.ConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager.PhyInfoSchemaContext;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.config.schema.DefaultDbSchema;
import org.jetbrains.annotations.Nullable;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;

import java.util.List;

public class CommonMetaChanger {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommonMetaChanger.class);

    private static final ConfigManager CONFIG_MANAGER = MetaDbConfigManager.getInstance();

    public static PhyInfoSchemaContext getPhyInfoSchemaContext(String schemaName, String logicalTableName,
                                                               String dbIndex, String phyTableName) {
        TGroupDataSource dataSource = DdlHelper.getPhyDataSource(schemaName, dbIndex);

        String phyTableSchema = DdlHelper.getPhyTableSchema(dataSource);
        if (TStringUtil.isBlank(logicalTableName) || TStringUtil.isBlank(phyTableName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "validate", "invalid physical table name");
        }

        PhyInfoSchemaContext phyInfoSchemaContext = new PhyInfoSchemaContext();
        phyInfoSchemaContext.dataSource = dataSource;
        phyInfoSchemaContext.tableSchema = schemaName;
        phyInfoSchemaContext.tableName = logicalTableName;
        phyInfoSchemaContext.phyTableSchema = phyTableSchema;
        phyInfoSchemaContext.phyTableName = phyTableName;

        return phyInfoSchemaContext;
    }

    public static void sync(String dataId) {
        try {
            // Sync to trigger immediate call of registered listener associated with the dataId.
            CONFIG_MANAGER.sync(dataId);
        } catch (Exception e) {
            // Wait enough time to make sure that listener action can be done
            // on each node by config manager in case of sync failure.
            int waitingTime =
                2 * (MetaDbConfigManager.DEFAULT_SCAN_INTERVAL + MetaDbConfigManager.DEFAULT_NOTIFY_INTERVAL);
            LOGGER.warn(
                "Failed to sync with config manager. Caused by: " + e.getMessage()
                    + "\nLet's wait at most " + (waitingTime / 1000)
                    + "seconds (twice of the time that scan interval plus notify interval in config manager)", e);
            try {
                Thread.sleep(waitingTime);
            } catch (Exception ignored) {
            }
        }
    }

    public static void sync(List<String> dataIds) {
        try {
            // Sync to trigger immediate call of registered listener associated with the dataIds.
            for (String dataId : dataIds) {
                CONFIG_MANAGER.sync(dataId);
            }
        } catch (Exception e) {
            // Wait enough time to make sure that listener action can be done
            // on each node by config manager in case of sync failure.
            int waitingTime =
                2 * (MetaDbConfigManager.DEFAULT_SCAN_INTERVAL + MetaDbConfigManager.DEFAULT_NOTIFY_INTERVAL);
            LOGGER.warn(
                "Failed to sync with config manager. "
                    + "Let's wait at most " + (waitingTime / 1000)
                    + "seconds (twice of the time that scan interval plus notify interval in config manager)", e);
            try {
                Thread.sleep(waitingTime);
            } catch (Exception ignored) {
            }
        }
    }

    public static void finalOperationsOnSuccess(String schemaName, String logicalTableName) {
        DdlUtils.invalidatePlan(schemaName, logicalTableName, false);
        removeTableStatistic(schemaName, logicalTableName);
    }

    public static void dropTableFinalOperationsOnSuccess(String schemaName, String logicalTableName) {
        DdlUtils.invalidatePlan(schemaName, logicalTableName, true);
        removeTableStatistic(schemaName, logicalTableName);
    }

    public static void renameFinalOperationsOnSuccess(String schemaName, String logicalTableName,
                                                      String newLogicalTableName) {
        DdlUtils.invalidatePlan(schemaName, logicalTableName, true);
        renameStatistic(schemaName, logicalTableName, newLogicalTableName);
        invalidateBufferPool(schemaName, logicalTableName);
    }

    public static void alterTableColumnFinalOperationsOnSuccess(String schemaName, String logicalTableName,
                                                                List<String> columnList) {
        DdlUtils.invalidatePlan(schemaName, logicalTableName, false);
        invalidateAlterTableColumnStatistic(schemaName, logicalTableName, columnList);
        invalidateBufferPool(schemaName, logicalTableName);
    }

//    public static void invalidatePlanCache(String schemaName) {
//        PlanManager.getInstance().invalidateCacheBySchema(schemaName);
//    }

    private static void removeTableStatistic(String schemaName, String logicalTableName) {
        SyncManagerHelper.sync(new RemoveTableStatisticSyncAction(schemaName, logicalTableName), schemaName,
            SyncScope.ALL);
    }

    private static void invalidateAlterTableColumnStatistic(String schemaName, String logicalTableName,
                                                            List<String> columnList) {
        SyncManagerHelper.sync(new RemoveColumnStatisticSyncAction(schemaName, logicalTableName, columnList),
            schemaName, SyncScope.ALL);
    }

    private static void renameStatistic(String schemaName, String logicalTableName, String newLogicalTableName) {
        SyncManagerHelper.sync(new RenameStatisticSyncAction(schemaName, logicalTableName, newLogicalTableName),
            schemaName, SyncScope.ALL);
    }

    public static void invalidateBufferPool() {
        SyncManagerHelper.sync(new InvalidateBufferPoolSyncAction(), DefaultDbSchema.NAME, SyncScope.ALL);
    }

    public static void invalidateBufferPool(String schemaName) {
        invalidateBufferPool(schemaName, null);
    }

    private static void invalidateBufferPool(String schemaName, String logicalTableName) {
        SyncManagerHelper.sync(new InvalidateBufferPoolSyncAction(schemaName, logicalTableName), schemaName,
            SyncScope.ALL);
    }

    public static void clearFileSystemCache(@Nullable Engine engine, boolean all) {
        SyncManagerHelper.sync(new ClearFileSystemCacheSyncAction(engine, all), DefaultDbSchema.NAME, SyncScope.ALL);
    }

    public static void clearOSSFileSystemCache(List<String> paths, String schema) {
        SyncManagerHelper.sync(new DeleteOssFileSyncAction(paths), schema, SyncScope.ALL);
    }
}
