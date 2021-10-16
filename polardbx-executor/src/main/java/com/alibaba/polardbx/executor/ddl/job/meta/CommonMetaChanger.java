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

import com.alibaba.polardbx.atom.TAtomDataSource;
import com.alibaba.polardbx.atom.config.TAtomDsConfDO;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.executor.sync.BaselineValidateSyncAction;
import com.alibaba.polardbx.executor.sync.RemoveTableStatisticSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.listener.ConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager.PhyInfoSchemaContext;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;

import java.util.List;

public class CommonMetaChanger {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommonMetaChanger.class);

    private static final ConfigManager CONFIG_MANAGER = MetaDbConfigManager.getInstance();

    public static PhyInfoSchemaContext getPhyInfoSchemaContext(String schemaName, String logicalTableName,
                                                               String dbIndex, String phyTableName) {
        IGroupExecutor groupExecutor = ExecutorContext.getContext(schemaName).getTopologyHandler().get(dbIndex);

        if (groupExecutor == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "validate",
                "Not found group executor for " + dbIndex);
        }

        TGroupDataSource dataSource = (TGroupDataSource) groupExecutor.getDataSource();

        String phyTableSchema = getPhyTableSchema(dataSource);

        if (dataSource == null || TStringUtil.isBlank(logicalTableName) || TStringUtil.isBlank(phyTableName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "validate",
                "invalid data source or physical table name");
        }

        PhyInfoSchemaContext phyInfoSchemaContext = new PhyInfoSchemaContext();
        phyInfoSchemaContext.dataSource = dataSource;
        phyInfoSchemaContext.tableSchema = schemaName;
        phyInfoSchemaContext.tableName = logicalTableName;
        phyInfoSchemaContext.phyTableSchema = phyTableSchema;
        phyInfoSchemaContext.phyTableName = phyTableName;

        return phyInfoSchemaContext;
    }

    private static String getPhyTableSchema(TGroupDataSource groupDataSource) {
        if (groupDataSource != null && groupDataSource.getConfigManager() != null) {
            TAtomDataSource atomDataSource = groupDataSource.getConfigManager().getDataSource(MasterSlave.MASTER_ONLY);
            if (atomDataSource != null && atomDataSource.getDsConfHandle() != null) {
                TAtomDsConfDO runTimeConf = atomDataSource.getDsConfHandle().getRunTimeConf();
                return runTimeConf != null ? runTimeConf.getDbName() : null;
            }
        }
        return null;
    }

    public static void syncTableDataId(String schema, String tableName) {
        String tableDataId = MetaDbDataIdBuilder.getTableDataId(schema, tableName);
        sync(tableDataId);
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
                    + "seconds (twice of the time that scan interval plus notify interval in config manager)");
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
                    + "seconds (twice of the time that scan interval plus notify interval in config manager)");
            try {
                Thread.sleep(waitingTime);
            } catch (Exception ignored) {
            }
        }
    }

    public static void finalOperationsOnSuccess(String schemaName, String logicalTableName) {
        invalidatePlanCache(schemaName);
        validateBaseLine(schemaName);
        removeStatistic(schemaName, logicalTableName);
    }

    public static void invalidatePlanCache(String schemaName) {
        OptimizerContext.getContext(schemaName).getPlanManager().invalidateCache();
    }

    private static void validateBaseLine(String schemaName) {
        // null means to validate all baseline
        SyncManagerHelper.sync(new BaselineValidateSyncAction(schemaName, null), schemaName);
    }

    private static void removeStatistic(String schemaName, String logicalTableName) {
        SyncManagerHelper.sync(new RemoveTableStatisticSyncAction(schemaName, logicalTableName), schemaName);
    }

}
