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

package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.gms.GmsTableMetaManager;
import com.alibaba.polardbx.executor.gms.TableRuleManager;
import com.alibaba.polardbx.executor.mdl.MdlContext;
import com.alibaba.polardbx.executor.mdl.MdlDuration;
import com.alibaba.polardbx.executor.mdl.MdlKey;
import com.alibaba.polardbx.executor.mdl.MdlManager;
import com.alibaba.polardbx.executor.mdl.MdlRequest;
import com.alibaba.polardbx.executor.mdl.MdlTicket;
import com.alibaba.polardbx.executor.mdl.MdlType;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.planner.PlanCache;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;

import java.text.MessageFormat;
import java.util.Set;

public class TruncateSyncAction implements ISyncAction {

    private static final Logger LOGGER = LoggerFactory.getLogger(TruncateSyncAction.class);

    private String schemaName;
    private String primaryTableName;
    private String tmpPrimaryTableName;
    private String traceId;

    private boolean testMode;
    private Set<String> indexTableNames;

    // This will not conflict, because it is unique in clusters(ClusterAcceptIdGenerator).
    private long connId;

    public TruncateSyncAction(String schemaName, String primaryTableName, String tmpPrimaryTableName,
                              Set<String> indexTableNames, boolean testMode, long connId, String traceId) {
        this.schemaName = schemaName;
        this.primaryTableName = primaryTableName;
        this.tmpPrimaryTableName = tmpPrimaryTableName;
        this.indexTableNames = indexTableNames;
        this.testMode = testMode;
        this.connId = connId;
        this.traceId = traceId;
    }

    private MdlRequest writeRequest(Long trxId) {
        return new MdlRequest(trxId,
            MdlKey.getTableKeyWithLowerTableName(schemaName, primaryTableName),
            MdlType.MDL_EXCLUSIVE,
            MdlDuration.MDL_TRANSACTION);
    }

    @Override
    public ResultCursor sync() {
        syncForPolarDbX();
        return null;
    }

    private void syncForPolarDbX() {
        synchronized (OptimizerContext.getContext(schemaName)) {
            GmsTableMetaManager oldSchemaManager =
                (GmsTableMetaManager) OptimizerContext.getContext(schemaName).getLatestSchemaManager();
            TableMeta currentMeta = oldSchemaManager.getTableWithNull(primaryTableName);

            // Lock and unlock MDL on primary table to clear cross status transaction.
            final MdlContext context = MdlManager.addContext(schemaName, false);
            SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
                "{0}  {1}.addContext({2})", Thread.currentThread().getName(),
                this.hashCode(), schemaName));

            try {
                final MdlTicket ticket;
                ticket = context.acquireLock(new MdlRequest(1L,
                    MdlKey.getTableKeyWithLowerTableName(schemaName, currentMeta.getDigest()),
                    MdlType.MDL_EXCLUSIVE,
                    MdlDuration.MDL_TRANSACTION));

                if (primaryTableName != null) {
                    oldSchemaManager.tonewversion(primaryTableName);
                    if (testMode) {
                        clearShadowTablePlanCache();
                    }
                }

                SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
                    "[Mdl write lock acquired table[{0}]]",
                    primaryTableName));
                context.releaseLock(1L, ticket);

                // There should be no transaction on tmp table
                if (tmpPrimaryTableName != null) {
                    oldSchemaManager.tonewversion(tmpPrimaryTableName);
                }
            } finally {
                context.releaseAllTransactionalLocks();
                MdlManager.removeContext(context);
            }
        }
    }

    private void clearShadowTablePlanCache() {
        OptimizerContext optimizerContext = OptimizerContext.getContext(schemaName);
        if (optimizerContext != null) {
            PlanCache.getInstance().invalidate(primaryTableName);
            if (indexTableNames != null) {
                for (String indexTableName : indexTableNames) {
                    PlanCache.getInstance().invalidate(indexTableName);
                }
            }
        }
    }

    private void clear() {
        if (tmpPrimaryTableName != null) {
            try {
                OptimizerContext.getContext(schemaName).getLatestSchemaManager().reload(tmpPrimaryTableName);
                TableRuleManager.reload(schemaName, tmpPrimaryTableName);
            } catch (Exception e) {
                LOGGER.error("error occur while AlterPartitionKey schema change", e);
                try {
                    OptimizerContext.getContext(schemaName).getLatestSchemaManager().invalidate(tmpPrimaryTableName);
                } catch (Exception ignore) {
                }
            }
        }
        if (primaryTableName != null) {
            try {
                OptimizerContext.getContext(schemaName).getLatestSchemaManager().reload(primaryTableName);
                TableRuleManager.reload(schemaName, primaryTableName);
            } catch (Exception e) {
                LOGGER.error("error occur while AlterPartitionKey schema change", e);
                try {
                    OptimizerContext.getContext(schemaName).getLatestSchemaManager().invalidate(primaryTableName);
                } catch (Exception ignore) {
                }
            }
        }

        // Note: Invalidate plan cache is still necessary,
        // because non-multi-write plan for simple table may be cached.
        PlanManager.getInstance().invalidateCache();
    }

    public String getTraceId() {
        return this.traceId;
    }

    public void setTraceId(final String traceId) {
        this.traceId = traceId;
    }

    public long getConnId() {
        return this.connId;
    }

    public void setConnId(final long connId) {
        this.connId = connId;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getPrimaryTableName() {
        return primaryTableName;
    }

    public void setPrimaryTableName(String primaryTableName) {
        this.primaryTableName = primaryTableName;
    }

    public String getTmpPrimaryTableName() {
        return tmpPrimaryTableName;
    }

    public void setTmpPrimaryTableName(String tmpPrimaryTableName) {
        this.tmpPrimaryTableName = tmpPrimaryTableName;
    }

    public boolean isTestMode() {
        return testMode;
    }

    public void setTestMode(boolean testMode) {
        this.testMode = testMode;
    }

    public Set<String> getIndexTableNames() {
        return indexTableNames;
    }

    public void setIndexTableNames(Set<String> indexTableNames) {
        this.indexTableNames = indexTableNames;
    }
}
