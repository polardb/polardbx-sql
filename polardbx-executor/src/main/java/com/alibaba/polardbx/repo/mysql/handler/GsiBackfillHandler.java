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

package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.MetricLevel;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.executor.backfill.Loader;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.CheckGsiTask;
import com.alibaba.polardbx.executor.gsi.BackfillExecutor;
import com.alibaba.polardbx.executor.gsi.corrector.GsiChecker;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.GsiBackfill;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlSelect;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.executor.utils.ExecUtils.getQueryConcurrencyPolicy;

/**
 * Process backfill for GSI. GsiBackfillHandler extends CalciteHandlerCommon
 * because we're going to reuse `executeWithConcurrentPolicy` to execute
 * INSERTs.
 */
public class GsiBackfillHandler extends HandlerCommon {

    private static final Logger LOG = SQLRecorderLogger.ddlLogger;

    public GsiBackfillHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        GsiBackfill backfill = (GsiBackfill) logicalPlan;
        String schemaName = backfill.getSchemaName();
        String baseTableName = backfill.getBaseTableName();
        List<String> indexNames = backfill.getIndexNames();
        List<String> columnsName = backfill.getColumns();
        Map<String, String> srcCheckColumnMap = backfill.getSrcCheckColumnMap();
        Map<String, String> dstCheckColumnMap = backfill.getDstCheckColumnMap();
        List<String> modifyStringColumns = backfill.getModifyStringColumns();
        boolean useChangeSet = backfill.isUseChangeSet();
        boolean onlineModifyColumn = backfill.isOnlineModifyColumn();

        BackfillExecutor backfillExecutor = new BackfillExecutor((List<RelNode> inputs,
                                                                  ExecutionContext executionContext1) -> {
            // backfill batch insert 开启 group concurrent，在有前端流量时，会产生主键冲突，在 RR 下会产生 GAP 锁，容易发生死锁。
            // 关闭改参数，还原为以前的 SEQUENTIAL，减少死锁
            // 后续会将 backfill batch insert 事务改成 XA + RC 避免 gap 锁
            executionContext1.getExtraCmds()
                .put(ConnectionProperties.ENABLE_DML_GROUP_CONCURRENT_IN_TRANSACTION, false);
            QueryConcurrencyPolicy queryConcurrencyPolicy = getQueryConcurrencyPolicy(executionContext1);
            if (Loader.canUseBackfillReturning(executionContext1, schemaName)) {
                queryConcurrencyPolicy = QueryConcurrencyPolicy.GROUP_CONCURRENT_BLOCK;
            }
            List<Cursor> inputCursors = new ArrayList<>(inputs.size());
            executeWithConcurrentPolicy(executionContext1, inputs, queryConcurrencyPolicy, inputCursors, schemaName);
            return inputCursors;
        });

        boolean useBinary = executionContext.getParamManager().getBoolean(ConnectionParams.BACKFILL_USING_BINARY);
        boolean omcForce = executionContext.getParamManager().getBoolean(ConnectionParams.OMC_FORCE_TYPE_CONVERSION);
        boolean canUseReturning = Loader.canUseBackfillReturning(executionContext, schemaName);

        // online modify column, does not clear sql_mode
        if (onlineModifyColumn) {
            executionContext = setChangeSetApplySqlMode(executionContext.copy());
            if (!useBinary && !omcForce) {
                // select + insert, need encoding
                upgradeEncoding(executionContext, schemaName, baseTableName);
            }
            // 暂时不使用 backfill insert ignore returning 优化，因为无法处理 sql_mode 严格模式行为
            canUseReturning = false;
        } else {
            executionContext = clearSqlMode(executionContext.copy());
            if (!useBinary) {
                upgradeEncoding(executionContext, schemaName, baseTableName);
            }
        }

        executionContext.getExtraCmds().put(ConnectionProperties.MPP_METRIC_LEVEL, MetricLevel.SQL.metricLevel);

        PhyTableOperationUtil.disableIntraGroupParallelism(schemaName, executionContext);

        // Force master first and following will copy this EC.
        executionContext.getExtraCmds().put(ConnectionProperties.MASTER, true);
        int affectRows;
        if (backfill.isAddColumnsBackfill()) {
            // Add column on clustered GSI.
            assert indexNames.size() > 0;
            affectRows = backfillExecutor
                .addColumnsBackfill(schemaName, baseTableName, indexNames, columnsName, executionContext);
        } else if (backfill.isMirrorCopy()) {
            // Normal creating GSI.
            assert 1 == indexNames.size();
            affectRows =
                backfillExecutor.mirrorCopyGsiBackfill(schemaName, baseTableName, indexNames.get(0), useChangeSet,
                    useBinary, onlineModifyColumn, executionContext);
        } else {
            // Normal creating GSI.
            assert 1 == indexNames.size();
            affectRows =
                backfillExecutor.backfill(schemaName, baseTableName, indexNames.get(0), useBinary, useChangeSet,
                    canUseReturning, modifyStringColumns, onlineModifyColumn, executionContext);
        }

        // Check GSI immediately after creation by default.
        final ParamManager pm = executionContext.getParamManager();
        boolean check = pm.getBoolean(ConnectionParams.GSI_CHECK_AFTER_CREATION) && !useChangeSet;
        if (!check) {
            return new AffectRowCursor(affectRows);
        }

        String lockMode = SqlSelect.LockMode.UNDEF.toString();
        GsiChecker.Params params = GsiChecker.Params.buildFromExecutionContext(executionContext);

        // TODO(moyi) separate check to another task
        for (String indexName : indexNames) {
            boolean isPrimaryBroadCast =
                OptimizerContext.getContext(schemaName).getRuleManager().isBroadCast(baseTableName);
            boolean isGsiBroadCast = OptimizerContext.getContext(schemaName).getRuleManager().isBroadCast(indexName);

            CheckGsiTask checkTask =
                new CheckGsiTask(schemaName, baseTableName, indexName, lockMode, lockMode, params, false, "",
                    isPrimaryBroadCast, isGsiBroadCast, onlineModifyColumn);
            if (onlineModifyColumn) {
                checkTask.setSrcCheckColumnMap(srcCheckColumnMap);
                checkTask.setDstCheckColumnMap(dstCheckColumnMap);
            }
            checkTask.checkInBackfill(executionContext);
        }

        return new AffectRowCursor(affectRows);
    }
}
