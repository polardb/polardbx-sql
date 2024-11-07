package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.MetricLevel;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.backfill.Loader;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.CheckGsiTask;
import com.alibaba.polardbx.executor.gsi.BackfillExecutor;
import com.alibaba.polardbx.executor.gsi.corrector.GsiChecker;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.GsiPkRangeBackfill;
import com.alibaba.polardbx.optimizer.core.row.Row;
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
 * Process backfill for GSI. GsiPkRangeBackfillHandler extends CalciteHandlerCommon
 * because we're going to reuse `executeWithConcurrentPolicy` to execute
 * INSERTs.
 */
public class GsiPkRangeBackfillHandler extends HandlerCommon {

    private static final Logger LOG = SQLRecorderLogger.ddlLogger;

    public GsiPkRangeBackfillHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        GsiPkRangeBackfill backfill = (GsiPkRangeBackfill) logicalPlan;
        String schemaName = backfill.getSchemaName();
        String baseTableName = backfill.getBaseTableName();
        List<String> indexNames = backfill.getIndexNames();
        List<String> columnsName = backfill.getColumns();
        int totalThreadCount = backfill.getTotalThreadCount();
        Map<String, String> virtualColumnMap = backfill.getSrcCheckColumnMap();
        Map<String, String> backfillColumnMap = backfill.getDstCheckColumnMap();
        List<String> modifyStringColumns = backfill.getModifyStringColumns();
        boolean useChangeSet = backfill.isUseChangeSet();
        boolean modifyColumn = backfill.isOnlineModifyColumn();
        Pair<Map<Integer, ParameterContext>, Map<Integer, ParameterContext>> pkRange = backfill.getPkRange();

        BackfillExecutor backfillExecutor = new BackfillExecutor((List<RelNode> inputs,
                                                                  ExecutionContext executionContext1) -> {
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
        if (modifyColumn) {
            executionContext = setChangeSetApplySqlMode(executionContext);
            if (!useBinary && !omcForce) {
                // select + insert, need encoding
                upgradeEncoding(executionContext, schemaName, baseTableName);
            }
            // 暂时不使用 backfill insert ignore returning 优化，因为无法处理 sql_mode 严格模式行为
            canUseReturning = false;
        } else {
            executionContext = clearSqlMode(executionContext);
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
                    useBinary, modifyColumn, executionContext);
        } else {
            // Normal creating GSI.
            assert 1 == indexNames.size();
            affectRows =
                backfillExecutor.backfill(schemaName, baseTableName, indexNames.get(0), useBinary, useChangeSet,
                    canUseReturning, modifyStringColumns, pkRange, null, modifyColumn, totalThreadCount, executionContext);
        }

        // Check GSI immediately after creation by default.
        final ParamManager pm = executionContext.getParamManager();
        boolean check = pm.getBoolean(ConnectionParams.GSI_CHECK_AFTER_CREATION) && !useChangeSet;
        if (!check) {
            return new AffectRowCursor(affectRows);
        }

        String lockMode = SqlSelect.LockMode.UNDEF.toString();
        GsiChecker.Params params = GsiChecker.Params.buildFromExecutionContext(executionContext);

        for (String indexName : indexNames) {
            baseTableName = getPrimaryTableName(schemaName, baseTableName, backfill.isMirrorCopy(), executionContext);
            boolean isPrimaryBroadCast =
                OptimizerContext.getContext(schemaName).getRuleManager().isBroadCast(baseTableName);
            boolean isGsiBroadCast = OptimizerContext.getContext(schemaName).getRuleManager().isBroadCast(indexName);
//            CheckGsiTask checkTask =
//                new CheckGsiTask(schemaName, baseTableName, indexName, lockMode, lockMode, params, false, "",
//                    isPrimaryBroadCast, isGsiBroadCast, virtualColumnMap, backfillColumnMap);
//
//            checkTask.checkInBackfill(executionContext);
        }

        return new AffectRowCursor(affectRows);
    }

    public static String getPrimaryTableName(String schemaName, String baseTableName, boolean mirrorCopy,
                                             ExecutionContext executionContext) {
        String primaryTableName = baseTableName;
        TableMeta sourceTableMeta = executionContext.getSchemaManager(schemaName).getTable(baseTableName);
        if (mirrorCopy && sourceTableMeta.isGsi()) {
            primaryTableName = sourceTableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
        }
        return primaryTableName;
    }

}
