package com.alibaba.polardbx.executor.scaleout.fastchecker;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.fastchecker.FastChecker;
import com.alibaba.polardbx.executor.gsi.PhysicalPlanBuilder;
import com.alibaba.polardbx.executor.workqueue.PriorityWorkQueue;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;

import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class MoveTableFastChecker extends FastChecker {
    public MoveTableFastChecker(String schemaName, String srcLogicalTableName, Map<String, String> sourceTargetGroup,
                                Map<String, Set<String>> srcPhyDbAndTables, Map<String, Set<String>> dstPhyDbAndTables,
                                List<String> srcColumns,
                                PhyTableOperation planSelectHashCheckSrc,
                                PhyTableOperation planSelectHashCheckDst,
                                PhyTableOperation planIdleSelectSrc, PhyTableOperation planIdleSelectDst,
                                long parallelism, int lockTimeOut) {
        super(schemaName, srcLogicalTableName, srcLogicalTableName, sourceTargetGroup, srcPhyDbAndTables,
            dstPhyDbAndTables,
            srcColumns, srcColumns, planSelectHashCheckSrc, planSelectHashCheckDst, planIdleSelectSrc,
            planIdleSelectDst, parallelism, lockTimeOut);
    }

    public static FastChecker create(String schemaName, String tableName, Map<String, String> sourceTargetGroup,
                                     Map<String, Set<String>> srcPhyDbAndTables,
                                     Map<String, Set<String>> dstPhyDbAndTables,
                                     long parallelism, ExecutionContext ec) {
        final SchemaManager sm = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        final TableMeta tableMeta = sm.getTable(tableName);

        if (null == tableMeta) {
            throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE, "Incorrect SCALEOUT relationship.");
        }

        final List<String> allColumns = tableMeta.getAllColumns()
            .stream()
            .map(ColumnMeta::getName)
            .collect(Collectors.toList());

        if (parallelism <= 0) {
            parallelism = Math.max(PriorityWorkQueue.getInstance().getCorePoolSize() / 2, 1);
        }

        final int lockTimeOut = ec.getParamManager().getInt(ConnectionParams.FASTCHECKER_LOCK_TIMEOUT);

        final PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, ec);

        return new MoveTableFastChecker(schemaName, tableName, sourceTargetGroup, srcPhyDbAndTables,
            dstPhyDbAndTables, allColumns,
            builder.buildSelectHashCheckForChecker(tableMeta, allColumns),
            builder.buildSelectHashCheckForChecker(tableMeta, allColumns),
            builder.buildIdleSelectForChecker(tableMeta, allColumns),
            builder.buildIdleSelectForChecker(tableMeta, allColumns),
            parallelism, lockTimeOut);
    }

    @Override
    public boolean check(ExecutionContext baseEc) {
        boolean tsoCheckResult = tsoCheck(baseEc);
        if (tsoCheckResult) {
            return true;
        } else {
            SQLRecorderLogger.ddlLogger
                .warn(MessageFormat.format("[{0}] FastChecker with TsoCheck failed, begin XaCheck",
                    baseEc.getTraceId()));
        }

        /**
         * When tsoCheck is failed, bypath to use old checker directly.
         * because xaCheck of scaleout/gsi is easily to caused deadlock by using lock tables
         */
        //boolean xaCheckResult = xaCheckForHeterogeneousTable(baseEc);
        return tsoCheckResult;
    }
}
