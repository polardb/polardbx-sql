package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.physicalbackfill.PhysicalBackfillExecutor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalBackfill;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
import org.apache.calcite.rel.RelNode;

import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.executor.utils.ExecUtils.getQueryConcurrencyPolicy;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class PhysicalBackfillHandler extends HandlerCommon {

    public PhysicalBackfillHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        PhysicalBackfill backfill = (PhysicalBackfill) logicalPlan;
        String schemaName = backfill.getSchemaName();
        String logicalTable = backfill.getLogicalTableName();

        PhysicalBackfillExecutor backfillExecutor = new PhysicalBackfillExecutor();

        executionContext = clearSqlMode(executionContext.copy());

        upgradeEncoding(executionContext, schemaName, logicalTable);

        PhyTableOperationUtil.disableIntraGroupParallelism(schemaName, executionContext);

        Map<String, Set<String>> sourcePhyTables = backfill.getSourcePhyTables();
        Map<String, Set<String>> targetPhyTables = backfill.getTargetPhyTables();
        Map<String, String> sourceTargetGroup = backfill.getSourceTargetGroup();
        boolean isBroadcast = backfill.getBroadcast();
        backfillExecutor.backfill(schemaName, logicalTable, sourcePhyTables, targetPhyTables, sourceTargetGroup,
            isBroadcast, executionContext);
        return new AffectRowCursor(0);
    }
}
