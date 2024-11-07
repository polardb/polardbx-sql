package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.DirectMultiDBTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
import com.alibaba.polardbx.repo.mysql.spi.MyPhyTableModifyReturningCursor;
import com.alibaba.polardbx.repo.mysql.spi.MyRepository;
import com.google.common.base.Preconditions;
import org.apache.calcite.rel.RelNode;

public class MySingleTableModifyReturningHandler extends MySingleTableModifyHandler {

    public MySingleTableModifyReturningHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected Cursor handleInner(RelNode logicalPlan, ExecutionContext executionContext) {
        if (logicalPlan instanceof PhyTableOperation) {
            if (((PhyTableOperation) logicalPlan).isOnlyOnePartitionAfterPruning()) {
                PhyTableOperationUtil.enableIntraGroupParallelism(((BaseTableOperation) logicalPlan).getSchemaName(),
                    executionContext);
            }
        } else {
            if (logicalPlan instanceof DirectMultiDBTableOperation) {
                PhyTableOperationUtil.enableIntraGroupParallelism(
                    ((DirectMultiDBTableOperation) logicalPlan).getBaseSchemaName(executionContext),
                    executionContext);
            } else if (logicalPlan instanceof BaseTableOperation && !(logicalPlan instanceof PhyDdlTableOperation)) {
                PhyTableOperationUtil.enableIntraGroupParallelism(((BaseTableOperation) logicalPlan).getSchemaName(),
                    executionContext);
            }
        }

        long oldLastInsertId = executionContext.getConnection().getLastInsertId();
        Long[] result = handleWithSequence(logicalPlan, executionContext);

        Long lastInsertId = null, returnedLastInsertId = null;
        if (result != null) {
            lastInsertId = result[0];
            returnedLastInsertId = result[1];
        }

        Preconditions.checkArgument(logicalPlan instanceof BaseTableOperation);

        return new MyPhyTableModifyReturningCursor(executionContext,
            (BaseTableOperation) logicalPlan,
            (MyRepository) repo,
            oldLastInsertId,
            lastInsertId,
            returnedLastInsertId);
    }
}
