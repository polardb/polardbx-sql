package com.alibaba.polardbx.optimizer.partition.util;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.rel.Gather;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.PartPrunedResult;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruner;
import org.apache.calcite.rel.RelNode;

import java.util.List;

/**
 * A Full Condition Expression Partition Router
 * for the internal usage of routing,
 * such as Split hot value.
 *
 * <pre>
 *     Notice:
 *          (1)
 *          PartCondExprRouter is non-thread-safe，
 *          so route condition one by one.
 *          (2)
 *          PartCondExprRouter is not support parameterized condExpr like 'a> ? and b < ?'
 *     e.g:
 *     PartCondExprRouter router = new PartCondExprRouter(partInfo, ec, "a> 1000 and b < 2000");
 *     router.init()
 *     PartPrunedResult result = router.route()
 *     ...
 *
 *
 * </pre>
 *
 * @author chenghui.lch
 */
public class PartCondExprRouter extends AbstractLifecycle {
    private PartitionInfo partInfo;
    private PartitionByDefinition partByDef;
    private ExecutionContext context;
    private boolean useSubPartBy = false;
    private String condExprString = "";
    private RelNode tarLogicalView = null;

    public PartCondExprRouter(PartitionInfo partInfo,
                              ExecutionContext ec,
                              String condExprString) {
        this.partInfo = partInfo;
        this.context = ec.copy();
        this.useSubPartBy = partInfo.getPartitionBy().getSubPartitionBy() != null;
        this.condExprString = condExprString;
    }

    @Override
    protected void doInit() {
        initPartCondExprRouter();
    }

    public PartPrunedResult route() {
        if (tarLogicalView != null) {
            List<PartPrunedResult> prunedResults = PartitionPruner.prunePartitions(tarLogicalView, context);
            if (prunedResults != null && prunedResults.size() > 0) {
                PartPrunedResult prunedResult = prunedResults.get(0);
                return prunedResult;
            }
        }
        return null;
    }

    protected void initPartCondExprRouter() {
        String tblSchema = partInfo.getTableSchema();
        String tblName = partInfo.getTableName();
        List<String> partCols = partInfo.getPartitionColumns();
        String selectSql = buildSelectPartTblByWhereCondExpr(tblSchema, tblName, partCols, condExprString);
        try {
            SqlParameterized sqlParameterized = SqlParameterizeUtils.parameterize(selectSql);
            ExecutionPlan plan = Planner.getInstance().doBuildPlan(sqlParameterized, context);
            RelNode planRel = plan.getPlan();
            LogicalView tarLv = null;
            if (planRel instanceof LogicalView) {
                tarLv = (LogicalView) planRel;
            } else if (planRel instanceof Gather) {
                Gather gather = (Gather) planRel;
                if (gather.getInput() instanceof LogicalView) {
                    tarLv = (LogicalView) gather.getInput();
                }
            }
            this.tarLogicalView = tarLv;
        } catch (Throwable ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                String.format("Failed to build LogicalView for select by using the condition expression: %s ",
                    condExprString));
        }
    }

    protected String buildSelectPartTblByWhereCondExpr(String tableSchema,
                                                       String tableName,
                                                       List<String> partCols,
                                                       String condExprSqlString) {

        String partColListStr = "";
        for (int i = 0; i < partCols.size(); i++) {
            if (i > 0) {
                partColListStr += ",";
            }
            partColListStr += String.format("`%s`", partCols.get(i));
        }
        String selectTtlTblByWhereCondExpr =
            String.format("select %s from `%s`.`%s` force index(primary) where %s", partColListStr, tableSchema,
                tableName, condExprSqlString);
        return selectTtlTblByWhereCondExpr;
    }

}
