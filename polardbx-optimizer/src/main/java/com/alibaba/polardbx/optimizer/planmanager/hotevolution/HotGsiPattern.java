package com.alibaba.polardbx.optimizer.planmanager.hotevolution;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.BKAJoin;
import com.alibaba.polardbx.optimizer.core.rel.Gather;
import com.alibaba.polardbx.optimizer.core.rel.LogicalIndexScan;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.sql.SqlSelect;
import org.apache.commons.collections.CollectionUtils;

/**
 * check whether the plan follows the following pattern
 */
public class HotGsiPattern extends RelVisitor {
    /**
     * true only if one lookup join in the table
     */
    private boolean canEvolute;
    private LogicalIndexScan targetIndexScan;

    public HotGsiPattern() {
        this.canEvolute = false;
        this.targetIndexScan = null;
    }

    @Override
    public void visit(RelNode node, int ordinal, RelNode parent) {
        /**
         * (any node expect BiRel,window or tableModify)
         *            |
         *         BKA join
         *        /        \
         *  (Gather/sort)  (GATHER)
         *     /              \
         *  IndexScan         LV
         */
        if (!(node instanceof BiRel
            || node instanceof Window
            || node instanceof TableModify)) {
            super.visit(node, ordinal, parent);
        }

        if (node instanceof BKAJoin) {
            RelNode outer = ((Join) node).getOuter();
            RelNode inner = ((Join) node).getInner();
            while (outer instanceof Gather || outer instanceof Sort) {
                outer = outer.getInput(0);
            }
            while (inner instanceof Gather) {
                inner = inner.getInput(0);
            }
            if (outer instanceof LogicalIndexScan && inner instanceof LogicalView) {
                LogicalIndexScan indexScan = (LogicalIndexScan) outer;
                LogicalView logicalView = (LogicalView) inner;
                SqlSelect.LockMode lockMode = indexScan.getLockMode();
                // don't evaluate if lock mode is set
                if (!(lockMode == null || lockMode == SqlSelect.LockMode.UNDEF)) {
                    return;
                }
                // don't evaluate if has subquery
                if (CollectionUtils.isNotEmpty(indexScan.getCorrelateVariableScalar())
                    || CollectionUtils.isNotEmpty(logicalView.getCorrelateVariableScalar())) {
                    return;
                }
                // outer side must be gsi of inner side
                if ((indexScan.getTableNames().size() == 1 && logicalView.getTableNames().size() == 1)) {
                    TableMeta tm = CBOUtil.getTableMeta(logicalView.getTable());
                    if (tm.withGsi(TddlSqlToRelConverter.unwrapGsiName(indexScan.getLogicalTableName()))) {
                        setEvolute(indexScan);
                    }
                }
            }
        }
    }

    private void setEvolute(LogicalIndexScan indexScan) {
        canEvolute = true;
        targetIndexScan = indexScan;
    }

    boolean canEvolute() {
        return canEvolute;
    }

    private LogicalIndexScan getTargetIndexScan() {
        return targetIndexScan;
    }

    /**
     * check whether the plan follows the hot gsi pattern
     *
     * @param rel the root of the plan
     * @return Pair<canEvolute, targetIndexScan>
     * canEvolute is true if the plan follows the hot gsi pattern, false otherwise
     * targetIndexScan is the gsi to be checked, NULL if not found
     */
    public static Pair<Boolean, LogicalIndexScan> findPattern(RelNode rel) {
        HotGsiPattern hotGsiPattern = new HotGsiPattern();
        hotGsiPattern.go(rel);
        return Pair.of(hotGsiPattern.canEvolute(), hotGsiPattern.getTargetIndexScan());
    }
}