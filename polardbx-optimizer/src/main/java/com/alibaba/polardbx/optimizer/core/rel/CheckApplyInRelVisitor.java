package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

import java.util.ArrayList;
import java.util.List;

/**
 * check the rexNode whether contain the 'APPLY_IN_VALUES_PARAM_INDEX'
 */
public class CheckApplyInRelVisitor extends RelShuttleImpl {

    protected List<RexDynamicParam> applyInValues = new ArrayList<>();

    @Override
    public RelNode visit(LogicalFilter filter) {

        CheckApplyInRexVisitor checkApplyInRexVisitor = new CheckApplyInRexVisitor();
        RexNode rexNode = filter.getCondition();
        rexNode.accept(checkApplyInRexVisitor);
        return super.visit(filter);
    }

    @Override
    public RelNode visit(TableScan scan) {

        if (scan instanceof LogicalView) {
            RelNode rexNodePushed = ((LogicalView) scan).getPushDownOpt().getPushedRelNode();
            rexNodePushed.accept(this);
        }
        return super.visit(scan);
    }


    public List<RexDynamicParam> getApplyInValues() {
        return applyInValues;
    }

    public class CheckApplyInRexVisitor extends RexShuttle {

        @Override
        public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
            if (dynamicParam.getIndex() == PlannerUtils.APPLY_IN_VALUES_PARAM_INDEX) {
                applyInValues.add(dynamicParam);
            }
            return dynamicParam;
        }
    }

}
