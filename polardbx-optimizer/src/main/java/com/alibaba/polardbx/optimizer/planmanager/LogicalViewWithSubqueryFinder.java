package com.alibaba.polardbx.optimizer.planmanager;

import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;

import java.util.ArrayList;
import java.util.List;

public class LogicalViewWithSubqueryFinder extends RelShuttleImpl {

    private List<LogicalView> result = new ArrayList<>();

    public LogicalViewWithSubqueryFinder() {
    }

    @Override
    public RelNode visit(TableScan scan) {
        if (scan instanceof LogicalView) {
            result.add((LogicalView) scan);
        }
        return scan;
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        RexUtil.RexSubqueryListFinder finder = new RexUtil.RexSubqueryListFinder();
        filter.getCondition().accept(finder);
        for (RexSubQuery subQuery : finder.getSubQueries()) {
            subQuery.rel.accept(this);
        }
        return visitChild(filter, 0, filter.getInput());
    }

    @Override
    public RelNode visit(LogicalProject project) {
        RexUtil.RexSubqueryListFinder finder = new RexUtil.RexSubqueryListFinder();
        for (RexNode node : project.getProjects()) {
            node.accept(finder);
        }
        for (RexSubQuery subQuery : finder.getSubQueries()) {
            subQuery.rel.accept(this);
        }
        return visitChild(project, 0, project.getInput());
    }

    @Override
    public RelNode visit(LogicalJoin join) {
        RexUtil.RexSubqueryListFinder finder = new RexUtil.RexSubqueryListFinder();
        join.getCondition().accept(finder);
        for (RexSubQuery subQuery : finder.getSubQueries()) {
            subQuery.rel.accept(this);
        }
        return super.visit(join);
    }

    public List<LogicalView> getResult() {
        return result;
    }
}
