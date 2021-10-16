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

package com.alibaba.polardbx.optimizer.core.rel;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableLookup;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;

import java.util.List;

/**
 * @author chenmo.cm
 */
public class CheckBkaJoinRelVisitor extends RelShuttleImpl {

    protected boolean isSupportUseBkaJoin = true;

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
        this.isSupportUseBkaJoin = false;
        return aggregate;
    }

    @Override
    public RelNode visit(TableScan scan) {

        if (scan instanceof LogicalView) {
            RelNode rexNodePushed = ((LogicalView) scan).getPushDownOpt().getPushedRelNode();
            rexNodePushed.accept(this);
            if (!this.isSupportUseBkaJoin) {
                return scan;
            }
        }
        return super.visit(scan);
    }

    @Override
    public RelNode visit(LogicalFilter filter) {

        CheckBkaJoinRexVisitor checkBkaJoinRexVisitor = new CheckBkaJoinRexVisitor();
        RexNode rexNode = filter.getCondition();
        rexNode.accept(checkBkaJoinRexVisitor);
        if (!checkBkaJoinRexVisitor.isSupportUseBkaJoin()) {
            this.isSupportUseBkaJoin = false;
            return filter;
        } else {
            return super.visit(filter);
        }
    }

    @Override
    public RelNode visit(LogicalProject project) {

        CheckBkaJoinRexVisitor checkBkaJoinRexVisitor = new CheckBkaJoinRexVisitor();
        List<RexNode> projRexList = project.getProjects();
        for (int i = 0; i < projRexList.size(); ++i) {
            RexNode rexNode = projRexList.get(i);
            rexNode.accept(checkBkaJoinRexVisitor);
            if (!checkBkaJoinRexVisitor.isSupportUseBkaJoin()) {
                this.isSupportUseBkaJoin = false;
                return project;
            }
        }
        return super.visit(project);
    }

    @Override
    public RelNode visit(LogicalJoin join) {
        this.isSupportUseBkaJoin = false;
        return join;
    }

    @Override
    public RelNode visit(LogicalTableLookup tableLookup) {
        visit(tableLookup.getProject());
        return tableLookup;
    }

    @Override
    public RelNode visit(LogicalCorrelate correlate) {
        this.isSupportUseBkaJoin = false;
        return correlate;

    }

    @Override
    public RelNode visit(MultiJoin mjoin) {
        this.isSupportUseBkaJoin = false;
        return mjoin;
    }

    @Override
    public RelNode visit(LogicalSort sort) {

        if (sort.offset != null || sort.fetch != null) {
            this.isSupportUseBkaJoin = false;
            return sort;
        } else {
            return super.visit(sort);
        }
    }

    @Override
    public RelNode visit(RelNode other) {
        return super.visit(other);
    }

    public static class CheckBkaJoinRexVisitor extends RexShuttle {

        protected boolean isSupportUseBkaJoin = true;

        @Override
        public RexNode visitSubQuery(RexSubQuery subQuery) {
            this.isSupportUseBkaJoin = false;
            return subQuery;
        }

        @Override
        public RexNode visitCorrelVariable(RexCorrelVariable variable) {
            this.isSupportUseBkaJoin = false;
            return variable;
        }

        @Override
        public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
            this.isSupportUseBkaJoin = false;
            return fieldAccess;
        }

        public boolean isSupportUseBkaJoin() {
            return isSupportUseBkaJoin;
        }

        public void setSupportUseBkaJoin(boolean supportUseBkaJoin) {
            this.isSupportUseBkaJoin = supportUseBkaJoin;
        }
    }

    public boolean isSupportUseBkaJoin() {
        return this.isSupportUseBkaJoin;
    }

    public void setSupportUseBkaJoin(boolean supportUseBkaJoin) {
        this.isSupportUseBkaJoin = supportUseBkaJoin;
    }

}
