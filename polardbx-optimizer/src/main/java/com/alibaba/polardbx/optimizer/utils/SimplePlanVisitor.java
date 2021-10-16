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

package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.BroadcastTableModify;
import com.alibaba.polardbx.optimizer.core.rel.Gather;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.dal.BaseDalOperation;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalValues;

/**
 * @author chenghui.lch
 */
public class SimplePlanVisitor extends RelShuttleImpl {

    protected boolean isSamplePlan = false;

    public SimplePlanVisitor() {
    }

    public RelNode visit(BaseQueryOperation other) {

        if (!isParentGatherOrEmpty()) {
            return other;
        }
        isSamplePlan = true;
        return other;
    }

    public RelNode visit(LogicalInsert other) {
        if (!isParentGatherOrEmpty()) {
            return other;
        }
        isSamplePlan = true;
        return other;
    }

    public RelNode visit(BroadcastTableModify other) {
        if (!isParentGatherOrEmpty()) {
            return other;
        }
        isSamplePlan = true;
        return other;
    }

    public RelNode visit(DDL ddl) {
        if (!isParentGatherOrEmpty()) {
            return ddl;
        }
        isSamplePlan = true;
        return ddl;
    }

    public RelNode visit(Gather gather) {
        return visitChildren(gather);
    }

    @Override
    public RelNode visit(TableScan scan) {
        if (scan instanceof LogicalView) {
            if (!isParentGatherOrEmpty()) {
                return scan;
            }
            isSamplePlan = true;
        }
        return scan;
    }

    @Override
    public RelNode visit(LogicalValues values) {

        if (!isParentGatherOrEmpty()) {
            return values;
        }
        isSamplePlan = true;
        return values;
    }

    @Override
    public RelNode visit(LogicalFilter filter) {

        if (!isParentGatherOrEmpty()) {
            return filter;
        }

        RelNode inputRel = filter.getInput();
        if (inputRel instanceof BaseDalOperation) {
            isSamplePlan = true;
        }
        return filter;
    }

    @Override
    public RelNode visit(LogicalProject project) {
        if (!isParentGatherOrEmpty()) {
            return project;
        }
        if (project.getInput(0) != null
            && project.getInput(0) instanceof LogicalValues) {
            isSamplePlan = true;
        }
        return project;
    }

    @Override
    public RelNode visit(LogicalSort sort) {
        if (!isParentGatherOrEmpty()) {
            return sort;
        }
        RelNode inputRel = sort.getInput();
        if (inputRel instanceof LogicalShow) {
            isSamplePlan = true;
        }
        return sort;

    }

    @Override
    public RelNode visit(RelNode other) {
        if (!isParentGatherOrEmpty()) {
            return other;
        }
        if (other instanceof BaseQueryOperation) {
            visit((BaseQueryOperation) other);
        }
        return visitChildren(other);

    }

    protected boolean isParentGatherOrEmpty() {
        if (stack.isEmpty()) {
            return true;
        }

        if (stack.getFirst() instanceof Gather) {
            return true;
        }

        return false;

    }

    public boolean isSamplePlan() {
        return isSamplePlan;
    }
}


