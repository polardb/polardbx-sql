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

package com.alibaba.polardbx.optimizer.core.planner.Xplanner;

import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;

/**
 * @version 1.0
 */
public class SpecialFunctionRelFinder extends RelVisitor {

    private boolean hasGeoFunction = false;
    private boolean hasTrimFunction = false;
    private boolean pureDynParamInProj = false;

    public boolean isHasGeoFunction() {
        return hasGeoFunction;
    }

    public boolean isHasTrimFunction() {
        return hasTrimFunction;
    }

    public boolean isPureDynParamInProj() {
        return pureDynParamInProj;
    }

    public boolean supportGalaxyPrepare() {
        return !isHasGeoFunction() && // GEO function not supported.
            !isHasTrimFunction() && // Bugs on AliSQL 5.7.
            !pureDynParamInProj; // Bugs on AliSQL 5.7.
    }

    @Override
    public void visit(RelNode node, int ordinal, RelNode parent) {
        if (node instanceof Filter) {
            final SpecialFunctionRexFinder rexFinder = new SpecialFunctionRexFinder();
            ((Filter) node).getCondition().accept(rexFinder);
            if (rexFinder.isHasGeoFunction()) {
                hasGeoFunction = true;
            }
            if (rexFinder.isHasTrimFunction()) {
                hasTrimFunction = true;
            }
        } else if (node instanceof Project) {
            final SpecialFunctionRexFinder rexFinder = new SpecialFunctionRexFinder();
            for (RexNode rex : ((Project) node).getProjects()) {
                rex.accept(rexFinder);
                if (rex instanceof RexDynamicParam && !(parent instanceof TableModify)) {
                    // set to true only if dyn param in select list of sub-query
                    pureDynParamInProj = true;
                }
            }
            if (rexFinder.isHasGeoFunction()) {
                hasGeoFunction = true;
            }
            if (rexFinder.isHasTrimFunction()) {
                hasTrimFunction = true;
            }
        } else if (node instanceof LogicalJoin) {
            final SpecialFunctionRexFinder rexFinder = new SpecialFunctionRexFinder();
            ((LogicalJoin) node).getCondition().accept(rexFinder);
            if (rexFinder.isHasGeoFunction()) {
                hasGeoFunction = true;
            }
            if (rexFinder.isHasTrimFunction()) {
                hasTrimFunction = true;
            }
        } else if (node instanceof LogicalSemiJoin) {
            final SpecialFunctionRexFinder rexFinder = new SpecialFunctionRexFinder();
            ((LogicalSemiJoin) node).getCondition().accept(rexFinder);
            if (rexFinder.isHasGeoFunction()) {
                hasGeoFunction = true;
            }
            if (rexFinder.isHasTrimFunction()) {
                hasTrimFunction = true;
            }
        } else if (node instanceof LogicalView) {
            final RelNode pushedRelNode = ((LogicalView) node).getPushedRelNode();
            go(pushedRelNode);
        }
        node.childrenAccept(this);
    }

}
