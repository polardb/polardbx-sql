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

package com.alibaba.polardbx.optimizer.core.rel.Xplan;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Table scan for X-plan which can be Get, Multi-Get and TableScan
 *
 * @version 1.0
 */
public class XPlanTableScan extends TableScan {

    private final RelDataType originalRowType;

    // Which physical index to scan.
    private String getIndex = null;
    // List of GetExpr which represent equal key conditions on physical index.
    private final List<List<XPlanEqualTuple>> getExprs = new ArrayList<>();
    // TableProject if needed.
    private final List<Integer> projects = new ArrayList<>();

    private RelNode nodeForMetaQuery;

    public XPlanTableScan(TableScan scan) {
        super(scan.getCluster(), scan.getTraitSet(), scan.getTable(), scan.getHints(), scan.getIndexNode(),
            scan.getPartitions());
        this.originalRowType = scan.getRowType();
    }

    public RelDataType getOriginalRowType() {
        return originalRowType;
    }

    public String getGetIndex() {
        return getIndex;
    }

    public void setGetIndex(String getIndex) {
        this.getIndex = getIndex;
    }

    public List<List<XPlanEqualTuple>> getGetExprs() {
        return getExprs;
    }

    public List<Integer> getProjects() {
        return projects;
    }

    public synchronized RelNode getNodeForMetaQuery() {
        if (nodeForMetaQuery == null) {
            nodeForMetaQuery = LogicalTableScan.create(getCluster(), table, hints, indexNode, null);
            if (!getExprs.isEmpty()) {
                assert 1 == getExprs.size();
                RexNode filter = null;
                for (XPlanEqualTuple tuple : getExprs.get(0)) {
                    final RexNode rex = getCluster().getRexBuilder()
                        .makeCall(tuple.getOperator(), tuple.getKey(), tuple.getValue());
                    if (null == filter) {
                        filter = rex;
                    } else {
                        filter = getCluster().getRexBuilder()
                            .makeCall(SqlStdOperatorTable.AND, filter, rex);
                    }
                }
                nodeForMetaQuery = LogicalFilter.create(nodeForMetaQuery, filter);
            }
            if (!projects.isEmpty()) {
                final List<RexInputRef> refs =
                    projects.stream().map(idx -> getCluster().getRexBuilder().makeInputRef(nodeForMetaQuery, idx))
                        .collect(Collectors.toList());
                nodeForMetaQuery = LogicalProject.create(nodeForMetaQuery, refs, deriveRowType());
            }
        }
        return nodeForMetaQuery;
    }

    public synchronized void resetNodeForMetaQuery() {
        nodeForMetaQuery = null;
    }

    @Override
    public RelDataType deriveRowType() {
        assert originalRowType == super.deriveRowType();

        if (projects.isEmpty()) {
            return originalRowType;
        }

        final RelDataTypeFactory.Builder builder = this.getCluster().getTypeFactory().builder();
        for (Integer idx : projects) {
            builder.add(originalRowType.getFieldList().get(idx));
        }
        return builder.build();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .item("getIndex", getIndex)
            .item("getExprs", getExprs)
            .item("projects", projects);
    }
}
