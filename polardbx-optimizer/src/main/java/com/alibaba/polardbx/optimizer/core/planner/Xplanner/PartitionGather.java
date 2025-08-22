/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.core.planner.Xplanner;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.LogicalDynamicValues;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModifyView;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.PhyQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSelect;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PartitionGather extends RelVisitor {
    protected static final Logger logger = LoggerFactory.getLogger(PartitionGather.class);

    final ExecutionContext ec;
    @Getter
    // schema -> group -> isWrite
    final Map<String, Map<String, Boolean>> targetGroups = new HashMap<>();

    public PartitionGather(ExecutionContext ec) {
        this.ec = ec;
    }

    @Override
    public void visit(RelNode node, int ordinal, RelNode parent) {
        try {
            if (node instanceof LogicalView) {
                final String schema = ((LogicalView) node).getSchemaName();
                final Map<String, List<List<String>>> tmp = ((LogicalView) node).getTargetTables(ec);
                if (tmp != null) {
                    for (final Map.Entry<String, List<List<String>>> entry : tmp.entrySet()) {
                        final String group = entry.getKey();
                        final SqlSelect.LockMode lockMode = ((LogicalView) node).getLockMode();
                        final boolean isWrite =
                            node instanceof LogicalModifyView || SqlSelect.LockMode.SHARED_LOCK == lockMode
                                || SqlSelect.LockMode.EXCLUSIVE_LOCK == lockMode;
                        targetGroups.compute(schema, (k, v) -> {
                            if (null == v) {
                                v = new HashMap<>();
                                v.put(group, isWrite);
                            } else {
                                v.compute(group, (k1, v1) -> null == v1 ? isWrite : v1 || isWrite);
                            }
                            return v;
                        });
                    }
                }
                return;
            } else if (node instanceof LogicalInsert) {
                // Init param
                final Parameters parameterSettings;
                final Map<Integer, ParameterContext> params = ec.getParamMap();
                if (params != null) {
                    // Copy param
                    parameterSettings = new Parameters(new HashMap<>(params), false);
                } else {
                    parameterSettings = new Parameters(new HashMap<>(), false);
                }

                final ExecutionContext executionContext = new ExecutionContext();
                executionContext.setParams(parameterSettings);
                if (ec.getSchemaName() != null) {
                    executionContext.setSchemaName(ec.getSchemaName());
                }

                final LogicalInsert logicalInsert = ((LogicalInsert) node);
                final RelNode input = logicalInsert.getInput();
                if (input instanceof LogicalValues || input instanceof LogicalDynamicValues) {
                    if (!logicalInsert.isSourceSelect()) {
                        RexUtils.calculateAndUpdateAllRexCallParams(logicalInsert, executionContext);
                    }

                    // For batch insert, change params index.
                    if (logicalInsert.getBatchSize() > 0) {
                        logicalInsert.buildParamsForBatch(executionContext);
                    }

                    if (null == logicalInsert.getTargetTablesHintCache()) {
                        RexUtils.updateParam(logicalInsert, executionContext, true, null);
                    }

                    final List<RelNode> inputs = logicalInsert.getPhyPlanForDisplay(executionContext, logicalInsert);
                    for (final RelNode phyOps : inputs) {
                        if (phyOps instanceof PhyTableOperation) {
                            targetGroups.compute(((PhyTableOperation) phyOps).getSchemaName(), (k, v) -> {
                                if (null == v) {
                                    v = new HashMap<>();
                                    v.put(((PhyTableOperation) phyOps).getDbIndex(), true);
                                } else {
                                    v.compute(((PhyTableOperation) phyOps).getDbIndex(), (k1, v1) -> true);
                                }
                                return v;
                            });
                        }
                    }
                }
            } else if (node instanceof BaseTableOperation) {
                final String schema = ((BaseTableOperation) node).getSchemaName();
                final Pair<String, Map<Integer, ParameterContext>> dbIndexAndParam =
                    ((BaseTableOperation) node).getDbIndexAndParam(ec.getParamMap(), null, ec);
                final String group = dbIndexAndParam.getKey();
                final boolean isWrite =
                    ((BaseTableOperation) node).withLock() || ((BaseTableOperation) node).getKind() != SqlKind.SELECT;
                targetGroups.compute(schema, (k, v) -> {
                    if (null == v) {
                        v = new HashMap<>();
                        v.put(group, isWrite);
                    } else {
                        v.compute(group, (k1, v1) -> null == v1 ? isWrite : v1 || isWrite);
                    }
                    return v;
                });
            } else if (node instanceof PhyQueryOperation) {
                final String schema = ((PhyQueryOperation) node).getSchemaName();
                final String group = ((PhyQueryOperation) node).getDbIndex();
                final boolean isWrite = ((PhyQueryOperation) node).getLockMode() != SqlSelect.LockMode.UNDEF
                    || ((PhyQueryOperation) node).getKind() != SqlKind.SELECT;
                targetGroups.compute(schema, (k, v) -> {
                    if (null == v) {
                        v = new HashMap<>();
                        v.put(group, isWrite);
                    } else {
                        v.compute(group, (k1, v1) -> null == v1 ? isWrite : v1 || isWrite);
                    }
                    return v;
                });
            }
        } catch (Exception e) {
            logger.error("Error in PartitionGather", e);
            return;
        }
        node.childrenAccept(this);
    }
}
