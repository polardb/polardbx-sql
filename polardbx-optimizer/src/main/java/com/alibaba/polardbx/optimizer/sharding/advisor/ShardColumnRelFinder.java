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

package com.alibaba.polardbx.optimizer.sharding.advisor;

import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.sql.SqlKind;

/**
 * finds all columns that can be used to push down.
 * Currently, only {@link RexInputRef} and {@link org.apache.calcite.rex.RexLiteral} in equation
 * are considered.
 *
 * @author shengyu
 */
public class ShardColumnRelFinder extends RelVisitor {
    private RelMetadataQuery mq;

    private final ShardColumnEdges shardColumnEdges;
    private final ShardColumnFinder shardColumnFinder;
    private int count;

    public ShardColumnRelFinder(ParamManager paramManager) {
        shardColumnEdges = new ShardColumnEdges(paramManager);
        shardColumnFinder = new ShardColumnFinder(paramManager);
    }

    public void setMq(RelMetadataQuery mq) {
        this.mq = mq;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public ShardColumnEdges getShardColumnEdges() {
        return shardColumnEdges;
    }

    @Override
    public void visit(RelNode node, int ordinal, RelNode parent) {
        if (node instanceof LogicalView) {
            visit(((LogicalView) node).getOptimizedPushedRelNodeForMetaQuery(), 0, node);
            return;
        }
        if (node instanceof Filter) {
            Filter filter = (Filter) node;
            shardColumnFinder.init(mq, node, shardColumnEdges, count);
            shardColumnFinder.findEqualCondition(filter.getCondition());
        } else if (node instanceof Join) {
            Join logicalJoin = (Join) node;
            shardColumnFinder.init(mq, node, shardColumnEdges, count);
            shardColumnFinder.findEqualCondition(logicalJoin.getCondition());
        } else if (node instanceof LogicalCorrelate) {
            LogicalCorrelate logicalCorrelate = (LogicalCorrelate) node;
            if (logicalCorrelate.getOpKind() == SqlKind.EQUALS) {
                shardColumnFinder.init(mq, node, shardColumnEdges, count);
                shardColumnFinder.findEqualCondition(logicalCorrelate.getLeftConditions());
            }
        } else if (node instanceof TableScan) {
            // record the primary key of each table
            TableScan tableScan = (TableScan) node;
            TableMeta tableMeta = CBOUtil.getTableMeta(tableScan.getTable());
            // ignore gsi table
            if (tableMeta.isGsi()) {
                return;
            }
            if (tableMeta.isHasPrimaryKey()) {
                IndexMeta indexMeta = tableMeta.getPrimaryIndex();
                String schema = tableMeta.getSchemaName();
                String table = tableMeta.getTableName();
                if (AdvisorUtil.notBacktick(table)) {
                    return;
                }
                int column = tableMeta.getAllColumns().indexOf(indexMeta.getKeyColumns().get(0));
                String columnName = tableScan.getTable().getRowType().getFieldNames().get(column);

                // if the table is already a partition table, don't consider primary key
                if (OptimizerContext.getContext(schema).getLatestSchemaManager().getTddlRuleManager().isShard(table)) {
                    return;
                }

                // if the primary key don't support hash, don't consider the key
                if (AdvisorUtil.columnHashable(tableMeta, column)) {
                    return;
                }
                long rowCount = (long) tableMeta.getRowCount(null);
                shardColumnEdges.addPrimeKey(schema, table, columnName, column, rowCount);
                return;
            }
        }
        node.childrenAccept(this);
    }
}
