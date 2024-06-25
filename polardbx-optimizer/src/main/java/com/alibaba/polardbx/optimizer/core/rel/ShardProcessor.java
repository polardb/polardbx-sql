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

import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStep;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionTupleRouteInfo;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.google.common.base.Preconditions;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.model.sqljep.ExtComparative;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStep;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionTupleRouteInfo;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.rule.TableRule;
import com.google.common.base.Preconditions;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.collections.MapUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * @author lingce.ldm 2017-12-08 14:12
 */
public abstract class ShardProcessor {

    protected final TableRule tableRule;

    protected ShardProcessor(TableRule TableRule) {
        this.tableRule = TableRule;
    }

    public static ShardProcessor createPartTblShardProcessor(String schemaName,
                                                             String tableName,
                                                             PartitionPruneStep stepInfo,
                                                             PartitionTupleRouteInfo routeInfo,
                                                             boolean isQueryRouting) {
        ShardProcessor shardProcessor;
        if (isQueryRouting) {
            shardProcessor = new PartTableQueryShardProcessor(stepInfo);
        } else {
            shardProcessor = new PartTableInsertShardProcessor(routeInfo);
        }
        return shardProcessor;
    }

    public static ShardProcessor build(String schemaName,
                                       String tableName,
                                       List<String> columns,
                                       TableRule tableRule,
                                       Map<String, Comparative> comparatives,
                                       Map<String, DataType> dataTypes) {
        Map<String, Integer> condIndex = buildColumnCondIndex(columns, comparatives);
        if (MapUtils.isEmpty(condIndex)) {
            return new EmptyShardProcessor(tableRule);
        }

        /**
         * 简单规则
         */
        if (isSimpleRule(tableRule)) {
            Preconditions.checkArgument(condIndex.size() == 1);
            return new SimpleShardProcessor(tableRule,
                condIndex.values().iterator().next(),
                dataTypes.get(columns.get(0)));
        }

        /**
         * 非简单规则
         */
        return new NormalShardProcessor(schemaName, tableName, columns, tableRule, comparatives, dataTypes, condIndex);
    }

    public static boolean isSimpleRule(TableRule tableRule) {
        return tableRule.isSimple()
            && (tableRule.getExtPartitions() == null || tableRule.getExtPartitions().size() == 0);
    }

    public static ShardProcessor build(LogicalView logicalView, String tableName, ExecutionContext ec) {
        TddlRuleManager or = OptimizerContext.getContext(logicalView.getSchemaName()).getRuleManager();
        TableRule tr = or.getTableRule(tableName);

        List<String> shardColumns = tr.getShardColumns();
        Map<String, Comparative> comparatives = logicalView.getComparative();
        SchemaManager schemaManager = ec.getSchemaManager(logicalView.getSchemaName());
        Map<String, DataType> dataTypeMap =
            PlannerUtils.buildDataType(shardColumns, schemaManager.getTable(tableName));
        return ShardProcessor.build(logicalView.getSchemaName(),
            tableName,
            shardColumns,
            tr,
            comparatives,
            dataTypeMap);
    }

    public static SimpleShardProcessor buildSimple(LogicalView logicalView, String tableName, ExecutionContext ec) {
        TddlRuleManager or = ec.getSchemaManager().getTddlRuleManager();
        TableRule tr = or.getTableRule(tableName);

        List<String> shardColumns = tr.getShardColumns();
        SchemaManager schemaManager = ec.getSchemaManager(logicalView.getSchemaName());
        Map<String, DataType> dataTypeMap =
            PlannerUtils.buildDataType(shardColumns, schemaManager.getTable(tableName));
        return new SimpleShardProcessor(tr,
            0,
            dataTypeMap.get(shardColumns.get(0)));
    }

    public static boolean canSimpleShard(List<String> shardColumns,
                                         Map<String, Comparative> comparatives) {
        for (String col : shardColumns) {
            Comparative c = comparatives.get(col);
            if (!PlannerUtils.comparativeIsASimpleEqual(c)) {
                return false;
            }

            if (!(c.getValue() instanceof RexDynamicParam)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 构建列对应的参数的下标
     */
    private static Map<String, Integer> buildColumnCondIndex(List<String> columns,
                                                             Map<String, Comparative> comparatives) {
        Map<String, Integer> columnIndex = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        for (String col : columns) {
            Comparative c = comparatives.get(col);
            if (c == null || !PlannerUtils.comparativeIsASimpleEqual(c)) {
                return null;
            }

            if (c.getValue() instanceof RexDynamicParam) {
                RexDynamicParam dynamicParam = (RexDynamicParam) c.getValue();
                columnIndex.put(col, dynamicParam.getIndex());
            } else {
                return null;
            }
        }
        return columnIndex;
    }

    /**
     * 判断 Comparative 是一个简单的等值比较
     */
    private static boolean comparativeIsAEqual(Comparative c) {
        if (c.getValue() instanceof Comparative) {
            return false;
        }

        if (c.getComparison() == Comparative.Equivalent) {
            return true;
        }

        return false;
    }

    public static TreeSet<String> listToTreeSet(List<String> list) {
        TreeSet<String> treeSet = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        if (list == null) {
            return treeSet;
        }
        treeSet.addAll(list);
        return treeSet;
    }

    public static ShardProcessor buildSimpleShard(LogicalInsert logicalInsert) {

        // build comparatives after processing sequence
        String tableName = logicalInsert.getLogicalTableName();
        String schemaName = logicalInsert.getSchemaName();
        OptimizerContext context = OptimizerContext.getContext(schemaName);

        ShardProcessor processor = null;
        if (!DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            TableRule tableRule = context.getRuleManager().getTableRule(tableName);
            List<String> shardColumns = tableRule.getShardColumns();
            Map<String, DataType> dataTypeMap = PlannerUtils.buildDataType(shardColumns,
                PlannerContext.getPlannerContext(logicalInsert).getExecutionContext()
                    .getSchemaManager(logicalInsert.getSchemaName()).getTable(tableName));
            Map<String, Comparative> comparatives = new HashMap<>();

            // get full row type
            LogicalDynamicValues values = (LogicalDynamicValues) logicalInsert.getInput();
            List<RelDataTypeField> fields = values.getRowType().getFieldList();
            for (int i = 0; i < fields.size(); i++) {
                for (String shardColumn : shardColumns) {
                    if (fields.get(i).getName().equalsIgnoreCase(shardColumn)) {
                        RexNode rexNode = values.getTuples().get(0).get(i);
                        Comparative comparative = new ExtComparative(shardColumn, Comparative.Equivalent, rexNode);
                        comparatives.put(shardColumn, comparative);
                        break;
                    }
                }
            }
            processor = ShardProcessor.build(
                schemaName,
                tableName,
                shardColumns,
                tableRule,
                comparatives,
                dataTypeMap);

        } else {
            // the table is in new part db
            processor = ShardProcessor
                .createPartTblShardProcessor(schemaName, tableName, null, logicalInsert.getTupleRoutingInfo(), false);
        }

        return processor;
    }

    /**
     * 根据规则和参数值计算出唯一的一组 GroupName 和 RuleName
     */
    abstract Pair<String, String> shard(Map<Integer, ParameterContext> param, ExecutionContext executionContext);

    public TableRule getTableRule() {
        return tableRule;
    }
}
