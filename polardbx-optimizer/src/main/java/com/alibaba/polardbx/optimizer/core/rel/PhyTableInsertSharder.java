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

import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.hint.util.HintUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.exception.NoFoundPartitionsException;
import com.alibaba.polardbx.optimizer.partition.pruning.PartPrunedResult;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruner;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionTupleRouteInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.model.TargetDB;
import com.alibaba.polardbx.rule.utils.CalcParamsAttribute;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlInsert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PhyTableInsertSharder {

    /**
     * SQL Template, containing all values.
     */
    private SqlInsert sqlTemplate;
    private LogicalInsert parent;
    private Parameters parameterSettings;
    private boolean autoValueOnZero;

    private Long lastInsertId = null;
    private Long returnedLastInsertId = null;
    private boolean usingSequence = true;

    /**
     * Generated sequence values. Format: [{fieldIndex, seqValue}, ...], each
     * Map corresponds to one row.
     */
    private List<Map<Integer, Long>> sequenceValues = null;

    private String schemaName = null;

    public PhyTableInsertSharder(LogicalInsert parent, Parameters parameterSettings, boolean autoValueOnZero) {
        this.parent = parent;
        this.sqlTemplate = (SqlInsert) parent.getSqlTemplate();
        this.parameterSettings = parameterSettings;
        this.autoValueOnZero = autoValueOnZero;
        this.schemaName = parent.getSchemaName();
    }

    public long getLastInsertId() {
        if (lastInsertId != null) {
            return lastInsertId;
        } else {
            return 0;
        }
    }

    public void setLastInsertId(Long lastInsertId) {
        this.lastInsertId = lastInsertId;
    }

    public long getReturnedLastInsertId() {
        if (returnedLastInsertId != null) {
            return returnedLastInsertId;
        } else {
            return 0;
        }
    }

    public void setReturnedLastInsertId(Long returnedLastInsertId) {
        this.returnedLastInsertId = returnedLastInsertId;
    }

    public boolean isUsingSequence() {
        return usingSequence;
    }

    public void setUsingSequence(boolean usingSequence) {
        this.usingSequence = usingSequence;
    }

    public SqlInsert getSqlTemplate() {
        return sqlTemplate;
    }

    public Parameters getParameterSettings() {
        return parameterSettings;
    }

    public void setParameterSettings(Parameters parameterSettings) {
        this.parameterSettings = parameterSettings;
    }

    public List<Map<Integer, Long>> getSequenceValues() {
        return sequenceValues;
    }

    public void setSequenceValues(List<Map<Integer, Long>> sequenceValues) {
        this.sequenceValues = sequenceValues;
    }

    public List<PhyTableShardResult> shard(ExecutionContext executionContext) {
        return shard(executionContext, false);
    }

    public List<PhyTableShardResult> shard(ExecutionContext executionContext,
                                           boolean isGetShardResultForReplicationTable) {

        String schemaName = executionContext.getSchemaName();
        Map<String, Object> extraCmd = executionContext.getExtraCmds();
        String logicalTableName = parent.getLogicalTableName();

        TddlRuleManager or = OptimizerContext.getContext(parent.getSchemaName()).getRuleManager();
        List<PhyTableShardResult> shardResults = null;
        // single db, single table
        if (or.isTableInSingleDb(logicalTableName)) {

            // Transaction doesn't support MySQL auto increment column. So we
            // use group sequence even in single table.
            handleWithSequence(schemaName, true);

            String groupIndex = "";
            String physicalTableName;
            if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                PartitionInfo partitionInfo = OptimizerContext.getContext(schemaName).
                    getPartitionInfoManager().getPartitionInfo(logicalTableName);
                physicalTableName = partitionInfo.getPrefixTableName();

                Map<String, Set<String>> topology = partitionInfo.getTopology();
                assert(topology.size() == 1);
                for (Map.Entry<String, Set<String>> entry : topology.entrySet()) {
                    groupIndex = entry.getKey();
                }
            } else {
                TableRule tr = or.getTableRule(logicalTableName);
                if (tr != null) {
                    groupIndex = tr.getDbNamePattern();
                } else {
                    groupIndex = or.getDefaultDbIndex(logicalTableName);
                }
                physicalTableName = tr.getTbNamePattern();
            }

            PhyTableShardResult shardResult = new PhyTableShardResult(groupIndex, physicalTableName, null);
            shardResults = Lists.newArrayList(shardResult);

        } else if (or.isBroadCast(logicalTableName)) {

            handleWithSequence(schemaName, true);
            List<String> groupNames = HintUtil.allGroup(parent.getSchemaName());

            // May use jingwei to sync broadcast table
            boolean enableBroadcast =
                executionContext.getParamManager().getBoolean(ConnectionParams.CHOOSE_BROADCAST_WRITE);
            if (!enableBroadcast && groupNames != null) {
                groupNames = groupNames.subList(0, 1);
            }

            String physicalTableName;
            if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                PartitionInfo partitionInfo = OptimizerContext.getContext(schemaName).
                    getPartitionInfoManager().getPartitionInfo(logicalTableName);
                physicalTableName = partitionInfo.getPrefixTableName();
            } else {
                TableRule tr = or.getTableRule(logicalTableName);
                physicalTableName = tr.getTbNamePattern();
            }

            shardResults = new ArrayList<>();
            for (String groupIndex : groupNames) {
                PhyTableShardResult shardResult = new PhyTableShardResult(groupIndex, physicalTableName, null);
                shardResults.add(shardResult);
            }

        } else {
            // Steps: fill with sequence; shard; build SqlInsert; transform to
            // sql and params.
            handleWithSequence(schemaName, false);
            RelNode input = parent.getInput();
            shardResults = shardValues(input, logicalTableName, executionContext);
        }

        return shardResults;
    }

    /**
     * Find explicit(SqlSequenceFunction) and implicit(SqlLiteral) sequences in
     * values, and assign values for them. Q: Why not replace implicit sequences
     * with SqlSequenceFunction in planner rule to take advantage of the
     * PlanCache? A: For instance, "insert into tb(id) values(0)", `0` should be
     * replaced by SqlSequenceFunction. However, `0` is replaced by `?` in
     * planner, so we can't figure out whether it should be replaced. We must
     * scan the values again after running the planner.
     */
    public void handleWithSequence(String schemaName, boolean isSingleTable) {
        if (ConfigDataMode.isFastMock()) {
            return;
        }
        if (OptimizerContext.getContext(schemaName).isSqlMock()) {
            return;
        }

        int seqColumnIndex = parent.getSeqColumnIndex();

        usingSequence = seqColumnIndex >= 0;

        // If it's not using sequence, and it's not using batch, which means
        // there won't be NEXTVAL, skip calculating sequence.
        if (usingSequence || parameterSettings == null || !parameterSettings.isBatch()) {
            // explicit call to NEXTVAL may exist in single table INSERT
            ReplaceSequenceWithLiteralVisitor visitor = new ReplaceSequenceWithLiteralVisitor(parameterSettings,
                seqColumnIndex,
                parent.getSchemaName(),
                parent.getLogicalTableName(),
                autoValueOnZero);
            sqlTemplate = visitor.visit(sqlTemplate);

            lastInsertId = visitor.getLastInsertId();
            if (lastInsertId == null) {
                returnedLastInsertId = visitor.getReturnedLastInsertId();
            } else {
                returnedLastInsertId = lastInsertId;
            }

            sequenceValues = visitor.getSequenceValues();
            parameterSettings = visitor.getParameters();
        }
    }

    public List<PhyTableShardResult> shardValues(RelNode relNode, String logicalTableName,
                                                 ExecutionContext executionContext) {
        return shardValues(relNode, logicalTableName, executionContext, false);
    }

    /**
     * @param relNode LogicalDynamicValues(in case of parameterized sql) or
     * LogicalValues(in case of original sql)
     * @return <groupIndex, tableName> : [rowIndex1, rowIndex2, ...]
     */
    public List<PhyTableShardResult> shardValues(RelNode relNode, String logicalTableName,
                                                 ExecutionContext executionContext,
                                                 boolean isGetShardResultForReplicationTable) {
        if (relNode instanceof LogicalValues) { // not parameterized
            LogicalValues logicalValues = (LogicalValues) relNode;
            List<Pair<Integer, RelDataTypeField>> shardColumns = getShardColumns(logicalValues.getRowType()
                .getFieldList(), logicalTableName, executionContext);
            List<ImmutableList<RexLiteral>> tuples = logicalValues.getTuples();
            return getShardResultByValues(tuples, shardColumns, logicalTableName, executionContext,
                isGetShardResultForReplicationTable);
        } else if (relNode instanceof LogicalDynamicValues) { // parameterized
            if (executionContext.getLoadDataContext() != null
                && executionContext.getLoadDataContext().getShardProcessor() != null
                && !executionContext.getLoadDataContext().isGsiInsertTurn()) {
                return getShardResultByValues(
                    executionContext.getLoadDataContext().getShardProcessor(), executionContext);
            } else {
                LogicalDynamicValues dynamicValues = (LogicalDynamicValues) relNode;
                List<Pair<Integer, RelDataTypeField>> shardColumns = getShardColumns(dynamicValues.getRowType()
                    .getFieldList(), logicalTableName, executionContext);
                List<ImmutableList<RexNode>> tuples = dynamicValues.getTuples();
                return getShardResultByValues(tuples, shardColumns, logicalTableName, executionContext,
                    isGetShardResultForReplicationTable);
            }
        } else { // explain insert select
            return new ArrayList<>();
        }
    }

    private List<Pair<Integer, RelDataTypeField>> getShardColumns(List<RelDataTypeField> fieldList,
                                                                  String logicalTableName, ExecutionContext ec) {
        OptimizerContext context = OptimizerContext.getContext(parent.getSchemaName());
        TddlRuleManager or = context.getRuleManager();
        List<String> shardColNames = or.getSharedColumns(logicalTableName);
        TableMeta tableMeta = ec.getSchemaManager(parent.getSchemaName()).getTable(parent.getLogicalTableName());
        List<Pair<Integer, RelDataTypeField>> shardColumns = new ArrayList<>(shardColNames.size());
        RelDataTypeFactory typeFactory = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
        for (String colName : shardColNames) {
            boolean found = false;
            for (int i = 0; i < fieldList.size(); i++) {
                RelDataTypeField field = fieldList.get(i);
                if (field.getName().equalsIgnoreCase(colName)) {
                    // field is created according to sql statement, not table
                    // meta.
                    // If the column is defined `date` and passed `char`, we
                    // should use `date`.
                    RelDataTypeField relDataTypeField = tableMeta.getRowTypeIgnoreCase(colName, typeFactory);
                    Pair<Integer, RelDataTypeField> columnInfo = new Pair<>(i, relDataTypeField);
                    shardColumns.add(columnInfo);
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw new TddlRuntimeException(ErrorCode.ERR_INSERT_CONTAINS_NO_SHARDING_KEY, logicalTableName,
                    colName);
            }
        }
        return shardColumns;
    }

    private List<PhyTableShardResult> getShardResultByValues(
        SimpleShardProcessor simpleShardProcessor,
        ExecutionContext executionContext) {
        int iterSize = 0;
        Map<Pair<String, String>, List<Integer>> shardResult = new HashMap<>();
        Map<Integer, ParameterContext> params = null;
        if (parameterSettings != null) {
            if (parameterSettings.isBatch()) {
                iterSize = parameterSettings.getBatchSize();
            } else {
                params = parameterSettings.getCurrentParameter();
            }
        } else {
            params = new HashMap<>();
        }

        for (int i = 0; i < iterSize; i++) {
            if (parameterSettings.isBatch()) {
                params = parameterSettings.getBatchParameters().get(i);
            }

            String groupIndex = null;
            String tableName = null;
            Pair<String, String> target = simpleShardProcessor.shard(params, executionContext);
            groupIndex = target.getKey();
            tableName = target.getValue();
            Pair<String, String> phyTable = new Pair<>(groupIndex, tableName);
            List<Integer> values = shardResult.get(phyTable);
            if (values == null) {
                values = new ArrayList<>();
                shardResult.put(phyTable, values);
            }
            values.add(i);
        }

        List<PhyTableShardResult> phyTableShardResults = new ArrayList<>(shardResult.size());
        for (Map.Entry<Pair<String, String>, List<Integer>> entry : shardResult.entrySet()) {
            Pair<String, String> groupAndTableName = entry.getKey();
            PhyTableShardResult result = new PhyTableShardResult(groupAndTableName.getKey(),
                groupAndTableName.getValue(),
                entry.getValue());
            phyTableShardResults.add(result);
        }
        return phyTableShardResults;
    }

    private <T extends RexNode> List<PhyTableShardResult> getShardResultByValues(List<ImmutableList<T>> tuples,
                                                                                 List<Pair<Integer, RelDataTypeField>> shardColumns,
                                                                                 String logicalTableName,
                                                                                 ExecutionContext executionContext,
                                                                                 boolean isGetShardResultForReplicationTable) {
        OptimizerContext context = OptimizerContext.getContext(parent.getSchemaName());
        Map<Pair<String, String>, List<Integer>> shardResult = new HashMap<>();

        boolean isBatch = false;
        int iterSize = tuples.size();
        Map<Integer, ParameterContext> params = null;
        if (parameterSettings != null) {
            if (parameterSettings.isBatch()) {
                isBatch = true;
                iterSize = parameterSettings.getBatchSize();
            } else {
                params = parameterSettings.getCurrentParameter();
            }
        } else {
            params = new HashMap<>();
        }

        // Use ColumnMeta instead of RelDataTypeField to shard
        TableMeta tableMeta =
            executionContext.getSchemaManager(parent.getSchemaName()).getTable(parent.getLogicalTableName());
        List<DataType> dataTypes = new ArrayList<>();
        for (Pair<Integer, RelDataTypeField> pair : shardColumns) {
            ColumnMeta columnMeta = tableMeta.getColumnIgnoreCase(pair.getValue().getName());
            dataTypes.add(columnMeta.getDataType());
        }

        ExecutionContext tmpEc = executionContext.copy();
        for (int i = 0; i < iterSize; i++) {

            ImmutableList<T> rowValues;
            if (isBatch) {
                params = parameterSettings.getBatchParameters().get(i);
                rowValues = tuples.get(0);
            } else {
                rowValues = tuples.get(i);
            }
            Pair<String, String> phyTable =
                doInsertSharding(parent,
                    shardColumns,
                    logicalTableName,
                    tmpEc, params, dataTypes, i,
                    rowValues,
                    isBatch, isGetShardResultForReplicationTable);
            List<Integer> values = shardResult.get(phyTable);
            if (values == null) {
                values = new ArrayList<>();
                shardResult.put(phyTable, values);
            }
            values.add(i);
        }

        List<PhyTableShardResult> phyTableShardResults = new ArrayList<>(shardResult.size());
        for (Map.Entry<Pair<String, String>, List<Integer>> entry : shardResult.entrySet()) {
            Pair<String, String> groupAndTableName = entry.getKey();
            PhyTableShardResult result = new PhyTableShardResult(groupAndTableName.getKey(),
                groupAndTableName.getValue(),
                entry.getValue());
            phyTableShardResults.add(result);
        }
        return phyTableShardResults;
    }

    private <T extends RexNode> Pair<String, String> doInsertSharding(
        LogicalInsert parent,
        List<Pair<Integer, RelDataTypeField>> shardColumns,
        String logicalTableName,
        ExecutionContext executionContext,
        Map<Integer, ParameterContext> params,
        List<DataType> dataTypes,
        int i,
        ImmutableList<T> rowValues, boolean isBatch,
        boolean isGetShardResultForReplicationTable) {

        String groupIndex;
        String tableName;
        TddlRuleManager ruleManager = executionContext.getSchemaManager(parent.getSchemaName()).getTddlRuleManager();
        PartitionInfoManager partitionInfoManager = ruleManager.getPartitionInfoManager();
        if (!partitionInfoManager.isNewPartDbTable(logicalTableName)) {

            Map<String, Comparative> comparatives;
            comparatives = TddlRuleManager.getInsertComparative(rowValues,
                shardColumns,
                params,
                sequenceValues == null ? null : sequenceValues.get(i),
                dataTypes);

            Map<String, Object> calcParams = new HashMap<>();
            calcParams.put(CalcParamsAttribute.SHARD_FOR_EXTRA_DB, false);
            final Map<String, Comparative> insertFullComparative =
                TddlRuleManager.getInsertFullComparative(comparatives);
            final Map<String, Map<String, Comparative>> stringMapMap = Maps.newHashMap();
            stringMapMap.put(logicalTableName, insertFullComparative);
            calcParams.put(CalcParamsAttribute.COM_DB_TB, stringMapMap);
            calcParams.put(CalcParamsAttribute.CONN_TIME_ZONE, executionContext.getTimeZone());
            List<TargetDB> dbs = ruleManager.shard(logicalTableName,
                true,
                true,
                comparatives,
                params,
                calcParams, executionContext);
            if (dbs.size() != 1) {
                throw new TddlRuntimeException(ErrorCode.ERR_INSERT_SHARD);
            }
            groupIndex = dbs.get(0).getDbIndex();
            tableName = (String) dbs.get(0).getTableNames().toArray()[0];

        } else {

            TableMeta tableMeta =
                executionContext.getSchemaManager(parent.getSchemaName()).getTable(logicalTableName);
            PartitionTupleRouteInfo tupleRouting =
                isGetShardResultForReplicationTable ?
                    parent.getReplicationTupleRoutingInfo(tableMeta.getNewPartitionInfo()) :
                    parent.getTupleRoutingInfo();
            Parameters tmpParameters = new Parameters(params);
            executionContext.setParams(tmpParameters);
            int tupleTemplateIdx = i;
            if (isBatch) {
                tupleTemplateIdx = 0;
            }
            PartPrunedResult result =
                PartitionPruner.doPruningByTupleRouteInfo(tupleRouting, tupleTemplateIdx, executionContext);
            List<PhysicalPartitionInfo> prunedPartnfos = result.getPrunedParttions();
            if (prunedPartnfos.size() == 0) {
                throw new NoFoundPartitionsException();
            }
            PhysicalPartitionInfo prunedPart = prunedPartnfos.get(0);
            groupIndex = prunedPart.getGroupKey();
            tableName = prunedPart.getPhyTable();
        }
        return new Pair<>(groupIndex, tableName);
    }

    public static class PhyTableShardResult {

        private String groupName;
        private String phyTableName;
        private List<Integer> valueIndices;

        public PhyTableShardResult(String groupName, String phyTableName, List<Integer> valueIndices) {
            this.groupName = groupName;
            this.phyTableName = phyTableName;
            this.valueIndices = valueIndices;
        }

        public String getGroupName() {
            return groupName;
        }

        public String getPhyTableName() {
            return phyTableName;
        }

        public List<Integer> getValueIndices() {
            return valueIndices;
        }
    }
}
