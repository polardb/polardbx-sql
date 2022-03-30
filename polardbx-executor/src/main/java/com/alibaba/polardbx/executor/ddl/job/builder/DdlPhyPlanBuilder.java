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

package com.alibaba.polardbx.executor.ddl.job.builder;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.converter.DdlJobDataConverter;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.meta.delegate.TableInfoManagerDelegate;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.metadb.table.TablesExtRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceTableNameWithQuestionMarkVisitor;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DdlPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.sharding.DataNodeChooser;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.model.TargetDB;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SequenceBean;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Build physical plan for ddl node
 *
 * @since 2021/07
 */
public abstract class DdlPhyPlanBuilder {

    protected DDL relDdl;

    protected DdlPreparedData ddlPreparedData;

    protected ExecutionContext executionContext;
    protected OptimizerContext optimizerContext;

    protected Map<Integer, ParameterContext> params;
    protected List<Integer> paramIndex = new ArrayList<>();

    protected SqlNode sqlTemplate;
    protected SqlNode originSqlTemplate;

    protected SequenceBean sequenceBean;

    protected TableRule tableRule;
    protected PartitionInfo partitionInfo;
    protected Map<String, List<List<String>>> tableTopology;
    protected List<PhyDdlTableOperation> physicalPlans;

    protected boolean tableRuleFromMeta = false;

    /**
     * A flag to identify whether physical plan has been built, to avoid build multiple times.
     */
    protected boolean built = false;

    public DdlPhyPlanBuilder(@Deprecated DDL ddl, DdlPreparedData preparedData, ExecutionContext executionContext) {
        this.relDdl = ddl;
        this.ddlPreparedData = preparedData;
        this.executionContext = executionContext;
        this.optimizerContext = OptimizerContext.getContext(ddlPreparedData.getSchemaName());
        this.params = executionContext.getParams() == null ? null : executionContext.getParams().getCurrentParameter();
    }

    /**
     * // TODO(moyi) merge build in genPhysicalPlan which could simplify Builder usage
     * Build all data structures.
     */
    public DdlPhyPlanBuilder build() {
        if (built) {
            return this;
        }
        buildTableRuleAndTopology();
        buildPhysicalPlans();
        built = true;
        return this;
    }

    protected abstract void buildTableRuleAndTopology();

    protected abstract void buildPhysicalPlans();

    /**
     * Generate physical plan data
     *
     * @return physical plan
     */
    public PhysicalPlanData genPhysicalPlanData(boolean autoPartition) {
        Objects.requireNonNull(tableTopology);
        Objects.requireNonNull(physicalPlans);
        if (!built) {
            throw new AssertionError("DdlPhyPlanBuilder::build has not been called!");
        }

        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        return DdlJobDataConverter.convertToPhysicalPlanData(tableTopology, physicalPlans, false, autoPartition);
    }

    public PhysicalPlanData genPhysicalPlanData() {
        return genPhysicalPlanData(false);
    }

    protected void buildExistingTableRule(String tableName) {
        this.tableRule = optimizerContext.getRuleManager().getTddlRule().getTable(tableName);
        if (this.tableRule == null) {
            TablesExtRecord tablesExtRecord = new TableInfoManagerDelegate<TablesExtRecord>(new TableInfoManager()) {
                @Override
                protected TablesExtRecord invoke() {
                    return this.tableInfoManager
                        .queryTableExt(ddlPreparedData.getSchemaName(), ddlPreparedData.getTableName(), false);
                }
            }.execute();
            this.tableRule = DdlJobDataConverter.buildTableRule(tablesExtRecord);
            this.tableRuleFromMeta = true;
        }
        setPartitionForTableRule();
    }

    protected void setPartitionForTableRule() {
        if (tableRule != null && !PlannerUtils.isSingleTable(tableRule) && !tableRule.isBroadcast()) {
            this.relDdl.setPartition(true);
        }
    }

    protected void buildNewTableTopology(String schemaName, String tableName) {
        List<List<TargetDB>> targetDBs = DataNodeChooser.shardCreateTable(schemaName, tableName, relDdl, tableRule);
        tableTopology = convertTargetDBs(schemaName, targetDBs);
    }

    protected void buildChangedTableTopology(String schemaName, String tableName) {
        List<List<TargetDB>> targetDBs;
        if (tableRuleFromMeta) {
            targetDBs = DataNodeChooser.shardCreateTable(schemaName, tableName, relDdl, tableRule);
        } else {
            targetDBs = DataNodeChooser.shardChangeTable(schemaName, tableName, executionContext);
        }
        tableTopology = convertTargetDBs(schemaName, targetDBs);
    }

    protected void buildGsiTableTopology(String schemaName, String tableName) {
        List<List<TargetDB>> targetDBs = DataNodeChooser.shardGsi(schemaName, tableName, executionContext);
        tableTopology = convertTargetDBs(schemaName, targetDBs);
    }

    private Map<String, List<List<String>>> convertTargetDBs(String schemaName, List<List<TargetDB>> targetDBs) {
        final Set<String> groupIntersection = PlannerUtils.getGroupIntersection(targetDBs);
        targetDBs = PlannerUtils.filterGroup(targetDBs, groupIntersection, schemaName);

        final List<Group> groups = optimizerContext.getMatrix().getGroups();
        targetDBs = PlannerUtils.fillGroup(targetDBs, groups, tableRule);

        return PlannerUtils.convertTargetDB(targetDBs, schemaName);
    }

    protected void buildPhysicalPlans(String tableName) {
        initParameterIndex();

        SqlNode newTableName = relDdl.getNewTableName();

        List<PhyDdlTableOperation> physicalPlans = new ArrayList<>();
        for (Map.Entry<String, List<List<String>>> t : tableTopology.entrySet()) {
            String group = t.getKey();
            List<List<String>> tableNames = t.getValue();
            for (List<String> subTableNames : tableNames) {
                PhyDdlTableOperation phyDdlTable =
                    PhyDdlTableOperation.create(ddlPreparedData.getSchemaName(), tableName, executionContext);
                phyDdlTable.setDbIndex(group);
                phyDdlTable.setLogicalTableName(tableName);
                if (newTableName != null) {
                    phyDdlTable.setRenameLogicalTableName(((SqlIdentifier) newTableName).getLastName());
                }
                phyDdlTable.setTableNames(ImmutableList.of(subTableNames));
                phyDdlTable.setKind(sqlTemplate.getKind());
                Pair<String, Map<Integer, ParameterContext>> sqlAndParam = buildSqlAndParam(subTableNames);
                phyDdlTable.setSqlTemplate(sqlAndParam.getKey());
                phyDdlTable.setNativeSqlNode(sqlTemplate);
                phyDdlTable.setDbType(DbType.MYSQL);
                phyDdlTable.setParam(sqlAndParam.getValue());
                phyDdlTable.setTableRule(tableRule);
                phyDdlTable.setPartitioned(
                    tableRule != null && !PlannerUtils.isSingleTable(tableRule) && !tableRule.isBroadcast());
                phyDdlTable.setPartitionInfo(partitionInfo);
                phyDdlTable.setSequence(sequenceBean);
                phyDdlTable.setHint(ddlPreparedData.isWithHint());
                phyDdlTable.setSchemaName(ddlPreparedData.getSchemaName());
                physicalPlans.add(phyDdlTable);
            }
        }
        this.physicalPlans = physicalPlans;
    }

    protected void initParameterIndex() {
        if (tableTopology != null) {
            if (tableTopology.keySet().size() == 0) {
                return;
            }
            final String next = tableTopology.keySet().iterator().next();
            final List<List<String>> listSplit = tableTopology.get(next);
            if (listSplit == null || listSplit.size() == 0) {
                return;
            }
            final List<String> paramCount = listSplit.get(0);
            for (int i = 0; paramCount != null && i < paramCount.size(); i++) {
                paramIndex.add(Integer.valueOf(-1));
            }
        }
    }

    protected void buildSqlTemplate() {
        ReplaceTableNameWithQuestionMarkVisitor visitor =
            new ReplaceTableNameWithQuestionMarkVisitor(ddlPreparedData.getSchemaName(), executionContext);
        this.sqlTemplate = this.relDdl.sqlNode.accept(visitor);
        this.originSqlTemplate = this.sqlTemplate;
    }

    private Pair<String, Map<Integer, ParameterContext>> buildSqlAndParam(List<String> tableNames) {
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(tableNames));
        String sql = RelUtils.toNativeSql(sqlTemplate, DbType.MYSQL);
        Map<Integer, ParameterContext> params = buildParams(tableNames);
        return new Pair<>(sql, params);
    }

    private Map<Integer, ParameterContext> buildParams(List<String> tableNames) {
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(tableNames));
        return PlannerUtils.buildParam(tableNames, this.params, paramIndex);
    }

    protected SqlNode generateDbPartition(TableMeta tableMeta, String indexColName) {
        final ColumnMeta columnMeta =
            tableMeta.getPhysicalColumns().stream().filter(col -> col.getName().equalsIgnoreCase(indexColName))
                .findFirst().orElseThrow(() -> new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                "Unknown GSI column '" + indexColName + "'"));
        final String typeName = columnMeta.getField().getDataType().getStringSqlType().toLowerCase();
        return SqlValidatorImpl.assignAutoPartition(new SqlIdentifier(indexColName, SqlParserPos.ZERO), typeName);
    }

    public TableRule getTableRule() {
        return tableRule;
    }

    public void setPartitionInfo(PartitionInfo partitionInfo) {
        this.partitionInfo = partitionInfo;
    }

    public PartitionInfo getPartitionInfo() {
        return partitionInfo;
    }

    public Map<String, List<List<String>>> getTableTopology() {
        return tableTopology;
    }

    public List<PhyDdlTableOperation> getPhysicalPlans() {
        return physicalPlans;
    }

}
