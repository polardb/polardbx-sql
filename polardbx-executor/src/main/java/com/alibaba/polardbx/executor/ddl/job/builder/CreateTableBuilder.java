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

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateTablePreparedData;
import com.alibaba.polardbx.optimizer.partition.LocalPartitionDefinitionInfo;
import com.alibaba.polardbx.optimizer.index.TableRuleBuilder;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.TddlRule;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlDdlNodes;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartitionByRange;
import org.apache.calcite.util.Pair;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class CreateTableBuilder extends DdlPhyPlanBuilder {

    protected final CreateTablePreparedData preparedData;

    public CreateTableBuilder(DDL ddl, CreateTablePreparedData preparedData, ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
        this.preparedData = preparedData;
    }

    @Override
    public void buildTableRuleAndTopology() {
        buildNewTableRule();
        buildNewTableTopology(preparedData.getSchemaName(), preparedData.getTableName());
    }

    @Override
    public void buildPhysicalPlans() {
        buildSqlTemplate();
        buildPhysicalPlans(preparedData.getTableName());
    }

    protected void buildNewTableRule() {
        if (tableRule != null) {
            return;
        }

        TddlRule tddlRule = null;
        try {
            if (preparedData.isShadow()) {
                tddlRule = optimizerContext.getRuleManager().getTddlRule();
                if (tddlRule != null) {
                    tddlRule.prepareForShadowTable(preparedData.getTableName());
                }
            }

            TableRule tableRule;
            if (preparedData.isBroadcast()) {
                if (preparedData.isSharding()) {
                    throw new IllegalArgumentException("Broadcast and sharding are exclusive");
                }
                tableRule = TableRuleBuilder.buildBroadcastTableRule(
                    preparedData.getTableName(),
                    preparedData.getTableMeta(),
                    optimizerContext,
                    executionContext.isRandomPhyTableEnabled()
                );
            } else if (preparedData.isSharding()) {
                boolean supportSingleDbMultiTbs = checkIfSupportSingleDbMultiTbs();
                if (!supportSingleDbMultiTbs) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        "A single database shard with multiple table shards is not allowed in PolarDB-X");
                }
                tableRule = TableRuleBuilder.buildShardingTableRule(
                    preparedData.getTableName(),
                    preparedData.getTableMeta(),
                    preparedData.getDbPartitionBy(),
                    preparedData.getDbPartitions(),
                    preparedData.getTbPartitionBy(),
                    preparedData.getTbPartitions(),
                    optimizerContext,
                    executionContext
                );
            } else {
                tableRule = TableRuleBuilder.buildSingleTableRule(
                    preparedData.getSchemaName(),
                    preparedData.getTableName(),
                    preparedData.getLocality(),
                    optimizerContext,
                    executionContext.isRandomPhyTableEnabled()
                );
            }

            this.tableRule = tableRule;
            setPartitionForTableRule();

            preparedData.setTableRule(tableRule);

            if (ConfigDataMode.isFastMock()) {
                tableRule.init();
            }
        } finally {
            if (tddlRule != null) {
                tddlRule.cleanupForShadowTable(preparedData.getTableName());
            }
        }
    }

    private boolean checkIfSupportSingleDbMultiTbs() {
        int dbCount = 1, tbCount = 1;

        if (preparedData.getDbPartitions() != null) {
            dbCount = ((SqlLiteral) preparedData.getDbPartitions()).intValue(false);
        }

        if (preparedData.getTbPartitions() != null) {
            tbCount = ((SqlLiteral) preparedData.getTbPartitions()).intValue(false);
        }

        boolean singleDb = preparedData.getDbPartitionBy() == null
            || (preparedData.getDbPartitions() != null && dbCount == 1);
        boolean multiTbs = preparedData.getTbPartitionBy() != null && tbCount > 1;

        return ConfigDataMode.isSupportSingleDbMultiTbs() || !(singleDb && multiTbs);
    }

    @Override
    protected void buildSqlTemplate() {
        super.buildSqlTemplate();

        final SqlCreateTable sqlTemplate = (SqlCreateTable) this.sqlTemplate;
        Engine engine = sqlTemplate.getEngine();

        this.sqlTemplate = SqlDdlNodes.createTable(
            sqlTemplate.getParserPosition(),
            sqlTemplate.isReplace(),
            sqlTemplate.isIfNotExists(),
            sqlTemplate.getName(),
            null,
            sqlTemplate.getColumnList(),
            sqlTemplate.getQuery(),
            null,
            null,
            null,
            null,
            sqlTemplate.rewrite().toString(),
            false,
            sqlTemplate.getAutoIncrement(),
            null,
            sqlTemplate.getLocalPartition(),
            null,
            sqlTemplate.getLocalPartitionSuffix()
        );

        ((SqlCreateTable) this.sqlTemplate).setEngine(engine);
        ((SqlCreateTable) this.sqlTemplate).setTemporary(sqlTemplate.isTemporary());
        validatePartitionColumnInUkForLocalPartition(sqlTemplate);

        sequenceBean = sqlTemplate.getAutoIncrement();
    }

    @Override
    public PhysicalPlanData genPhysicalPlanData(boolean autoPartition) {
        PhysicalPlanData data = super.genPhysicalPlanData(autoPartition);
        data.setLocalityDesc(preparedData.getLocality());
        return data;
    }

    private void validatePartitionColumnInUkForLocalPartition(SqlCreateTable sqlCreateTable){
        List<SqlIndexDefinition> allUniqueKeys = new ArrayList<>();
        SqlNode localPartition = sqlCreateTable.getLocalPartition();
        if(localPartition==null){
            return;
        }

        if(sqlCreateTable.getPrimaryKey()!=null){
            allUniqueKeys.add(sqlCreateTable.getPrimaryKey());
        }
        if(CollectionUtils.isNotEmpty(sqlCreateTable.getUniqueKeys())){
            for(Pair<SqlIdentifier, SqlIndexDefinition> pair: sqlCreateTable.getUniqueKeys()){
                allUniqueKeys.add(pair.getValue());
            }
        }
        if(CollectionUtils.isNotEmpty(sqlCreateTable.getGlobalUniqueKeys())){
            for(Pair<SqlIdentifier, SqlIndexDefinition> pair: sqlCreateTable.getGlobalUniqueKeys()){
                allUniqueKeys.add(pair.getValue());
            }
        }
        if(CollectionUtils.isNotEmpty(sqlCreateTable.getClusteredUniqueKeys())){
            for(Pair<SqlIdentifier, SqlIndexDefinition> pair: sqlCreateTable.getClusteredUniqueKeys()){
                allUniqueKeys.add(pair.getValue());
            }
        }

        if(CollectionUtils.isEmpty(allUniqueKeys)){
            return;
        }
        SqlIdentifier column = (SqlIdentifier) ((SqlPartitionByRange) localPartition).getColumns().get(0);
        String localPartitionColumn = column.getLastName().replace("`", "").toLowerCase();

        for(SqlIndexDefinition sqlIndexDefinition: allUniqueKeys){
            List<String> primaryColumnNameList = sqlIndexDefinition.getColumns()
                .stream()
                .map(e->e.getColumnNameStr().replace("`", "").toLowerCase())
                .collect(Collectors.toList());

            if(!primaryColumnNameList.contains(localPartitionColumn)){
                throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED_INDEX_TABLE_DEFINITION,
                    String.format("Primary/Unique Key must contain local partition column: %s", localPartitionColumn));
            }
        }
    }
}
