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

package com.alibaba.polardbx.optimizer.core.rel.ddl;

import com.alibaba.polardbx.common.ArchiveMode;
import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionByRange;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateLocalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateTableWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.parse.TableMetaParser;
import com.alibaba.polardbx.optimizer.partition.LocalPartitionDefinitionInfo;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.rel.ddl.CreateTable;
import org.apache.calcite.sql.SqlBinaryStringLiteral;
import org.apache.calcite.sql.SqlColumnDeclaration;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlPartitionByRange;
import org.apache.calcite.util.Pair;

import java.util.Map;
import java.util.TreeMap;

public class LogicalCreateTable extends LogicalTableOperation {

    private SqlCreateTable sqlCreateTable;

    private String createTableSqlForLike;

    private CreateTablePreparedData createTablePreparedData;
    private CreateTableWithGsiPreparedData createTableWithGsiPreparedData;

    private LogicalCreateTable(CreateTable createTable) {
        super(createTable);
        this.sqlCreateTable = (SqlCreateTable) relDdl.sqlNode;
    }

    public static LogicalCreateTable create(CreateTable createTable) {
        return new LogicalCreateTable(createTable);
    }

    public boolean isWithGsi() {
        return createTableWithGsiPreparedData != null
            && createTableWithGsiPreparedData.hasGsi()
            && !Engine.isFileStore(sqlCreateTable.getEngine()); // no gsi for oss table
    }

    public boolean isBroadCastTable() {
        return sqlCreateTable.isBroadCast();
    }

    public boolean isPartitionTable() {
        return sqlCreateTable.getSqlPartition() != null;
    }

    public void setCreateTableSqlForLike(String createTableSqlForLike) {
        this.createTableSqlForLike = createTableSqlForLike;
    }

    public String getCreateTableSqlForLike() {
        return this.createTableSqlForLike;
    }

    public CreateTablePreparedData getCreateTablePreparedData() {
        return createTablePreparedData;
    }

    public CreateTableWithGsiPreparedData getCreateTableWithGsiPreparedData() {
        return createTableWithGsiPreparedData;
    }

    public void prepareData(ExecutionContext executionContext) {
        // A normal logical table or a primary table with GSIs.
        createTablePreparedData = preparePrimaryData(executionContext);

        final boolean isAutoPartition = sqlCreateTable.isAutoPartition();

        if (Engine.isFileStore(sqlCreateTable.getEngine())) {
            // no gsi for oss table
            return;
        }

        if (sqlCreateTable.createGsi()) {
            String primaryTableName = createTablePreparedData.getTableName();
            String primaryTableDefinition = sqlCreateTable.rewriteForGsi().toString();

            createTablePreparedData.setTableDefinition(primaryTableDefinition);

            createTableWithGsiPreparedData = new CreateTableWithGsiPreparedData();
            createTableWithGsiPreparedData.setPrimaryTablePreparedData(createTablePreparedData);

            if (sqlCreateTable.getGlobalKeys() != null) {
                for (Pair<SqlIdentifier, SqlIndexDefinition> gsi : sqlCreateTable.getGlobalKeys()) {
                    CreateGlobalIndexPreparedData indexTablePreparedData =
                        prepareGsiData(primaryTableName, primaryTableDefinition, gsi, false);
                    createTableWithGsiPreparedData.addIndexTablePreparedData(indexTablePreparedData);

                    if (isAutoPartition) {
                        createTableWithGsiPreparedData.addLocalIndex(
                            prepareAutoPartitionLocalIndex(primaryTableName, gsi));
                    }
                }
            }

            if (sqlCreateTable.getGlobalUniqueKeys() != null) {
                for (Pair<SqlIdentifier, SqlIndexDefinition> gusi : sqlCreateTable.getGlobalUniqueKeys()) {
                    CreateGlobalIndexPreparedData uniqueIndexTablePreparedData =
                        prepareGsiData(primaryTableName, primaryTableDefinition, gusi, true);
                    createTableWithGsiPreparedData.addIndexTablePreparedData(uniqueIndexTablePreparedData);
                }
            }

            if (sqlCreateTable.getClusteredKeys() != null) {
                for (Pair<SqlIdentifier, SqlIndexDefinition> gsi : sqlCreateTable.getClusteredKeys()) {
                    CreateGlobalIndexPreparedData indexTablePreparedData =
                        prepareGsiData(primaryTableName, primaryTableDefinition, gsi, false);
                    createTableWithGsiPreparedData.addIndexTablePreparedData(indexTablePreparedData);

                    if (isAutoPartition) {
                        createTableWithGsiPreparedData.addLocalIndex(
                            prepareAutoPartitionLocalIndex(primaryTableName, gsi));
                    }
                }
            }

            if (sqlCreateTable.getClusteredUniqueKeys() != null) {
                for (Pair<SqlIdentifier, SqlIndexDefinition> gusi : sqlCreateTable.getClusteredUniqueKeys()) {
                    CreateGlobalIndexPreparedData uniqueIndexTablePreparedData =
                        prepareGsiData(primaryTableName, primaryTableDefinition, gusi, true);
                    createTableWithGsiPreparedData.addIndexTablePreparedData(uniqueIndexTablePreparedData);
                }
            }
        }
    }

    private CreateTablePreparedData preparePrimaryData(ExecutionContext executionContext) {
        if (sqlCreateTable.getLikeTableName() != null) {
            prepareCreateTableLikeData();
        }

        String tableName = ((SqlIdentifier) sqlCreateTable.getName()).getLastName();
        final TableMeta tableMeta = TableMetaParser.parse(tableName, sqlCreateTable);
        tableMeta.setSchemaName(schemaName);

        LocalPartitionDefinitionInfo localPartitionDefinitionInfo = LocalPartitionDefinitionInfo.create(
                schemaName,
                tableMeta.getTableName(),
                (SqlPartitionByRange) sqlCreateTable.getLocalPartition()
            );
        if(localPartitionDefinitionInfo!=null){
            SQLPartitionByRange sqlPartitionByRange = LocalPartitionDefinitionInfo.generateLocalPartitionStmtForCreate(
                localPartitionDefinitionInfo,
                localPartitionDefinitionInfo.evalPivotDate(executionContext));
            sqlCreateTable.setLocalPartitionSuffix(sqlPartitionByRange);
        }

        CreateTablePreparedData res = prepareCreateTableData(tableMeta,
            sqlCreateTable.isShadow(),
            sqlCreateTable.isAutoPartition(),
            sqlCreateTable.isBroadCast(),
            sqlCreateTable.getDbpartitionBy(),
            sqlCreateTable.getDbpartitions(),
            sqlCreateTable.getTbpartitionBy(),
            sqlCreateTable.getTbpartitions(),
            sqlCreateTable.getSqlPartition(),
            localPartitionDefinitionInfo,
            sqlCreateTable.getTableGroupName(),
            ((CreateTable) relDdl).getPartBoundExprInfo());

        boolean hasTimestampColumnDefault = false;
        for (Pair<SqlIdentifier, SqlColumnDeclaration> colDef : GeneralUtil.emptyIfNull(sqlCreateTable.getColDefs())) {
            if (isTimestampColumnWithDefault(colDef.getValue())) {
                hasTimestampColumnDefault = true;
                break;
            }
        }
        res.setTimestampColumnDefault(hasTimestampColumnDefault);

        Map<String, String> binaryColumnDefaultValues = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Pair<SqlIdentifier, SqlColumnDeclaration> colDef : GeneralUtil.emptyIfNull(sqlCreateTable.getColDefs())) {
            if (colDef.getValue().getDefaultVal() instanceof SqlBinaryStringLiteral) {
                String columnName = colDef.getKey().getLastName();
                String hexValue =
                    ((SqlBinaryStringLiteral) colDef.getValue().getDefaultVal()).getBitString().toHexString();
                binaryColumnDefaultValues.put(columnName, hexValue);
            }
        }
        res.setBinaryColumnDefaultValues(binaryColumnDefaultValues);

        // create table with locality
        if (TStringUtil.isNotBlank(sqlCreateTable.getLocality())) {
            LocalityDesc desc = LocalityDesc.parse(sqlCreateTable.getLocality());
            if (!desc.isEmpty()) {
                res.setLocality(desc);
            }
        }

        if (sqlCreateTable.getLoadTableName() != null) {
            res.setLoadTableName(sqlCreateTable.getLoadTableName());
            if (sqlCreateTable.getLoadTableSchema() != null) {
                res.setLoadTableSchema(sqlCreateTable.getLoadTableSchema());
            } else {
                res.setLoadTableSchema(schemaName);
            }
        }
        if (sqlCreateTable.getArchiveMode() == ArchiveMode.TTL) {
            res.setArchiveTableName(sqlCreateTable.getLoadTableName());
        }

        return res;
    }

    private void prepareCreateTableLikeData() {
        // For `create table like xx` statement, we create a new "Create Table" AST for the target table
        // based on the LIKE table, then execute it as normal flow.
        SqlCreateTable createTableAst = (SqlCreateTable) new FastsqlParser()
            .parse(createTableSqlForLike, PlannerContext.getPlannerContext(this).getExecutionContext()).get(0);

        SqlIdentifier tableName = (SqlIdentifier) getTableNameNode();

        createTableAst.setTargetTable(tableName);

        if (createTableAst.getAutoIncrement() != null) {
            createTableAst.getAutoIncrement().setStart(null);
        }

        createTableAst.setMappingRules(null);
        createTableAst.setGlobalKeys(null);
        createTableAst.setGlobalUniqueKeys(null);

        // set engine if there is engine option in user sql
        Engine engine;
        if ((engine = this.sqlCreateTable.getEngine()) != null) {
            createTableAst.setEngine(engine);
        }
        ArchiveMode archiveMode;
        if ((archiveMode = this.sqlCreateTable.getArchiveMode()) != null) {
            createTableAst.setArchiveMode(archiveMode);
        }
        if (this.sqlCreateTable.shouldLoad() || this.sqlCreateTable.shouldBind()) {
            if (sqlCreateTable.getLikeTableName() instanceof SqlIdentifier) {
                if (((SqlIdentifier) sqlCreateTable.getLikeTableName()).names.size() == 2) {
                    createTableAst.setLoadTableSchema(
                        ((SqlIdentifier) sqlCreateTable.getLikeTableName()).getComponent(0).getLastName());
                    createTableAst.setLoadTableName(
                        ((SqlIdentifier) sqlCreateTable.getLikeTableName()).getComponent(1).getLastName());
                } else if (((SqlIdentifier) sqlCreateTable.getLikeTableName()).names.size() == 1) {
                    createTableAst.setLoadTableName(
                        ((SqlIdentifier) sqlCreateTable.getLikeTableName()).getComponent(0).getLastName());
                }
            }
        }

        // Replace the original AST
        this.sqlCreateTable = createTableAst;
        this.relDdl.sqlNode = createTableAst;
    }

    private CreateGlobalIndexPreparedData prepareGsiData(String primaryTableName, String primaryTableDefinition,
                                                         Pair<SqlIdentifier, SqlIndexDefinition> gsi,
                                                         boolean isUnique) {

        String indexTableName = RelUtils.lastStringValue(gsi.getKey());
        SqlIndexDefinition indexDef = gsi.getValue();

        /**
         * If the primary table is auto-partition, the global index of this table
         * is also auto-partitioned
         */
        boolean autoPartitionGsi = createTablePreparedData.isAutoPartition();

        CreateGlobalIndexPreparedData preparedData =
            prepareCreateGlobalIndexData(primaryTableName,
                primaryTableDefinition,
                indexTableName,
                createTablePreparedData.getTableMeta(),
                createTablePreparedData.isShadow(),
                autoPartitionGsi,
                false,
                indexDef.getDbPartitionBy(),
                indexDef.getDbPartitions(),
                indexDef.getTbPartitionBy(),
                indexDef.getTbPartitions(),
                indexDef.getPartitioning(),
                createTablePreparedData.getLocalPartitionDefinitionInfo(),
                isUnique,
                indexDef.isClustered(),
                null,
                ((CreateTable) relDdl).getPartBoundExprInfo());

        preparedData.setIndexDefinition(indexDef);
        if (indexDef.getOptions() != null) {
            final String indexComment = indexDef.getOptions()
                .stream()
                .filter(option -> null != option.getComment())
                .findFirst()
                .map(option -> RelUtils.stringValue(option.getComment()))
                .orElse("");
            preparedData.setIndexComment(indexComment);
        }
        if (indexDef.getIndexType() != null) {
            preparedData.setIndexType(null == indexDef.getIndexType() ? null : indexDef.getIndexType().name());
        }

        return preparedData;
    }

    private CreateLocalIndexPreparedData prepareAutoPartitionLocalIndex(String tableName,
                                                                        Pair<SqlIdentifier, SqlIndexDefinition> gsi) {
        String indexName = RelUtils.lastStringValue(gsi.getKey());
        boolean isOnClustered = gsi.getValue().isClustered();

        return prepareCreateLocalIndexData(tableName, indexName, isOnClustered, true);
    }

}
