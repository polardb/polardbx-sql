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

package com.alibaba.polardbx.executor.ddl.job.builder.gsi;

import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLIndex;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.google.common.collect.Maps;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.builder.CreatePartitionTableBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionTableType;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlAddIndex;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlCreateIndex;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexColumnName;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class CreatePartitionGlobalIndexBuilder extends CreateGlobalIndexBuilder {

    public CreatePartitionGlobalIndexBuilder(@Deprecated DDL ddl, CreateGlobalIndexPreparedData gsiPreparedData,
                                             ExecutionContext executionContext) {
        super(ddl, gsiPreparedData, executionContext);
    }

    @Override
    public CreateGlobalIndexBuilder build() {
        buildTablePartitionInfoAndTopology();
        buildPhysicalPlans();
        built = true;
        return this;
    }

    private void buildTablePartitionInfoAndTopology() {
        CreateTablePreparedData indexTablePreparedData = gsiPreparedData.getIndexTablePreparedData();

        SqlDdl sqlDdl = (SqlDdl) relDdl.sqlNode;
        if (sqlDdl.getKind() == SqlKind.CREATE_TABLE || sqlDdl.getKind() == SqlKind.ALTER_TABLE) {
            refreshShardingInfo(gsiPreparedData.getIndexDefinition(), indexTablePreparedData);
        } else if (sqlDdl.getKind() == SqlKind.CREATE_INDEX) {
            refreshShardingInfo(gsiPreparedData.getSqlCreateIndex(), indexTablePreparedData);
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_UNSUPPORTED,
                "DDL Kind '" + sqlDdl.getKind() + "' for GSI creation");
        }

        indexTableBuilder = new CreatePartitionTableBuilder(relDdl, indexTablePreparedData, executionContext,
            PartitionTableType.GSI_TABLE);
        indexTableBuilder.buildTableRuleAndTopology();
        this.gsiPreparedData.setIndexPartitionInfo(indexTableBuilder.getPartitionInfo());
        this.partitionInfo = indexTableBuilder.getPartitionInfo();
        this.tableTopology = indexTableBuilder.getTableTopology();
    }

    @Override
    protected void buildPhysicalPlans() {
        buildSqlTemplate();
        buildPhysicalPlans(gsiPreparedData.getIndexTableName());
    }

    @Override
    protected SqlNode buildIndexTableDefinition(final SqlAlterTable sqlAlterTable, final boolean forceAllowGsi) {
        final SqlIndexDefinition indexDef = ((SqlAddIndex) sqlAlterTable.getAlters().get(0)).getIndexDef();

        /**
         * build global secondary index table
         */
        final List<SqlIndexColumnName> covering =
            indexDef.getCovering() == null ? new ArrayList<>() : indexDef.getCovering();
        final Map<String, SqlIndexColumnName> coveringMap =
            Maps.uniqueIndex(covering, SqlIndexColumnName::getColumnNameStr);
        final Map<String, SqlIndexColumnName> indexColumnMap = new LinkedHashMap<>(indexDef.getColumns().size());
        indexDef.getColumns().forEach(cn -> indexColumnMap.putIfAbsent(cn.getColumnNameStr(), cn));

        /**
         * for CREATE TABLE with GSI, primaryRule is null, cause at this point
         * primary table has not been created
         */
        final PartitionInfo primaryPartitionInfo = gsiPreparedData.getPrimaryPartitionInfo();
        /**
         * check if index columns contains all sharding columns
         */
        final PartitionInfo indexPartitionInfo = gsiPreparedData.getIndexPartitionInfo();

        final Set<String> indexColumnSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        indexColumnSet.addAll(indexColumnMap.keySet());
        if (!containsAllShardingColumns(indexColumnSet, indexPartitionInfo)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_INDEX_AND_SHARDING_COLUMNS_NOT_MATCH);
        }

        /**
         * check single/broadcast table
         */
        if (null != primaryPartitionInfo) {
            if (forceAllowGsi == false && (primaryPartitionInfo.isBroadcastTable() || primaryPartitionInfo
                .isSingleTable())) {
                throw new TddlRuntimeException(
                    ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED_PRIMARY_TABLE_DEFINITION,
                    "Does not support create Global Secondary Index on single or broadcast table");
            }
        }

        /**
         * copy table structure from main table
         */
        final MySqlCreateTableStatement astCreateIndexTable = (MySqlCreateTableStatement) SQLUtils
            .parseStatements(indexDef.getPrimaryTableDefinition(), JdbcConstants.MYSQL).get(0).clone();

        assert primaryPartitionInfo != null;
        final Set<String> shardingColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        shardingColumns.addAll(primaryPartitionInfo.getPartitionColumns());

        return createIndexTable(sqlAlterTable,
            indexColumnMap,
            coveringMap,
            astCreateIndexTable,
            shardingColumns,
            relDdl,
            gsiPreparedData.getSchemaName(),
            executionContext);
    }

    @Override
    protected SqlNode buildIndexTableDefinition(final SqlCreateIndex sqlCreateIndex) {
        /**
         * build global secondary index table
         */
        final Map<String, SqlIndexColumnName> coveringMap =
            null == sqlCreateIndex.getCovering() ? new HashMap<>() : Maps.uniqueIndex(sqlCreateIndex.getCovering(),
                SqlIndexColumnName::getColumnNameStr);
        final Map<String, SqlIndexColumnName> indexColumnMap = new LinkedHashMap<>(sqlCreateIndex.getColumns().size());
        sqlCreateIndex.getColumns().forEach(cn -> indexColumnMap.putIfAbsent(cn.getColumnNameStr(), cn));

        /**
         * check if index columns contains all sharding columns
         */
        final SqlIdentifier indexName = sqlCreateIndex.getIndexName();
        final PartitionInfo primaryPartitionInfo = gsiPreparedData.getPrimaryPartitionInfo();
        final PartitionInfo indexPartitionInfo = gsiPreparedData.getIndexPartitionInfo();

        final Set<String> indexColumnSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        indexColumnSet.addAll(indexColumnMap.keySet());
        if (!containsAllShardingColumns(indexColumnSet, indexPartitionInfo)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_INDEX_AND_SHARDING_COLUMNS_NOT_MATCH);
        }

        /**
         * check single/broadcast table
         */
        if (null != primaryPartitionInfo) {
            if (primaryPartitionInfo.isBroadcastTable() || primaryPartitionInfo.isSingleTable()) {
                throw new TddlRuntimeException(
                    ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED_PRIMARY_TABLE_DEFINITION,
                    "Does not support create Global Secondary Index on single or broadcast table");
            }
        }

        /**
         * copy table structure from main table
         */
        final MySqlCreateTableStatement indexTableStmt =
            (MySqlCreateTableStatement) SQLUtils.parseStatements(sqlCreateIndex.getPrimaryTableDefinition(),
                JdbcConstants.MYSQL)
                .get(0)
                .clone();

        final boolean unique = sqlCreateIndex.getConstraintType() != null
            && sqlCreateIndex.getConstraintType() == SqlCreateIndex.SqlIndexConstraintType.UNIQUE;
        assert primaryPartitionInfo != null;
        final Set<String> shardingColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        shardingColumns.addAll(primaryPartitionInfo.getPartitionColumns());
        final boolean isClusteredIndex = sqlCreateIndex.createClusteredIndex();

        if (isClusteredIndex) {
            return createClusteredIndexTable(indexName,
                indexColumnMap,
                indexTableStmt,
                unique,
                sqlCreateIndex.getOptions(),
                relDdl,
                null,
                gsiPreparedData.getSchemaName(),
                executionContext
            );
        } else {
            return createIndexTable(indexName,
                indexColumnMap,
                coveringMap,
                indexTableStmt,
                unique,
                sqlCreateIndex.getOptions(),
                shardingColumns,
                relDdl,
                null,
                gsiPreparedData.getSchemaName(),
                executionContext
            );
        }
    }

    protected boolean containsAllShardingColumns(Set<String> indexColumnSet, PartitionInfo indexPartitionInfo) {
        boolean result = false;

        if (null != indexPartitionInfo) {
            List<String> shardColumns = indexPartitionInfo.getPartitionColumns();
            if (null != shardColumns) {
                result = indexColumnSet.containsAll(shardColumns);
            }
        }

        return result;
    }

    @Override
    protected void refreshShardingInfo(SqlCreateIndex sqlCreateIndex, CreateTablePreparedData indexTablePreparedData) {

        sqlCreateIndex.setTargetTable(sqlCreateIndex.getIndexName());
        gsiPreparedData.setSqlCreateIndex(sqlCreateIndex);

        if (sqlCreateIndex.getPartitioning() != null) {
            indexTablePreparedData.setSharding(true);
            gsiPreparedData.getIndexTablePreparedData().setSharding(true);
        }

        indexTablePreparedData.setTableName(gsiPreparedData.getIndexTableName());
        indexTablePreparedData.setDbPartitionBy(null);
        indexTablePreparedData.setDbPartitions(sqlCreateIndex.getDbPartitions());
        indexTablePreparedData.setTbPartitionBy(sqlCreateIndex.getTbPartitionBy());
        indexTablePreparedData.setTbPartitions(sqlCreateIndex.getTbPartitions());
        indexTablePreparedData.setPartitioning(sqlCreateIndex.getPartitioning());
    }

    /**
     * Remove all local index on GSI for partition-table
     */
    @Override
    protected void removeIndexOnGsi(Set<String> fullColumn,
                                    Iterator<SQLTableElement> iterator,
                                    SQLIndex key) {
        iterator.remove();
    }

    @Override
    protected <T> void removePkOnGsi(Iterator<T> iterator) {
        iterator.remove();
    }

    @Override
    protected void genSimpleIndexForUGSI(MySqlCreateTableStatement indexTableStmt,
                                         List<SQLSelectOrderByItem> pkList) {
        final MySqlTableIndex index = new MySqlTableIndex();
        index.getIndexDefinition().setIndex(true);
        index.getIndexDefinition().setName(new SQLIdentifierExpr(TddlConstants.UGSI_PK_INDEX_NAME));
        index.getIndexDefinition().getColumns().addAll(pkList);
        index.getIndexDefinition().getOptions().setIndexType("BTREE");
        index.getIndexDefinition().setParent(index);
        index.setParent(indexTableStmt);
        indexTableStmt.getTableElementList().add(index);
    }
}
