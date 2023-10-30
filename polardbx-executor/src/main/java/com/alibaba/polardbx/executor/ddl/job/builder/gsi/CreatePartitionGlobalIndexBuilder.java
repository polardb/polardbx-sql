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
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLIndex;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnPrimaryKey;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlUnique;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.executor.ddl.job.builder.CreatePartitionTableBuilder;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.common.PartitionLocation;
import com.alibaba.polardbx.optimizer.partition.common.PartitionTableType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlAddIndex;
import org.apache.calcite.sql.SqlAddUniqueIndex;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlCreateIndex;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexColumnName;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlIndexOption;
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

        PartitionInfo indexPartInfo = indexTableBuilder.getPartitionInfo();
        PartitionInfo primaryPartitionInfo = gsiPreparedData.getPrimaryPartitionInfo();
        if (indexPartInfo.equals(primaryPartitionInfo)
            && primaryPartitionInfo.getTableGroupId() == TableGroupRecord.INVALID_TABLE_GROUP_ID
            && indexPartInfo.getTableGroupId() == TableGroupRecord.INVALID_TABLE_GROUP_ID) {
            physicalLocationAlignWithPrimaryTable(primaryPartitionInfo, indexPartInfo);
            gsiPreparedData.setIndexAlignWithPrimaryTableGroup(true);
        }

        indexTableBuilder.buildTableRuleAndTopology();
        this.gsiPreparedData.setIndexPartitionInfo(indexTableBuilder.getPartitionInfo());
        this.partitionInfo = indexTableBuilder.getPartitionInfo();
        this.tableTopology = indexTableBuilder.getTableTopology();
    }

    private void physicalLocationAlignWithPrimaryTable(PartitionInfo primaryPartitionInfo,
                                                       PartitionInfo indexPartitionInfo) {
        assert primaryPartitionInfo.equals(indexPartitionInfo);
        assert indexPartitionInfo.isGsi();
        List<PartitionSpec> primaryPhyPartitions = primaryPartitionInfo.getPartitionBy().getPhysicalPartitions();
        List<PartitionSpec> indexPhyPartitions = indexPartitionInfo.getPartitionBy().getPhysicalPartitions();
        for (int i = 0; i < primaryPhyPartitions.size(); i++) {
            PartitionLocation primaryLocation = primaryPhyPartitions.get(i).getLocation();
            PartitionLocation indexLocation = indexPhyPartitions.get(i).getLocation();
            indexLocation.setGroupKey(primaryLocation.getGroupKey());
        }
    }

    @Override
    protected void buildPhysicalPlans() {
        buildSqlTemplate();
        buildPhysicalPlans(gsiPreparedData.getIndexTableName());
    }

    private static List<String> getPrimaryKeyNames(MySqlCreateTableStatement astCreateIndexTable) {
        List<String> pks = astCreateIndexTable.getPrimaryKeyNames();
        if (!pks.isEmpty()) {
            return pks;
        }
        // Scan column defines.
        for (SQLTableElement element : astCreateIndexTable.getTableElementList()) {
            if (element instanceof SQLColumnDefinition) {
                final SQLColumnDefinition columnDefinition = (SQLColumnDefinition) element;
                if (null != columnDefinition.getConstraints()) {
                    for (SQLColumnConstraint constraint : columnDefinition.getConstraints()) {
                        if (constraint instanceof SQLColumnPrimaryKey) {
                            // PK found.
                            if (!pks.isEmpty()) {
                                throw new NotSupportException("Unexpected: Multiple pk definitions.");
                            }
                            pks.add(SQLUtils.normalize(columnDefinition.getColumnName()));
                        }
                    }
                }
            }
        }
        return pks;
    }

    @Override
    protected SqlNode buildIndexTableDefinition(final SqlAlterTable sqlAlterTable, final boolean forceAllowGsi) {
        final boolean uniqueIndex = sqlAlterTable.getAlters().get(0) instanceof SqlAddUniqueIndex;
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

        /**
         * copy table structure from main table
         */
        final MySqlCreateTableStatement astCreateIndexTable = (MySqlCreateTableStatement) SQLUtils
            .parseStatementsWithDefaultFeatures(indexDef.getPrimaryTableDefinition(), JdbcConstants.MYSQL).get(0)
            .clone();

        final Set<String> indexAndPkColumnSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        indexAndPkColumnSet.addAll(indexColumnMap.keySet());
        if (!uniqueIndex) {
            // Add PK in check set because simple index may concat PK as partition key.
            indexAndPkColumnSet.addAll(getPrimaryKeyNames(astCreateIndexTable));
        }
        if (!containsAllShardingColumns(indexAndPkColumnSet, indexPartitionInfo)) {
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

        assert primaryPartitionInfo != null;
        final Set<String> shardingColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        shardingColumns.addAll(primaryPartitionInfo.getPartitionColumns());
        final Set<String> indexShardingColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        indexShardingColumns.addAll(indexPartitionInfo.getPartitionColumns());

        return createIndexTable(sqlAlterTable,
            indexColumnMap,
            coveringMap,
            astCreateIndexTable,
            shardingColumns,
            indexShardingColumns,
            relDdl,
            gsiPreparedData.getSchemaName(),
            gsiPreparedData.getPrimaryTableName(),
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

        /**
         * copy table structure from main table
         */
        final MySqlCreateTableStatement indexTableStmt =
            (MySqlCreateTableStatement) SQLUtils.parseStatementsWithDefaultFeatures(
                    sqlCreateIndex.getPrimaryTableDefinition(),
                    JdbcConstants.MYSQL)
                .get(0)
                .clone();

        final boolean unique = sqlCreateIndex.getConstraintType() != null
            && sqlCreateIndex.getConstraintType() == SqlCreateIndex.SqlIndexConstraintType.UNIQUE;

        final Set<String> indexAndPkColumnSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        indexAndPkColumnSet.addAll(indexColumnMap.keySet());
        if (!unique) {
            // Add PK in check set because simple index may concat PK as partition key.
            indexAndPkColumnSet.addAll(getPrimaryKeyNames(indexTableStmt));
        }
        if (!containsAllShardingColumns(indexAndPkColumnSet, indexPartitionInfo)) {
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

        assert primaryPartitionInfo != null;
        final Set<String> shardingColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        shardingColumns.addAll(primaryPartitionInfo.getPartitionColumns());
        final Set<String> indexShardingColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        indexShardingColumns.addAll(indexPartitionInfo.getPartitionColumns());
        final boolean isClusteredIndex = sqlCreateIndex.createClusteredIndex();

        if (isClusteredIndex) {
            return createClusteredIndexTable(indexName,
                indexColumnMap,
                indexTableStmt,
                unique,
                sqlCreateIndex.getOptions(),
                indexShardingColumns,
                relDdl,
                null,
                gsiPreparedData.getSchemaName(),
                gsiPreparedData.getPrimaryTableName(),
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
                indexShardingColumns,
                relDdl,
                null,
                gsiPreparedData.getSchemaName(),
                gsiPreparedData.getPrimaryTableName(),
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
        indexTablePreparedData.setTableGroupName(sqlCreateIndex.getTableGroupName());
    }

    @Override
    protected void addLocalIndex(Map<String, SqlIndexColumnName> indexColumnMap,
                                 MySqlCreateTableStatement indexTableStmt,
                                 boolean unique, boolean isGsi,
                                 List<SqlIndexOption> options) {

//        final List<String> indexShardKey = gsiPreparedData.getShardColumnsNotReorder();
//        if (indexShardKey != null && indexShardKey.size() > 0) {
//            if (isRepartition()) {
//                // like create table, look SqlCreateTable
//                SqlCreateTable.addCompositeIndex(indexColumnMap, indexTableStmt, false, options, false, indexShardKey);
//            } else {
//                SqlCreateTable.addCompositeIndex(indexColumnMap, indexTableStmt, unique, options, isGsi, indexShardKey);
//            }
//        }

        List<List<String>> allLevelPartKeys = gsiPreparedData.getAllLevelPartColumns();
        boolean useSubPartBy = false;
        boolean subPartKeyContainAllPartKeyAsPrefixCols = false;
        List<String> partKeyList = allLevelPartKeys.get(0);
        List<String> subPartKeyList = null;
        if (allLevelPartKeys.size() > 1 && allLevelPartKeys.get(1).size() > 0) {
            useSubPartBy = true;
            subPartKeyList = allLevelPartKeys.get(1);
            subPartKeyContainAllPartKeyAsPrefixCols =
                SqlCreateTable.checkIfContainPrefixPartCols(subPartKeyList, partKeyList);
        }
        if (!(useSubPartBy & subPartKeyContainAllPartKeyAsPrefixCols)) {

//            if (partKeyList.size() == 1) {
//                SqlCreateTable.addIndex(indexColumnMap, indexTableStmt, unique, options, true, partKeyList);
//            } else {
//                SqlCreateTable.addIndex(indexColumnMap, indexTableStmt, unique, options, true, new ArrayList<>());
//                SqlCreateTable.addCompositeIndex(indexColumnMap, indexTableStmt, partKeyList);
//            }

            if (isRepartition()) {
                // like create table, look SqlCreateTable
                SqlCreateTable.addCompositeIndex(indexColumnMap, indexTableStmt, false, options, false, partKeyList,
                    false, "");
            } else {
                SqlCreateTable.addCompositeIndex(indexColumnMap, indexTableStmt, unique, options, isGsi, partKeyList,
                    false, "");
            }
        }

        if (useSubPartBy) {
            SqlCreateTable.addCompositeIndex(indexColumnMap, indexTableStmt, false, ImmutableList.<SqlIndexOption>of(),
                false, subPartKeyList, false, "");
        }

//        SqlCreateTable.addLocalIndexForAutoTbl(indexColumnMap, indexTableStmt, unique, isGsi, options, gsiPreparedData.getAllLevelPartColumns(), isRepartition());
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

    @Override
    protected void genUniqueIndexForUGSI(MySqlCreateTableStatement indexTableStmt,
                                         List<SQLSelectOrderByItem> pkList) {
        final MySqlUnique uniqueIndex = new MySqlUnique();
        uniqueIndex.getIndexDefinition().setType("UNIQUE");
        uniqueIndex.getIndexDefinition().setKey(true);
        uniqueIndex.getIndexDefinition().setName(new SQLIdentifierExpr(TddlConstants.UGSI_PK_UNIQUE_INDEX_NAME));
        uniqueIndex.getIndexDefinition().getColumns().addAll(pkList);
        uniqueIndex.getIndexDefinition().getOptions().setIndexType("BTREE");
        uniqueIndex.getIndexDefinition().setParent(uniqueIndex);
        uniqueIndex.setParent(indexTableStmt);
        indexTableStmt.getTableElementList().add(uniqueIndex);
    }
}
