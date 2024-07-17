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

package com.alibaba.polardbx.planner.common;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.google.common.collect.Maps;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlAddIndex;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlCreateIndex;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexColumnName;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.HashMap;
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
        // Columnar index do not force using index column as partition column
        final boolean isColumnar = indexDef.isColumnar();
        if (!isColumnar && !containsAllShardingColumns(indexColumnSet, indexPartitionInfo)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_INDEX_AND_SHARDING_COLUMNS_NOT_MATCH);
        }

        /**
         * check single/broadcast table
         */
        if (null != primaryPartitionInfo) {
            if (!forceAllowGsi
                && !isColumnar
                && (primaryPartitionInfo.isBroadcastTable() || primaryPartitionInfo.isSingleTable())) {
                throw new TddlRuntimeException(
                    ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED_PRIMARY_TABLE_DEFINITION,
                    "Does not support create Global Secondary Index on single or broadcast table");
            }
        }

        /**
         * copy table structure from main table
         */
        final MySqlCreateTableStatement astCreateIndexTable = (MySqlCreateTableStatement) SQLUtils
            .parseStatementsWithDefaultFeatures(indexDef.getPrimaryTableDefinition(), JdbcConstants.MYSQL).get(0)
            .clone();

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
        // Columnar index do not force using index column as partition column
        final boolean isColumnar = sqlCreateIndex.createCci();
        if (!isColumnar && !containsAllShardingColumns(indexColumnSet, indexPartitionInfo)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_INDEX_AND_SHARDING_COLUMNS_NOT_MATCH);
        }

        /**
         * check single/broadcast table
         */
        if (null != primaryPartitionInfo && !isColumnar) {
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
            (MySqlCreateTableStatement) SQLUtils.parseStatementsWithDefaultFeatures(
                    sqlCreateIndex.getPrimaryTableDefinition(),
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

}
