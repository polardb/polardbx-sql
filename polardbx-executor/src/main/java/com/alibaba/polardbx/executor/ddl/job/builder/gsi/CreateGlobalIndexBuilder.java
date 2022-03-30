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
import com.alibaba.polardbx.druid.sql.ast.SQLCurrentTimeExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLIndex;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLHexExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionByRange;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnPrimaryKey;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnReference;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MysqlForeignKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.partition.LocalPartitionDefinitionInfo;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RepartitionPrepareData;
import com.alibaba.polardbx.optimizer.partition.LocalPartitionDefinitionInfo;
import com.google.common.collect.Maps;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.ddl.job.builder.CreateTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceTableNameWithQuestionMarkVisitor;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.parse.custruct.FastSqlConstructUtils;
import com.alibaba.polardbx.optimizer.parse.visitor.ContextParameters;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SequenceBean;
import org.apache.calcite.sql.SqlAddIndex;
import org.apache.calcite.sql.SqlAddUniqueIndex;
import org.apache.calcite.sql.SqlAlterSpecification;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableRepartition;
import org.apache.calcite.sql.SqlAlterTablePartitionKey;
import org.apache.calcite.sql.SqlCreateIndex;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlDdlNodes;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexColumnName;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlIndexOption;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class CreateGlobalIndexBuilder extends DdlPhyPlanBuilder {

    protected final CreateGlobalIndexPreparedData gsiPreparedData;

    protected CreateTableBuilder indexTableBuilder;

    public CreateGlobalIndexBuilder(@Deprecated DDL ddl, CreateGlobalIndexPreparedData gsiPreparedData,
                                    ExecutionContext executionContext) {
        super(ddl, gsiPreparedData.getIndexTablePreparedData(), executionContext);
        this.gsiPreparedData = gsiPreparedData;
    }

    public static CreateGlobalIndexBuilder create(DDL ddl,
                                                  CreateGlobalIndexPreparedData preparedData,
                                                  ExecutionContext ec) {
        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(preparedData.getSchemaName());
        return isNewPartDb ?
            new CreatePartitionGlobalIndexBuilder(ddl, preparedData, ec) :
            new CreateGlobalIndexBuilder(ddl, preparedData, ec);
    }

    /**
     * @return true if it's repartition
     */
    public boolean isRepartition() {
        return relDdl != null && relDdl.sqlNode != null && (relDdl.sqlNode instanceof SqlAlterTablePartitionKey
            || relDdl.sqlNode instanceof SqlAlterTableRepartition);
    }

    /**
     * @return true if it's repartition single or broadcast
     */
    public boolean isRepartitionSingleOrBroadcast() {
        if (relDdl == null || relDdl.sqlNode == null) {
            return false;
        }
        if (relDdl.sqlNode instanceof SqlAlterTablePartitionKey) {
            return ((SqlAlterTablePartitionKey) relDdl.sqlNode).isSingleOrBroadcast();
        }
        if (relDdl.sqlNode instanceof SqlAlterTableRepartition) {
            return ((SqlAlterTableRepartition) relDdl.sqlNode).isSingleOrBroadcast();
        }
        return false;
    }

    @Override
    protected void buildTableRuleAndTopology() {
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

        indexTableBuilder = new CreateTableBuilder(relDdl, indexTablePreparedData, executionContext);
        indexTableBuilder.buildTableRuleAndTopology();

        gsiPreparedData.setIndexTableRule(indexTableBuilder.getTableRule());

        this.tableRule = indexTableBuilder.getTableRule();
        this.tableTopology = indexTableBuilder.getTableTopology();
    }

    protected void refreshShardingInfo(SqlIndexDefinition indexDef, CreateTablePreparedData indexTablePreparedData) {
        TableMeta tableToSchema = indexTablePreparedData.getTableMeta();

        // Generate auto partition for clustered index.
        final SqlNode dbpartition;
        if (tableToSchema.isAutoPartition() &&
            null == indexDef.getDbPartitionBy() && null == indexDef.getPartitioning()
            && !indexDef.isSingle() && !indexDef.isBroadcast()) {
            final String indexColName = indexDef.getColumns().get(0).getColumnNameStr();
            dbpartition = generateDbPartition(tableToSchema, indexColName);
            // Replace the index define.
            indexDef = indexDef.rebuildToGsi(null, dbpartition, indexDef.isClustered());
        } else {
            dbpartition = indexDef.getDbPartitionBy();
            if (indexDef.getPartitioning() == null &&
                indexDef.getDbPartitionBy() == null &&
                !indexDef.isSingle() &&
                !indexDef.isBroadcast()) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    "Global (clustered) secondary index must have dbpartition/partition by.");
            }
        }
        if (dbpartition != null) {
            indexTablePreparedData.setSharding(true);
            gsiPreparedData.setIndexDefinition(indexDef);
            gsiPreparedData.getIndexTablePreparedData().setDbPartitionBy(dbpartition);
        }

        indexTablePreparedData.setTableName(gsiPreparedData.getIndexTableName());
        indexTablePreparedData.setDbPartitionBy(dbpartition);
        indexTablePreparedData.setDbPartitions(indexDef.getDbPartitions());
        indexTablePreparedData.setTbPartitionBy(indexDef.getTbPartitionBy());
        indexTablePreparedData.setTbPartitions(indexDef.getTbPartitions());
        indexTablePreparedData.setBroadcast(indexDef.isBroadcast());
        indexTablePreparedData.setSharding(!indexDef.isSingle() && !indexDef.isBroadcast());
        indexTablePreparedData.setPartitioning(indexDef.getPartitioning());
    }

    protected void refreshShardingInfo(SqlCreateIndex sqlCreateIndex, CreateTablePreparedData indexTablePreparedData) {
        TableMeta tableToSchema = indexTablePreparedData.getTableMeta();

        // Generate auto partition for clustered index.
        final SqlNode dbpartition;
        if (tableToSchema.isAutoPartition() && null == sqlCreateIndex.getDbPartitionBy()) {
            final String indexColName = sqlCreateIndex.getColumns().get(0).getColumnNameStr();
            dbpartition = generateDbPartition(tableToSchema, indexColName);
            // Replace the index define.
            sqlCreateIndex = sqlCreateIndex.rebuildToGsi(null, dbpartition, sqlCreateIndex.createClusteredIndex());
        } else {
            dbpartition = sqlCreateIndex.getDbPartitionBy();
            if (null == dbpartition && sqlCreateIndex.getPartitioning() == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    "Global (clustered) secondary index must have dbpartition/partition by.");
            }
        }
        gsiPreparedData.setSqlCreateIndex(sqlCreateIndex);

        if (dbpartition != null) {
            indexTablePreparedData.setSharding(true);
            gsiPreparedData.getIndexTablePreparedData().setSharding(true);
            gsiPreparedData.getIndexTablePreparedData().setDbPartitionBy(dbpartition);
        }

        indexTablePreparedData.setTableName(gsiPreparedData.getIndexTableName());
        indexTablePreparedData.setDbPartitionBy(dbpartition);
        indexTablePreparedData.setDbPartitions(sqlCreateIndex.getDbPartitions());
        indexTablePreparedData.setTbPartitionBy(sqlCreateIndex.getTbPartitionBy());
        indexTablePreparedData.setTbPartitions(sqlCreateIndex.getTbPartitions());
        indexTablePreparedData.setPartitioning(sqlCreateIndex.getPartitioning());
    }

    @Override
    protected void buildPhysicalPlans() {
        buildSqlTemplate();
        buildPhysicalPlans(gsiPreparedData.getIndexTableName());
    }

    /**
     * build sqlTemplate
     * <p>
     * todo: maybe we should pass args in another way, pass args by AST makes code very complicate
     * further more, it makes reuse CreateTableBuilder impossible
     */
    @Override
    protected void buildSqlTemplate() {
        super.buildSqlTemplate();
        SqlDdl sqlDdl = (SqlDdl) relDdl.sqlNode;
        if (sqlDdl.getKind() == SqlKind.CREATE_TABLE || sqlDdl.getKind() == SqlKind.ALTER_TABLE) {
            buildSqlTemplate(gsiPreparedData.getIndexDefinition());
        } else if (sqlDdl.getKind() == SqlKind.CREATE_INDEX) {
            buildSqlTemplate(gsiPreparedData.getSqlCreateIndex());
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_UNSUPPORTED,
                "DDL Kind '" + sqlDdl.getKind() + "' for GSI creation");
        }
    }

    private void buildSqlTemplate(SqlIndexDefinition indexDef) {
        SqlIdentifier indexTableName = new SqlIdentifier(gsiPreparedData.getIndexTableName(), SqlParserPos.ZERO);
        SqlIdentifier primaryTableName = new SqlIdentifier(gsiPreparedData.getPrimaryTableName(), SqlParserPos.ZERO);

        final List<SqlAlterSpecification> alters = new ArrayList<>();
        if (gsiPreparedData.isUnique()) {
            alters.add(new SqlAddUniqueIndex(SqlParserPos.ZERO, indexTableName, indexDef));
        } else {
            alters.add(new SqlAddIndex(SqlParserPos.ZERO, indexTableName, indexDef));
        }

        indexDef.setPrimaryTableDefinition(gsiPreparedData.getPrimaryTableDefinition());

        final SqlAlterTable sqlAlterTable =
            new SqlAlterTable(primaryTableName, new HashMap<>(), "", null, alters, SqlParserPos.ZERO);

        this.originSqlTemplate = sqlAlterTable;
        this.sequenceBean = null;

        this.sqlTemplate = buildIndexTableDefinition(sqlAlterTable, isRepartition());
    }

    private void buildSqlTemplate(SqlCreateIndex sqlCreateIndex) {
        this.sqlTemplate = buildIndexTableDefinition(sqlCreateIndex);
    }

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
        final TableRule primaryRule = gsiPreparedData.getPrimaryTableRule();

        /**
         * check if index columns contains all sharding columns
         */
        final TableRule indexRule = gsiPreparedData.getIndexTableRule();

        final Set<String> indexColumnSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        indexColumnSet.addAll(indexColumnMap.keySet());
        if (!containsAllShardingColumns(indexColumnSet, indexRule)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_INDEX_AND_SHARDING_COLUMNS_NOT_MATCH);
        }

        /**
         * check single/broadcast table
         */
        if (null != primaryRule) {
            final boolean singleTable = GeneralUtil.isEmpty(primaryRule.getDbShardRules())
                && GeneralUtil.isEmpty(primaryRule.getTbShardRules());
            if (forceAllowGsi == false && (primaryRule.isBroadcast() || singleTable)) {
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

        assert primaryRule != null;
        final Set<String> shardingColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        shardingColumns.addAll(primaryRule.getShardColumns());

        return createIndexTable(sqlAlterTable,
            indexColumnMap,
            coveringMap,
            astCreateIndexTable,
            shardingColumns,
            relDdl,
            gsiPreparedData.getSchemaName(),
            gsiPreparedData.getPrimaryTableName(),
            executionContext);
    }

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
        final TableRule primaryRule = gsiPreparedData.getPrimaryTableRule();
        final TableRule indexRule = gsiPreparedData.getIndexTableRule();

        final Set<String> indexColumnSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        indexColumnSet.addAll(indexColumnMap.keySet());
        if (!containsAllShardingColumns(indexColumnSet, indexRule)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_INDEX_AND_SHARDING_COLUMNS_NOT_MATCH);
        }

        /**
         * check single/broadcast table
         */
        if (null != primaryRule) {
            final boolean singleTable = GeneralUtil.isEmpty(primaryRule.getDbShardRules())
                && GeneralUtil.isEmpty(primaryRule.getTbShardRules());
            if (primaryRule.isBroadcast() || singleTable) {
                throw new TddlRuntimeException(
                    ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED_PRIMARY_TABLE_DEFINITION,
                    "Does not support create Global Secondary Index on single or broadcast table");
            }
        }

        /**
         * copy table structure from main table
         */
        final MySqlCreateTableStatement indexTableStmt =
            (MySqlCreateTableStatement) SQLUtils.parseStatements(gsiPreparedData.getPrimaryTableDefinition(),
                    JdbcConstants.MYSQL)
                .get(0)
                .clone();

        final boolean unique = sqlCreateIndex.getConstraintType() != null
            && sqlCreateIndex.getConstraintType() == SqlCreateIndex.SqlIndexConstraintType.UNIQUE;
        assert primaryRule != null;
        final Set<String> shardingColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        shardingColumns.addAll(primaryRule.getShardColumns());
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
                relDdl,
                null,
                gsiPreparedData.getSchemaName(),
                gsiPreparedData.getPrimaryTableName(),
                executionContext
            );
        }
    }

    public SqlNode createIndexTable(SqlAlterTable sqlAlterTable,
                                    Map<String, SqlIndexColumnName> indexColumnMap,
                                    Map<String, SqlIndexColumnName> coveringMap,
                                    MySqlCreateTableStatement indexTableStmt, Set<String> shardingKey,
                                    DDL relDdl, String schemaName, String primaryTableName, ExecutionContext ec) {
        final SqlAddIndex addIndex = (SqlAddIndex) sqlAlterTable.getAlters().get(0);
        final SqlIdentifier indexTableName = addIndex.getIndexDef().getIndexName();
        final boolean unique = addIndex instanceof SqlAddUniqueIndex;
        final List<SqlIndexOption> options = addIndex.getIndexDef().getOptions();
        final boolean isClusteredIndex = addIndex.isClusteredIndex();

        if (isClusteredIndex) {
            return createClusteredIndexTable(indexTableName,
                indexColumnMap,
                indexTableStmt,
                unique,
                options,
                relDdl,
                sqlAlterTable,
                schemaName,
                primaryTableName,
                ec
            );
        } else {
            return createIndexTable(indexTableName,
                indexColumnMap,
                coveringMap,
                indexTableStmt,
                unique,
                options,
                shardingKey,
                relDdl,
                sqlAlterTable,
                schemaName,
                primaryTableName,
                ec
            );
        }
    }

    /**
     * Drop index from primary table when creating global index table.
     * 1. For sharding table: keep indices that not contained by existing columns
     * 2. For partition table: drop all local index
     *
     * @param fullColumn existing columns on global index table
     * @param iterator iterator over primary table keys
     * @param key key on primary table
     */
    protected void removeIndexOnGsi(Set<String> fullColumn,
                                    Iterator<SQLTableElement> iterator,
                                    SQLIndex key) {
        for (SQLSelectOrderByItem column : key.getColumns()) {
            final String columnName = SqlCreateTable.getIndexColumnName(column);
            if (fullColumn.contains(columnName)) {
                continue;
            }

            iterator.remove();
            break;
        }
    }

    /**
     * For sharding table: keep primary key.
     * For partition table: remove primary key if it's unique gsi
     *
     * @param iterator the primary key iterator
     */
    protected <T> void removePkOnGsi(Iterator<T> iterator) {
        // do not remove
    }

    /**
     * For sharding table: do nothing
     * For partition table: add a simple index for UGSI, since it's primary key is removed
     */
    protected void genSimpleIndexForUGSI(MySqlCreateTableStatement indexTableStmt,
                                         List<SQLSelectOrderByItem> pkList) {
    }

    /**
     * change gsi to local index when alter table repartition to single or broadcast table.
     */
    protected void genLocalIndexForRepartition(MySqlCreateTableStatement indexTableStmt) {
        RepartitionPrepareData repartitionPrepareData = gsiPreparedData.getRepartitionPrepareData();
        if (repartitionPrepareData == null || repartitionPrepareData.getLocalIndexes() == null) {
            return;
        }

        for (Map.Entry<String, List<String>> entry : repartitionPrepareData.getLocalIndexes().entrySet()) {
            final Iterator<SQLTableElement> it = indexTableStmt.getTableElementList().iterator();
            List<SQLSelectOrderByItem> indexColumns = new ArrayList<>();
            for (String name : entry.getValue()) {
                indexColumns.add(new SQLSelectOrderByItem(new SQLIdentifierExpr(name)));
            }

            boolean createLocalIndex = false;
            while (it.hasNext()) {
                final SQLTableElement tableElement = it.next();
                if (tableElement instanceof MySqlTableIndex) {
                    MySqlTableIndex mySqlTableIndex = (MySqlTableIndex) tableElement;
                    if (mySqlTableIndex.getIndexDefinition().getColumns().equals(indexColumns)) {
                        SQLIdentifierExpr indexName = (SQLIdentifierExpr) mySqlTableIndex.getName();
                        if (indexName != null && indexName.getName().contains(TddlConstants.AUTO_LOCAL_INDEX_PREFIX)) {
                            it.remove();
                            createLocalIndex = true;
                        }
                    }
                } else if (tableElement instanceof MySqlKey) {
                    MySqlKey mySqlKey = (MySqlKey) tableElement;
                    if (mySqlKey.getIndexDefinition().getColumns().equals(indexColumns)) {
                        SQLIdentifierExpr keyName = (SQLIdentifierExpr) mySqlKey.getName();
                        if (keyName != null && keyName.getName().contains(TddlConstants.AUTO_LOCAL_INDEX_PREFIX)) {
                            it.remove();
                            createLocalIndex = true;
                        }
                    }
                }
            }

            if (!createLocalIndex) {
                continue;
            }
            final MySqlTableIndex index = new MySqlTableIndex();
            index.getIndexDefinition().setIndex(true);
            index.getIndexDefinition().setName(new SQLIdentifierExpr(entry.getKey()));
            index.getIndexDefinition().getColumns().addAll(indexColumns);
            // BTREE OR HASH
            index.getIndexDefinition().getOptions().setIndexType("BTREE");
            index.getIndexDefinition().setParent(index);
            index.setParent(indexTableStmt);
            indexTableStmt.getTableElementList().add(index);
        }
    }

    /**
     * build SqlCreateTable for index table, add primary key and sharding key to
     * covering by default;
     */
    protected SqlNode createIndexTable(SqlIdentifier indexTableName,
                                       Map<String, SqlIndexColumnName> indexColumnMap,
                                       Map<String, SqlIndexColumnName> coveringMap,
                                       MySqlCreateTableStatement indexTableStmt, boolean unique,
                                       List<SqlIndexOption> options, Set<String> shardingKey,
                                       DDL relDdl, SqlAlterTable sqlAlterTable,
                                       String schemaName, String primaryTableName, ExecutionContext ec) {
        final String gsiName = indexTableName.getLastName();

        // update index table name
        indexTableStmt.setTableName(SqlIdentifier.surroundWithBacktick(gsiName));

        final Iterator<SQLTableElement> it = indexTableStmt.getTableElementList().iterator();
        final Set<String> sortedCovering = new LinkedHashSet<>();
        final Set<String> fullColumn = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        final Set<String> indexColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        indexColumns.addAll(indexColumnMap.keySet());
        final Set<String> coveringColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        coveringColumns.addAll(coveringMap.keySet());
        boolean withoutPk = true;
        String onUpdate = null;
        String defaultCurrentTime = null;
        String timestampWithoutDefault = null;
        String duplicatedIndexName = null;
        List<SQLSelectOrderByItem> pkList = new ArrayList<>();
        TableMeta primaryTableMeta = ec.getSchemaManager(schemaName).getTableWithNull(primaryTableName);

        /**
         * <pre>
         *     1. remove columns not included in sharding key or covering columns
         *     2. remove AUTO_INCREMENT property
         *     3. remove indexes(changed, no indexes should contained)
         *     4. remove foreign key
         *     5. remove primary key constraint if unique GSI
         *     6. add simple index of pk if primary key constraint removed
         *     7. check primary key exists
         *     8. check no DEFAULT CURRENT_TIMESTAMP specified for index or covering column
         *     9. check no ON UPDATE CURRENT_TIMESTAMP specified for index or covering column
         *    10. check all timestamp type columns has default value other than CURRENT_TIMESTAMP
         *    11. keep local index when alter table to single or broadcast table
         *    12. add local index (the partition key of gsi) when alter table with gsi to single or broadcast table
         *    13. check if we need set binary default value manually
         * </pre>
         */
        while (it.hasNext()) {
            final SQLTableElement tableElement = it.next();
            if (tableElement instanceof SQLColumnDefinition) {
                final SQLColumnDefinition columnDefinition = (SQLColumnDefinition) tableElement;
                final String columnName = SQLUtils.normalizeNoTrim(columnDefinition.getName().getSimpleName());

                if (!columnDefinition.isPrimaryKey() && !coveringColumns.contains(columnName)
                    && !indexColumns.contains(columnName) && !shardingKey.contains(columnName)) {
                    it.remove();
                } else {
                    final boolean addToCovering =
                        !indexColumns.contains(columnName) && (coveringColumns.contains(columnName) || shardingKey
                            .contains(columnName) || columnDefinition.isPrimaryKey());
                    if (addToCovering) {
                        sortedCovering.add(columnName);
                    }
                    fullColumn.add(columnName);

                    if (!isRepartition() && columnDefinition.isAutoIncrement()) {
                        columnDefinition.setAutoIncrement(false);
                    }

                    if (null != columnDefinition.getConstraints()) {
                        final Iterator<SQLColumnConstraint> constraintIt = columnDefinition.getConstraints().iterator();
                        while (constraintIt.hasNext()) {
                            final SQLColumnConstraint constraint = constraintIt.next();
                            if (constraint instanceof SQLColumnPrimaryKey) {
                                withoutPk = false;
                                if (!pkList.isEmpty()) {
                                    throw new TddlRuntimeException(
                                        ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED_PRIMARY_TABLE_DEFINITION,
                                        "multiple primary key definition");
                                }
                                pkList.add(new SQLSelectOrderByItem(columnDefinition.getName()));
                                if (unique) {
                                    removePkOnGsi(constraintIt);
                                }
                            } else if (constraint instanceof SQLColumnReference) {
                                // remove foreign key
                                constraintIt.remove();
                            }
                        }
                    }

                    final SQLExpr defaultExpr = columnDefinition.getDefaultExpr();
                    defaultCurrentTime = extractCurrentTimestamp(defaultCurrentTime, defaultExpr);

                    onUpdate = extractCurrentTimestamp(onUpdate, columnDefinition.getOnUpdate());

                    if ("timestamp".equalsIgnoreCase(columnDefinition.getDataType().getName()) && null == defaultExpr) {
                        timestampWithoutDefault = columnName;
                    }

                    // primaryTableMeta may be null if we create table with gsi, however in this case we will not need
                    // to convert default value
                    if (primaryTableMeta != null) {
                        ColumnMeta columnMeta = primaryTableMeta.getColumnIgnoreCase(columnName);
                        if (columnMeta.isBinaryDefault()) {
                            SQLHexExpr newDefaultVal = new SQLHexExpr(columnMeta.getField().getDefault());
                            columnDefinition.setDefaultExpr(newDefaultVal);
                        }
                    }
                }
            } else if (tableElement instanceof MySqlPrimaryKey) {
                withoutPk = false;

                final MySqlPrimaryKey primaryKey = (MySqlPrimaryKey) tableElement;
                if (!pkList.isEmpty()) {
                    throw new TddlRuntimeException(
                        ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED_PRIMARY_TABLE_DEFINITION,
                        "multiple primary key definition");
                }
                pkList.addAll(primaryKey.getColumns());
                if (unique) {
                    removePkOnGsi(it);
                }
            } else if (tableElement instanceof MySqlKey) {
                final MySqlKey key = (MySqlKey) tableElement;

                final String indexName = ((SQLIdentifierExpr) key.getName()).normalizedName();
                if (TStringUtil.equalsIgnoreCase(indexName, gsiName)) {
                    duplicatedIndexName = indexName;
                }

                if (!isRepartition() || (isRepartition() && indexName.contains(TddlConstants.AUTO_SHARD_KEY_PREFIX))) {
                    removeIndexOnGsi(fullColumn, it, key);
                }
            } else if (tableElement instanceof MySqlTableIndex) {
                final MySqlTableIndex tableIndex = (MySqlTableIndex) tableElement;

                final String indexName = ((SQLIdentifierExpr) tableIndex.getName()).normalizedName();
                if (TStringUtil.equalsIgnoreCase(indexName, gsiName)) {
                    duplicatedIndexName = indexName;
                }

                if (!isRepartition() || (isRepartition() && indexName.contains(TddlConstants.AUTO_SHARD_KEY_PREFIX))) {
                    removeIndexOnGsi(fullColumn, it, tableIndex);
                }
            } else if (tableElement instanceof MysqlForeignKey) {
                final MysqlForeignKey foreignKey = (MysqlForeignKey) tableElement;

                final String indexName = ((SQLIdentifierExpr) foreignKey.getName()).normalizedName();
                if (TStringUtil.equalsIgnoreCase(indexName, gsiName)) {
                    duplicatedIndexName = indexName;
                }

                it.remove();
            }
        }

        // Generate simple index of pk on unique GSI.
        if (unique && !pkList.isEmpty()) {
            genSimpleIndexForUGSI(indexTableStmt, pkList);
        }

        // for alter repartition
        // Generate simple index of other gsi partition key when alter table to single or broadcast
        // because the other gsi tables will be dropped
        if (isRepartitionSingleOrBroadcast()) {
            genLocalIndexForRepartition(indexTableStmt);
        }

        validatePartitionColumnInPkForLocalPartition(pkList);

        final PlannerContext context = (PlannerContext) relDdl.getCluster().getPlanner().getContext();
        final boolean defaultCurrentTimestamp =
            context.getParamManager().getBoolean(ConnectionParams.GSI_DEFAULT_CURRENT_TIMESTAMP);
        final boolean onUpdateCurrentTimestamp =
            context.getParamManager().getBoolean(ConnectionParams.GSI_ON_UPDATE_CURRENT_TIMESTAMP);

        if (withoutPk) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED_PRIMARY_TABLE_DEFINITION,
                "need primary key");
        }

        if (null != defaultCurrentTime && !defaultCurrentTimestamp) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED_INDEX_TABLE_DEFINITION,
                "cannot use DEFAULT " + defaultCurrentTime + " on index or covering column");
        }

        if (null != onUpdate && !onUpdateCurrentTimestamp) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED_INDEX_TABLE_DEFINITION,
                "cannot use ON UPDATE " + onUpdate + " on index or covering column");
        }

        if (null != timestampWithoutDefault && (!defaultCurrentTimestamp || !onUpdateCurrentTimestamp)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED_INDEX_TABLE_DEFINITION,
                "need default value other than CURRENT_TIMESTAMP for column `" + timestampWithoutDefault + "`");
        }

        if (null != duplicatedIndexName) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED_INDEX_TABLE_DEFINITION,
                "Duplicate index name '" + duplicatedIndexName + "'");
        }

        // rebuild covering columns
        SqlNode sqlNode = relDdl.getSqlNode();
        if (sqlNode instanceof SqlCreateIndex) {
            final SqlCreateIndex createIndex = (SqlCreateIndex) sqlNode;
            relDdl.sqlNode = createIndex.rebuildCovering(sortedCovering);
            gsiPreparedData.setSqlCreateIndex((SqlCreateIndex) relDdl.sqlNode);
        } else if (sqlNode instanceof SqlAlterTable) {
            final SqlAlterTable alterTable = (SqlAlterTable) sqlNode;
            final SqlAddIndex oldAddIndex = (SqlAddIndex) alterTable.getAlters().get(0);
            final SqlIndexDefinition oldIndexDef = oldAddIndex.getIndexDef();
            final SqlIndexDefinition newIndexDef = oldIndexDef.replaceCovering(sortedCovering);

            updateAddIndex(alterTable, 0, oldAddIndex, newIndexDef);
        } else if (sqlNode instanceof SqlCreateTable) {
            final SqlAddIndex addIndex = (SqlAddIndex) sqlAlterTable.getAlters().get(0);
            final SqlIndexDefinition oldIndexDef = addIndex.getIndexDef();
            final SqlIndexDefinition newIndexDef = oldIndexDef.replaceCovering(sortedCovering);

            updateAddIndex(sqlAlterTable, 0, addIndex, newIndexDef);
        }


        if (isRepartition()) {
            addLocalIndex(indexColumnMap, indexTableStmt, unique, options);
        } else {
            final List<String> indexShardKey = gsiPreparedData.getShardColumns();
            SqlCreateTable.addIndex(indexColumnMap, indexTableStmt, unique, options, true, indexShardKey);
        }

        final SqlNodeList columnList = new SqlNodeList(SqlParserPos.ZERO);
        final SequenceBean sequenceBean = FastSqlConstructUtils.convertTableElements(columnList,
            indexTableStmt.getTableElementList(), new ContextParameters(false), ec);
        if (sequenceBean != null) {
            sequenceBean.setSchemaName(schemaName);
        }

        final String createIndexTable = SQLUtils.toSQLString(indexTableStmt, com.alibaba.polardbx.druid.DbType.mysql);

        final SqlCreateTable result = SqlDdlNodes.createTable(SqlParserPos.ZERO,
            false,
            false,
            indexTableName,
            null,
            columnList,
            null,
            null,
            null,
            null,
            null,
            createIndexTable,
            false,
            sequenceBean,
            null,
            null,
            null,
            null);

        result.setUniqueShardingKey(unique);

        ReplaceTableNameWithQuestionMarkVisitor visitor = new ReplaceTableNameWithQuestionMarkVisitor(schemaName, ec);
        return result.accept(visitor);
    }

    protected void addLocalIndex(Map<String, SqlIndexColumnName> indexColumnMap,
                                 MySqlCreateTableStatement indexTableStmt,
                                 boolean unique,
                                 List<SqlIndexOption> options) {
        final List<String> indexShardKey = gsiPreparedData.getShardColumns();
        if (indexShardKey != null && indexShardKey.size() > 0) {
            SqlCreateTable.addIndex(indexColumnMap, indexTableStmt, unique, options, true, indexShardKey);
        }
    }

    /**
     * build SqlCreateTable for clustered index table, add sharding key to index;
     */
    protected SqlNode createClusteredIndexTable(SqlIdentifier indexTableName,
                                                Map<String, SqlIndexColumnName> indexColumnMap,
                                                MySqlCreateTableStatement indexTableStmt, boolean unique,
                                                List<SqlIndexOption> options, DDL relDdl,
                                                SqlAlterTable sqlAlterTable, String schemaName, String primaryTableName,
                                                ExecutionContext ec) {
        final String gsiName = indexTableName.getLastName();

        // update index table name
        indexTableStmt.setTableName(SqlIdentifier.surroundWithBacktick(gsiName));
        final Set<String> sortedCovering = new LinkedHashSet<>();
        final Set<String> indexColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        indexColumns.addAll(indexColumnMap.keySet());

        final Iterator<SQLTableElement> it = indexTableStmt.getTableElementList().iterator();

        boolean withoutPk = true;
        String onUpdate = null;
        String defaultCurrentTime = null;
        String timestampWithoutDefault = null;
        String duplicatedIndexName = null;
        List<SQLSelectOrderByItem> pkList = new ArrayList<>();
        TableMeta primaryTableMeta = ec.getSchemaManager(schemaName).getTableWithNull(primaryTableName);

        /**
         * <pre>
         *     1. remove AUTO_INCREMENT property
         *     2. remove foreign key
         *     3. check primary key exists
         *     4. check no DEFAULT CURRENT_TIMESTAMP specified for index or covering column
         *     5. check no ON UPDATE CURRENT_TIMESTAMP specified for index or covering column
         *     6. check all timestamp type columns has default value other than CURRENT_TIMESTAMP
         * </pre>
         */
        while (it.hasNext()) {
            final SQLTableElement tableElement = it.next();
            if (tableElement instanceof SQLColumnDefinition) {
                final SQLColumnDefinition columnDefinition = (SQLColumnDefinition) tableElement;
                final String columnName = SQLUtils.normalizeNoTrim(columnDefinition.getName().getSimpleName());

                if (!indexColumns.contains(columnName)) {
                    sortedCovering.add(columnName);
                }
                if (columnDefinition.isAutoIncrement()) {
                    columnDefinition.setAutoIncrement(false);
                }

                if (null != columnDefinition.getConstraints()) {
                    final Iterator<SQLColumnConstraint> constraintIt = columnDefinition.getConstraints().iterator();
                    while (constraintIt.hasNext()) {
                        final SQLColumnConstraint constraint = constraintIt.next();
                        if (constraint instanceof SQLColumnPrimaryKey) {
                            withoutPk = false;
                            if (!pkList.isEmpty()) {
                                throw new TddlRuntimeException(
                                    ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED_PRIMARY_TABLE_DEFINITION,
                                    "multiple primary key definition");
                            }
                            pkList.add(new SQLSelectOrderByItem(columnDefinition.getName()));
                        } else if (constraint instanceof SQLColumnReference) {
                            // remove foreign key
                            constraintIt.remove();
                        }
                    }
                }
                // 暂时不限制defaultCurrentTime，
//                final SQLExpr defaultExpr = columnDefinition.getDefaultExpr();
//                defaultCurrentTime = extractCurrentTimestamp(defaultCurrentTime, defaultExpr);
//
//                onUpdate = extractCurrentTimestamp(onUpdate, columnDefinition.getOnUpdate());
//
//                if ("timestamp".equalsIgnoreCase(columnDefinition.getDataType().getName()) && null == defaultExpr) {
//                    timestampWithoutDefault = columnName;
//                }

                // primaryTableMeta may be null if we create table with gsi, however in this case we will not need
                // to convert default value
                if (primaryTableMeta != null) {
                    ColumnMeta columnMeta = primaryTableMeta.getColumnIgnoreCase(columnName);
                    if (columnMeta.isBinaryDefault()) {
                        SQLHexExpr newDefaultVal = new SQLHexExpr(columnMeta.getField().getDefault());
                        columnDefinition.setDefaultExpr(newDefaultVal);
                    }
                }
            } else if (tableElement instanceof MySqlPrimaryKey) {
                withoutPk = false;
                final MySqlPrimaryKey primaryKey = (MySqlPrimaryKey) tableElement;
                if (!pkList.isEmpty()) {
                    throw new TddlRuntimeException(
                        ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED_PRIMARY_TABLE_DEFINITION,
                        "multiple primary key definition");
                }
                pkList.addAll(primaryKey.getColumns());
            } else if (tableElement instanceof MySqlKey) {
                final MySqlKey key = (MySqlKey) tableElement;

                final String indexName = ((SQLIdentifierExpr) key.getName()).normalizedName();
                if (TStringUtil.equalsIgnoreCase(indexName, gsiName)) {
                    duplicatedIndexName = indexName;
                }

            } else if (tableElement instanceof MySqlTableIndex) {
                final MySqlTableIndex tableIndex = (MySqlTableIndex) tableElement;

                final String indexName = ((SQLIdentifierExpr) tableIndex.getName()).normalizedName();
                if (TStringUtil.equalsIgnoreCase(indexName, gsiName)) {
                    duplicatedIndexName = indexName;
                }

            } else if (tableElement instanceof MysqlForeignKey) {
                final MysqlForeignKey foreignKye = (MysqlForeignKey) tableElement;

                final String indexName = ((SQLIdentifierExpr) foreignKye.getName()).normalizedName();
                if (TStringUtil.equalsIgnoreCase(indexName, gsiName)) {
                    duplicatedIndexName = indexName;
                }

                it.remove();
            }
        }

        validatePartitionColumnInPkForLocalPartition(pkList);

        if (withoutPk) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED_PRIMARY_TABLE_DEFINITION,
                "need primary key");
        }

        if (null != defaultCurrentTime) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED_INDEX_TABLE_DEFINITION,
                "cannot use DEFAULT " + defaultCurrentTime + " on index or covering column");
        }

        if (null != onUpdate) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED_INDEX_TABLE_DEFINITION,
                "cannot use ON UPDATE " + onUpdate + " on index or covering column");
        }

        if (null != timestampWithoutDefault) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED_INDEX_TABLE_DEFINITION,
                "need default value other than CURRENT_TIMESTAMP for column `" + timestampWithoutDefault + "`");
        }

        if (null != duplicatedIndexName) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED_INDEX_TABLE_DEFINITION,
                "Duplicate index name '" + duplicatedIndexName + "'");
        }

        // rebuild covering columns
        SqlNode sqlNode = relDdl.getSqlNode();
        if (sqlNode instanceof SqlCreateIndex) {
            final SqlCreateIndex createIndex = (SqlCreateIndex) sqlNode;
            relDdl.sqlNode = createIndex.rebuildCovering(sortedCovering);
            gsiPreparedData.setSqlCreateIndex((SqlCreateIndex) relDdl.sqlNode);
        } else if (sqlNode instanceof SqlAlterTable) {
            final SqlAlterTable alterTable = (SqlAlterTable) sqlNode;
            final SqlAddIndex oldAddIndex = (SqlAddIndex) alterTable.getAlters().get(0);
            final SqlIndexDefinition oldIndexDef = oldAddIndex.getIndexDef();
            final SqlIndexDefinition newIndexDef = oldIndexDef.replaceCovering(sortedCovering);

            updateAddIndex(alterTable, 0, oldAddIndex, newIndexDef);
        } else if (sqlNode instanceof SqlCreateTable) {
            final SqlAddIndex addIndex = (SqlAddIndex) sqlAlterTable.getAlters().get(0);
            final SqlIndexDefinition oldIndexDef = addIndex.getIndexDef();
            final SqlIndexDefinition newIndexDef = oldIndexDef.replaceCovering(sortedCovering);

            updateAddIndex(sqlAlterTable, 0, addIndex, newIndexDef);
        }

        final List<String> indexShardKey = gsiPreparedData.getShardColumns();
        SqlCreateTable.addIndex(indexColumnMap, indexTableStmt, unique, options, true, indexShardKey);

        final SqlNodeList columnList = new SqlNodeList(SqlParserPos.ZERO);
        final SequenceBean sequenceBean = FastSqlConstructUtils.convertTableElements(columnList,
            indexTableStmt.getTableElementList(),
            new ContextParameters(false), ec);
        if (sequenceBean != null) {
            sequenceBean.setSchemaName(schemaName);
        }

        final String createIndexTable = SQLUtils.toSQLString(indexTableStmt, com.alibaba.polardbx.druid.DbType.mysql);

        final SqlCreateTable result = SqlDdlNodes.createTable(SqlParserPos.ZERO,
            false,
            false,
            indexTableName,
            null,
            columnList,
            null,
            null,
            null,
            null,
            null,
            createIndexTable,
            false,
            sequenceBean,
            null,
            null,
            null,
            null);

        result.setUniqueShardingKey(unique);

        ReplaceTableNameWithQuestionMarkVisitor visitor = new ReplaceTableNameWithQuestionMarkVisitor(schemaName, ec);
        return result.accept(visitor);
    }

    private void updateAddIndex(SqlAlterTable sqlAlterTable, int itemIndex, SqlAddIndex addIndex,
                                SqlIndexDefinition newIndexDef) {
        switch (addIndex.getKind()) {
        case ADD_UNIQUE_INDEX:
            sqlAlterTable.getAlters().set(itemIndex,
                new SqlAddUniqueIndex(addIndex.getParserPosition(), addIndex.getIndexName(), newIndexDef));
            gsiPreparedData.setIndexDefinition(newIndexDef);
            break;
        case ADD_FULL_TEXT_INDEX:
        case ADD_SPATIAL_INDEX:
            // TODO handle FULL TEXT/SPATIAL INDEX
        case ADD_INDEX:
        default:
            sqlAlterTable.getAlters().set(itemIndex,
                new SqlAddIndex(addIndex.getParserPosition(), addIndex.getIndexName(), newIndexDef));
            gsiPreparedData.setIndexDefinition(newIndexDef);
            break;
        }
    }

    private String extractCurrentTimestamp(String onUpdate, SQLExpr onUpdateExpr) {
        if (onUpdateExpr instanceof SQLCurrentTimeExpr || onUpdateExpr instanceof SQLMethodInvokeExpr) {
            try {
                if (onUpdateExpr instanceof SQLMethodInvokeExpr) {
                    SQLCurrentTimeExpr.Type.valueOf(((SQLMethodInvokeExpr) onUpdateExpr).getMethodName().toUpperCase());
                    onUpdate = SQLUtils.toMySqlString(onUpdateExpr);
                } else {
                    onUpdate = ((SQLCurrentTimeExpr) onUpdateExpr).getType().name;
                }
            } catch (Exception e) {
                // ignore error for ON UPDATE CURRENT_TIMESTAMP(3);
            }
        }
        return onUpdate;
    }

    protected boolean containsAllShardingColumns(Set<String> indexColumnSet, TableRule indexRule) {
        boolean result = false;

        if (null != indexRule) {
            List<String> shardColumns = indexRule.getShardColumns();
            if (null != shardColumns) {
                result = indexColumnSet.containsAll(shardColumns);
            }
        }

        return result;
    }

    private void validatePartitionColumnInPkForLocalPartition(List<SQLSelectOrderByItem> pkList){
        if(CollectionUtils.isEmpty(pkList)){
            return;
        }
        if(gsiPreparedData.getIndexTablePreparedData() == null){
            return;
        }
        if(gsiPreparedData.getIndexTablePreparedData().getLocalPartitionDefinitionInfo()==null){
            return;
        }
        LocalPartitionDefinitionInfo definitionInfo =
            gsiPreparedData.getIndexTablePreparedData().getLocalPartitionDefinitionInfo();
        String localPartitionColumn = definitionInfo.getColumnName().replace("`", "").toLowerCase();
        List<String> columnNameList =
            pkList.stream().map(e->((SQLIdentifierExpr)e.getExpr()).getName().replace("`", "").toLowerCase()).collect(
                Collectors.toList());
        if(!columnNameList.contains(localPartitionColumn)){
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED_INDEX_TABLE_DEFINITION,
                String.format("Primary Key must contain local partition column: %s", localPartitionColumn));
        }
    }

}
