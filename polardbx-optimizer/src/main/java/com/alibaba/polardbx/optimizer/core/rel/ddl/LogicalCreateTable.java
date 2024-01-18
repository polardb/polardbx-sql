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
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionByRange;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.DefaultExprUtil;
import com.alibaba.polardbx.optimizer.config.table.GeneratedColumnUtil;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateLocalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateTableWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.parse.TableMetaParser;
import com.alibaba.polardbx.optimizer.partition.common.LocalPartitionDefinitionInfo;
import com.alibaba.polardbx.optimizer.utils.DdlCharsetInfo;
import com.alibaba.polardbx.optimizer.utils.DdlCharsetInfoUtil;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import groovy.sql.Sql;
import org.apache.calcite.rel.ddl.CreateTable;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryStringLiteral;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlColumnDeclaration;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartitionByRange;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_CREATE_SELECT_FUNCTION_ALIAS;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_CREATE_SELECT_UPDATE;
import static com.alibaba.polardbx.common.properties.ConnectionParams.CREATE_TABLE_WITH_CHARSET_COLLATE;

public class LogicalCreateTable extends LogicalTableOperation {

    protected SqlCreateTable sqlCreateTable;

    protected String createTableSqlForLike;

    protected CreateTablePreparedData createTablePreparedData;
    private CreateTableWithGsiPreparedData createTableWithGsiPreparedData;
    private SqlSelect sqlSelect;

    boolean ignore = false;
    boolean replace = false;

    protected LogicalCreateTable(CreateTable createTable) {
        super(createTable);
        this.sqlCreateTable = (SqlCreateTable) relDdl.sqlNode;
    }

    public static LogicalCreateTable create(CreateTable createTable) {
        return new LogicalCreateTable(createTable);
    }

    public SqlCreateTable getSqlCreate() {
        return sqlCreateTable;
    }

    public void setSqlSelect(SqlSelect sqlSelect) {
        this.sqlSelect = sqlSelect;
    }

    public SqlSelect getSqlSelect() {
        return sqlSelect;
    }

    public void setIgnore(boolean ignore) {
        this.ignore = ignore;
    }

    public boolean isIgnore() {
        return ignore;
    }

    public void setReplace(boolean replace) {
        this.replace = replace;
    }

    public boolean isReplace() {
        return replace;
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

    public SqlCreateTable getSqlCreateTable() {
        return sqlCreateTable;
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
                    indexTablePreparedData.setJoinGroupName(createTablePreparedData.getJoinGroupName());
                    indexTablePreparedData.setVisible(gsi.getValue().isVisible());
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
                    uniqueIndexTablePreparedData.setVisible(gusi.getValue().isVisible());
                    uniqueIndexTablePreparedData.setJoinGroupName(createTablePreparedData.getJoinGroupName());
                    createTableWithGsiPreparedData.addIndexTablePreparedData(uniqueIndexTablePreparedData);
                }
            }

            if (sqlCreateTable.getClusteredKeys() != null) {
                for (Pair<SqlIdentifier, SqlIndexDefinition> gsi : sqlCreateTable.getClusteredKeys()) {
                    CreateGlobalIndexPreparedData indexTablePreparedData =
                        prepareGsiData(primaryTableName, primaryTableDefinition, gsi, false);
                    indexTablePreparedData.setVisible(gsi.getValue().isVisible());
                    indexTablePreparedData.setJoinGroupName(createTablePreparedData.getJoinGroupName());
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
                    uniqueIndexTablePreparedData.setVisible(gusi.getValue().isVisible());
                    uniqueIndexTablePreparedData.setJoinGroupName(createTablePreparedData.getJoinGroupName());
                    createTableWithGsiPreparedData.addIndexTablePreparedData(uniqueIndexTablePreparedData);
                }
            }
        }
    }

    String generateInsert(ExecutionContext executionContext) {
        // executionContext.getSchemaManager().getTable().
        // selectSql = prepareSelectSql();
        // 设置完 selectSql，然后set一下
        // insert into table(+属性) select ** from
        // ignore 和 replace 和 排序
        SqlPrettyWriter writer = new SqlPrettyWriter(MysqlSqlDialect.DEFAULT);
        writer.setAlwaysUseParentheses(true);
        writer.setSelectListItemsOnSeparateLines(false);
        writer.setIndentation(0);
        if (replace == true) {
            writer.keyword("REPLACE");
        } else {
            writer.keyword("INSERT");
            if (ignore == true) {
                writer.keyword("IGNORE");
            }
        }

        writer.keyword("INTO");
        if (replace == false) {
            writer.keyword("TABLE");
        }
        sqlCreateTable.getName().unparse(writer, 0, 0);
        SqlWriter.Frame frame = writer.startList("(", ")");
        // 插入select的属性
        for (SqlNode c : sqlSelect.getSelectList()) {
            writer.sep(",");
            if (c instanceof SqlIdentifier) {
                SqlIdentifier col = (SqlIdentifier) c;
                writer.identifier(col.getLastName());
            } else if (c instanceof SqlBasicCall) {
                SqlBasicCall basicCall = (SqlBasicCall) c;
                if (basicCall.getOperator().getKind().equals(SqlKind.AS)) {
                    if (basicCall.getOperands()[0].toString()
                        .equalsIgnoreCase(basicCall.getOperands()[basicCall.getOperands().length - 1].toString())) {
                        throw new TddlRuntimeException(ERR_CREATE_SELECT_FUNCTION_ALIAS,
                            "must alias all function calls or expressions in the query");
                    }
                    basicCall.getOperands()[basicCall.getOperands().length - 1].unparse(writer, 0, 0);
                } else {
                    throw new TddlRuntimeException(ERR_CREATE_SELECT_FUNCTION_ALIAS,
                        "must alias all function calls or expressions in the query");
                }
            }
            // c.unparse(writer, 0, 0);
        }
        writer.endList(frame);
        sqlSelect.unparse(writer, 0, 0);

//        if (sqlCreateTable.getPrimaryKey() == null) {
//            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
//                String.format("Primary key is null."));
//        }
//        writer.keyword("ORDER");
//        writer.keyword("BY");
//        sqlCreateTable.getPrimaryKey().getColumns().get(0).unparse(writer, 0, 0);
//        if (sqlSelect.hasOrderBy() == false) {
//            SqlNode fromTable = sqlSelect.getFrom();
//            if (fromTable instanceof SqlIdentifier && ((SqlIdentifier) fromTable).names.size() > 1) {
//                // List<String> names = ((SqlIdentifier) fromTable).names;
//                writer.keyword("ORDER");
//                writer.keyword("BY");
//                SqlIdentifier table = (SqlIdentifier) fromTable;
//                executionContext.getSchemaManager().getTable(table.getLastName()).getPrimaryKey()
//            }
//        }
        return writer.toSqlString().getSql();
    }

    private CreateTablePreparedData preparePrimaryData(ExecutionContext executionContext) {
        if (sqlCreateTable.getLikeTableName() != null) {
            prepareCreateTableLikeData();
        }
        String selectSql = null;
        if (sqlCreateTable.isSelect()) {
            selectSql = generateInsert(executionContext);
        }
        // for mysql8.0
        MySqlCreateTableStatement stmt =
            (MySqlCreateTableStatement) FastsqlUtils.parseSql(sqlCreateTable.getSourceSql()).get(0);
        boolean useDbCharset = executionContext.getParamManager().getBoolean(CREATE_TABLE_WITH_CHARSET_COLLATE);
        String defaultCharset = sqlCreateTable.getDefaultCharset();
        String defaultCollation = sqlCreateTable.getDefaultCollation();
        if (useDbCharset) {
            String charset;
            String collation;
            StringBuilder builder = new StringBuilder();
            charset = DbInfoManager.getInstance().getDbChartSet(schemaName);
            collation = DbInfoManager.getInstance().getDbCollation(schemaName);

            if (StringUtils.isEmpty(charset) || StringUtils.isEmpty(collation)) {
                /**
                 * For some unit-test, its dbInfo is mock,so its charset & collation maybe null
                 */
                // Fetch server default collation
                DdlCharsetInfo serverDefaultCharsetInfo =
                    DdlCharsetInfoUtil.fetchServerDefaultCharsetInfo(executionContext, true);

                // Fetch db collation by charset & charset defined by user and server default collation
                DdlCharsetInfo createDbCharInfo =
                    DdlCharsetInfoUtil.decideDdlCharsetInfo(executionContext, serverDefaultCharsetInfo.finalCharset,
                        serverDefaultCharsetInfo.finalCollate,
                        charset, collation, true);
                charset = createDbCharInfo.finalCharset;
                collation = createDbCharInfo.finalCollate;
            }

            // Fetch tbl collation by charset & charset defined by user and db collation
            DdlCharsetInfo createTbCharInfo =
                DdlCharsetInfoUtil.decideDdlCharsetInfo(executionContext, charset, collation,
                    defaultCharset, defaultCollation, true);

            if (defaultCharset == null && defaultCollation == null && createTbCharInfo.finalCharset != null) {
                builder.append(" CHARSET `").append(createTbCharInfo.finalCharset.toLowerCase()).append("`");
                sqlCreateTable.setDefaultCharset(createTbCharInfo.finalCharset.toLowerCase());
            }

            if (defaultCollation == null && createTbCharInfo.finalCollate != null) {
                builder.append(" COLLATE `").append(createTbCharInfo.finalCollate.toLowerCase()).append("`");
                sqlCreateTable.setDefaultCollation(createTbCharInfo.finalCollate.toLowerCase());
            }

            stmt.setAfterSemi(false);
            if (sqlCreateTable.getQuery() == null) {
                sqlCreateTable.setSourceSql(stmt.toString() + builder);
            }
        }

        String tableName = ((SqlIdentifier) sqlCreateTable.getName()).getLastName();
        final TableMeta tableMeta = TableMetaParser.parse(tableName, sqlCreateTable);
        tableMeta.setSchemaName(schemaName);

        LocalPartitionDefinitionInfo localPartitionDefinitionInfo = LocalPartitionDefinitionInfo.create(
            schemaName,
            tableMeta.getTableName(),
            (SqlPartitionByRange) sqlCreateTable.getLocalPartition()
        );
        if (localPartitionDefinitionInfo != null) {
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
            sqlCreateTable.getJoinGroupName(),
            sqlCreateTable.getLocality(),
            ((CreateTable) relDdl).getPartBoundExprInfo(),
            sqlCreateTable.getOriginalSql(),
            sqlCreateTable.getLogicalReferencedTables(),
            sqlCreateTable.getAddedForeignKeys());
        res.setSelectSql(selectSql);

        boolean hasTimestampColumnDefault = false;
        for (Pair<SqlIdentifier, SqlColumnDeclaration> colDef : GeneralUtil.emptyIfNull(sqlCreateTable.getColDefs())) {
            if (isTimestampColumnWithDefault(colDef.getValue())) {
                hasTimestampColumnDefault = true;
                break;
            }
        }
        res.setTimestampColumnDefault(hasTimestampColumnDefault);

        Map<String, String> specialDefaultValues = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, Long> specialDefaultValueFlags = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, Set<String>> genColRefs = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Set<String> allReferencedColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        List<String> allColumns = new ArrayList<>();
        Set<String> autoIncColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        Set<String> autoUpdateColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        Set<String> mysqlGenColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

        for (Pair<SqlIdentifier, SqlColumnDeclaration> colDef : GeneralUtil.emptyIfNull(sqlCreateTable.getColDefs())) {
            String columnName = colDef.getKey().getLastName();
            allColumns.add(columnName);

            if (colDef.getValue().isAutoIncrement()) {
                autoIncColumns.add(columnName);
            }
            if (colDef.getValue().isOnUpdateCurrentTimestamp()) {
                autoUpdateColumns.add(columnName);
            }
            if (colDef.getValue().isGeneratedAlways() && !colDef.getValue().isGeneratedAlwaysLogical()) {
                mysqlGenColumns.add(columnName);
            }

            if (colDef.getValue().isGeneratedAlwaysLogical()) {
                if (colDef.getValue().isAutoIncrement()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        String.format("Generated column [%s] can not be auto_increment column.", colDef.getKey()));
                }
                if (colDef.getValue().isOnUpdateCurrentTimestamp()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        String.format("Generated column [%s] can not be auto update column.", colDef.getKey()));
                }
                if (!GeneratedColumnUtil.supportDataType(colDef.getValue())) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        String.format("Do not support data type of generated column [%s].", colDef.getKey()));
                }

                SqlCall expr = colDef.getValue().getGeneratedAlwaysExpr();
                GeneratedColumnUtil.validateGeneratedColumnExpr(expr);
                genColRefs.put(columnName, GeneratedColumnUtil.getReferencedColumns(expr));
                allReferencedColumns.addAll(genColRefs.get(columnName));
                specialDefaultValues.put(columnName, expr.toString());
                specialDefaultValueFlags.put(columnName, ColumnsRecord.FLAG_LOGICAL_GENERATED_COLUMN);
            } else if (colDef.getValue().isGeneratedAlways()) {
                SqlCall expr = colDef.getValue().getGeneratedAlwaysExpr();
                GeneratedColumnUtil.validateGeneratedColumnExpr(expr);
                specialDefaultValues.put(columnName, expr.toString());
                specialDefaultValueFlags.put(columnName, ColumnsRecord.FLAG_GENERATED_COLUMN);
            } else if (colDef.getValue().getDefaultVal() instanceof SqlBinaryStringLiteral) {
                String hexValue =
                    ((SqlBinaryStringLiteral) colDef.getValue().getDefaultVal()).getBitString().toHexString();
                specialDefaultValues.put(columnName, hexValue);
                specialDefaultValueFlags.put(columnName, ColumnsRecord.FLAG_BINARY_DEFAULT);
            } else if (colDef.getValue().getDefaultExpr() != null && InstanceVersion.isMYSQL80()) {
                if (!DefaultExprUtil.supportDataType(colDef.getValue())) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        String.format("Do not support data type of column `%s` with default expression.",
                            colDef.getKey()));
                }
                SqlCall expr = colDef.getValue().getDefaultExpr();
                DefaultExprUtil.validateColumnExpr(expr);
                specialDefaultValues.put(columnName, expr.toString());
                specialDefaultValueFlags.put(columnName, ColumnsRecord.FLAG_DEFAULT_EXPR);
            }
        }

        for (String referencedColumn : allReferencedColumns) {
            if (allColumns.stream().noneMatch(c -> c.equalsIgnoreCase(referencedColumn))) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    String.format("Referenced column [%s] does not exist", referencedColumn));
            } else if (autoIncColumns.contains(referencedColumn)) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    String.format("Referenced column [%s] can not be auto_increment column.", referencedColumn));
            } else if (autoUpdateColumns.contains(referencedColumn)) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    String.format("Referenced column [%s] can not be auto update column.", referencedColumn));
            } else if (mysqlGenColumns.contains(referencedColumn)) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    String.format("Referenced column [%s] can not be non-logical generated column.", referencedColumn));
            }
        }

        for (Pair<SqlIdentifier, SqlColumnDeclaration> colDef : GeneralUtil.emptyIfNull(sqlCreateTable.getColDefs())) {
            String columnName = colDef.getKey().getLastName();
            if (allReferencedColumns.contains(columnName)) {
                if (!GeneratedColumnUtil.supportDataType(colDef.getValue())) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        String.format("Do not support data type of referenced column [%s].", colDef.getKey()));
                }
            }
        }

        // Check cycle
        GeneratedColumnUtil.getGeneratedColumnEvaluationOrder(genColRefs);

        res.setSpecialDefaultValues(specialDefaultValues);
        res.setSpecialDefaultValueFlags(specialDefaultValueFlags);

        // create table with locality
        LocalityDesc desc = LocalityDesc.parse(sqlCreateTable.getLocality());
        res.setLocality(desc);

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
        SqlCreateTable createTableAst =
            (SqlCreateTable) new FastsqlParser()
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
                indexDef.getTableGroupName(),
                createTablePreparedData.getLocality().toString(),
                ((CreateTable) relDdl).getPartBoundExprInfo(),
                createTablePreparedData.getSourceSql());

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
