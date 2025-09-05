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

package org.apache.calcite.sql;

import com.alibaba.polardbx.common.ColumnarOptions;
import com.alibaba.polardbx.common.columnar.ColumnarOption;
import com.alibaba.polardbx.common.properties.ColumnarConfig;
import com.alibaba.polardbx.common.ColumnarOptions;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateIndexStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import org.apache.calcite.sql.SqlIndexDefinition.SqlIndexResiding;
import org.apache.calcite.sql.SqlIndexDefinition.SqlIndexType;
import org.apache.calcite.sql.SqlWriter.Frame;
import org.apache.calcite.sql.SqlWriter.FrameTypeEnum;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * ${DESCRIPTION}
 *
 * @author hongxi.chx
 */
public class SqlCreateIndex extends SqlCreate {

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("CREATE INDEX", SqlKind.CREATE_INDEX);

    private final SqlIdentifier originTableName;
    private final SqlIdentifier indexName;
    /**
     * Index name without suffix
     */
    private final SqlIdentifier originIndexName;
    private final List<SqlIndexColumnName> columns;
    private final SqlIndexConstraintType constraintType;
    private final SqlIndexResiding indexResiding;
    private final SqlIndexType indexType;
    private final List<SqlIndexOption> options;
    private final SqlIndexAlgorithmType algorithm;
    private final SqlIndexLockType lock;
    private final List<SqlIndexColumnName> covering;
    private final List<SqlIndexColumnName> originCovering;
    private final List<SqlIndexColumnName> clusteredKeys;
    private final SqlNode dbPartitionBy;
    private final SqlNode dbPartitions = null;
    private final SqlNode tbPartitionBy;
    private final SqlNode tbPartitions;

    private final SqlNode partitioning;
    private final SqlNode originPartitioning;
    private final String sourceSql;
    private final boolean clusteredIndex;
    private final boolean columnarIndex;
    private final SqlNode tableGroupName;
    private final SqlNode engineName;
    private final List<SqlIndexColumnName> dictColumns;
    //originalSql is the same as what user input, while sourceSql maybe rewrite
    private String originalSql;
    private String primaryTableDefinition;
    private SqlCreateTable primaryTableNode;
    private final boolean withImplicitTableGroup;
    private boolean visible = true;
    /**
     * May be uppercase or lowercase, defined by user input.
     */
    private final Map<String, String> columnarOptions;

    private SqlCreateIndex(SqlParserPos pos,
                           SqlIdentifier indexName,
                           SqlIdentifier originIndexName,
                           SqlIdentifier table,
                           List<SqlIndexColumnName> columns,
                           SqlIndexConstraintType constraintType,
                           SqlIndexResiding indexResiding,
                           SqlIndexType indexType,
                           List<SqlIndexOption> options,
                           SqlIndexAlgorithmType algorithm,
                           SqlIndexLockType lock,
                           List<SqlIndexColumnName> covering,
                           List<SqlIndexColumnName> originCovering,
                           SqlNode dbPartitionBy,
                           SqlNode tbPartitionBy,
                           SqlNode tbPartitions,
                           SqlNode partitioning,
                           SqlNode originPartitioning,
                           List<SqlIndexColumnName> clusteredKeys,
                           String sourceSql,
                           boolean clusteredIndex,
                           boolean columnarIndex,
                           SqlNode tableGroupName,
                           boolean withImplicitTableGroup,
                           SqlNode engineName,
                           List<SqlIndexColumnName> dictColumns,
                           boolean visible,
                           Map<String, String> columnarOptions) {
        super(OPERATOR, pos, false, false);
        this.indexName = indexName;
        this.originIndexName = originIndexName;
        this.name = table;
        this.originTableName = table;
        this.columns = columns;
        this.constraintType = constraintType;
        this.indexResiding = indexResiding;
        this.indexType = indexType;
        this.options = options;
        this.algorithm = algorithm;
        this.lock = lock;
        this.covering = covering;
        this.originCovering = originCovering;
        this.dbPartitionBy = dbPartitionBy;
        this.tbPartitionBy = tbPartitionBy;
        this.tbPartitions = tbPartitions;
        this.partitioning = partitioning;
        this.originPartitioning = originPartitioning;
        this.clusteredKeys = clusteredKeys;
        this.sourceSql = sourceSql;
        this.clusteredIndex = clusteredIndex;
        this.columnarIndex = columnarIndex;
        this.tableGroupName = tableGroupName;
        this.withImplicitTableGroup = withImplicitTableGroup;
        this.engineName = engineName;
        this.dictColumns = dictColumns;
        this.visible = visible;
        this.columnarOptions = columnarOptions;
    }

    public SqlCreateIndex(SqlParserPos pos,
                          boolean replace,
                          boolean ifNotExists,
                          SqlNode name,
                          SqlIdentifier originTableName,
                          SqlIdentifier indexName,
                          SqlIdentifier originIndexName,
                          List<SqlIndexColumnName> columns,
                          SqlIndexConstraintType constraintType,
                          SqlIndexResiding indexResiding,
                          SqlIndexType indexType,
                          List<SqlIndexOption> options,
                          SqlIndexAlgorithmType algorithm,
                          SqlIndexLockType lock,
                          List<SqlIndexColumnName> covering,
                          List<SqlIndexColumnName> originCovering,
                          SqlNode dbPartitionBy,
                          SqlNode tbPartitionBy,
                          SqlNode tbPartitions,
                          SqlNode partitioning,
                          SqlNode originPartitioning,
                          List<SqlIndexColumnName> clusteredKeys,
                          String sourceSql,
                          String primaryTableDefinition,
                          SqlCreateTable primaryTableNode,
                          boolean clusteredIndex,
                          boolean columnarIndex,
                          SqlNode tableGroupName,
                          boolean withImplicitTableGroup,
                          SqlNode engineName,
                          List<SqlIndexColumnName> dictColumns,
                          boolean visible,
                          Map<String, String> columnarOptions) {
        super(OPERATOR, pos, replace, ifNotExists);
        this.name = name;
        this.originTableName = originTableName;
        this.indexName = indexName;
        this.originIndexName = originIndexName;
        this.columns = columns;
        this.constraintType = constraintType;
        this.indexResiding = indexResiding;
        this.indexType = indexType;
        this.options = options;
        this.algorithm = algorithm;
        this.lock = lock;
        this.covering = covering;
        this.originCovering = originCovering;
        this.dbPartitionBy = dbPartitionBy;
        this.tbPartitionBy = tbPartitionBy;
        this.tbPartitions = tbPartitions;
        this.clusteredKeys = clusteredKeys;
        this.sourceSql = sourceSql;
        this.primaryTableDefinition = primaryTableDefinition;
        this.primaryTableNode = primaryTableNode;
        this.clusteredIndex = clusteredIndex;
        this.columnarIndex = columnarIndex;
        this.partitioning = partitioning;
        this.originPartitioning = originPartitioning;
        this.tableGroupName = tableGroupName;
        this.withImplicitTableGroup = withImplicitTableGroup;
        this.engineName = engineName;
        this.dictColumns = dictColumns;
        this.visible = visible;
        this.columnarOptions = columnarOptions;
    }

    public static SqlCreateIndex createLocalIndex(SqlIdentifier indexName, SqlIdentifier tableName,
                                                  List<SqlIndexColumnName> columns,
                                                  SqlNode tableGroupName,
                                                  boolean withImplicitTableGroup,
                                                  SqlIndexConstraintType constraintType, boolean explicit,
                                                  SqlIndexType indexType, List<SqlIndexOption> options,
                                                  SqlIndexAlgorithmType algorithm, SqlIndexLockType lock, String sql,
                                                  SqlParserPos pos) {
        return new SqlCreateIndex(pos,
            indexName,
            indexName,
            tableName,
            columns,
            constraintType,
            explicit ? SqlIndexResiding.LOCAL : null,
            indexType,
            options,
            algorithm,
            lock,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            sql,
            false,
            false,
            tableGroupName,
            withImplicitTableGroup,
            null,
            null,
            true,
            null);
    }

    public static SqlCreateIndex createGlobalIndex(SqlParserPos pos, SqlIdentifier indexName, SqlIdentifier table,
                                                   List<SqlIndexColumnName> columns,
                                                   SqlIndexConstraintType constraintType, SqlIndexType indexType,
                                                   List<SqlIndexOption> options, SqlIndexAlgorithmType algorithm,
                                                   SqlIndexLockType lock, List<SqlIndexColumnName> covering,
                                                   SqlNode dbPartitionBy, SqlNode tbPartitionBy, SqlNode tbPartitions,
                                                   SqlNode partitioning, String sourceSql, SqlNode tableGroupName,
                                                   boolean withImplicitTableGroup,
                                                   boolean visible) {
        return new SqlCreateIndex(pos,
            indexName,
            indexName,
            table,
            columns,
            constraintType,
            SqlIndexResiding.GLOBAL,
            indexType,
            options,
            algorithm,
            lock,
            covering,
            covering,
            dbPartitionBy,
            tbPartitionBy,
            tbPartitions,
            partitioning,
            partitioning,
            null,
            sourceSql,
            false,
            false,
            tableGroupName,
            withImplicitTableGroup,
            null,
            null,
            visible,
            null);
    }

    public static SqlCreateIndex createClusteredIndex(SqlParserPos pos, SqlIdentifier indexName, SqlIdentifier table,
                                                      List<SqlIndexColumnName> columns,
                                                      SqlIndexConstraintType constraintType, SqlIndexType indexType,
                                                      List<SqlIndexOption> options, SqlIndexAlgorithmType algorithm,
                                                      SqlIndexLockType lock, List<SqlIndexColumnName> covering,
                                                      SqlNode dbPartitionBy, SqlNode tbPartitionBy,
                                                      SqlNode tbPartitions,
                                                      SqlNode partitioning, String sourceSql,
                                                      SqlNode tableGroupName,
                                                      boolean withImplicitTableGroup, boolean visible) {
        return new SqlCreateIndex(pos,
            indexName,
            indexName,
            table,
            columns,
            constraintType,
            SqlIndexResiding.GLOBAL,
            indexType,
            options,
            algorithm,
            lock,
            covering,
            covering,
            dbPartitionBy,
            tbPartitionBy,
            tbPartitions,
            partitioning,
            partitioning,
            null,
            sourceSql,
            true,
            false,
            tableGroupName,
            withImplicitTableGroup,
            null,
            null,
            visible,
            null);
    }

    public static SqlCreateIndex createColumnarIndex(SqlParserPos pos, SqlIdentifier indexName, SqlIdentifier table,
                                                     List<SqlIndexColumnName> columns,
                                                     SqlIndexConstraintType constraintType, SqlIndexType indexType,
                                                     List<SqlIndexOption> options, SqlIndexAlgorithmType algorithm,
                                                     SqlIndexLockType lock, List<SqlIndexColumnName> covering,
                                                     SqlNode dbPartitionBy, SqlNode tbPartitionBy,
                                                     SqlNode tbPartitions,
                                                     SqlNode partitioning, List<SqlIndexColumnName> clusteredKeys,
                                                     String sourceSql,
                                                     SqlNode tableGroupName,
                                                     boolean withImplicitTableGroup,
                                                     SqlNode engineName,
                                                     List<SqlIndexColumnName> dictKeys,
                                                     boolean visible,
                                                     Map<String, String> columnarOptions) {
        return new SqlCreateIndex(pos,
            indexName,
            indexName,
            table,
            columns,
            constraintType,
            SqlIndexResiding.GLOBAL,
            indexType,
            options,
            algorithm,
            lock,
            covering,
            covering,
            dbPartitionBy,
            tbPartitionBy,
            tbPartitions,
            partitioning,
            partitioning,
            clusteredKeys,
            sourceSql,
            true,
            true,
            tableGroupName,
            withImplicitTableGroup,
            engineName,
            dictKeys,
            visible,
            columnarOptions);
    }

    public SqlNode getEngineName() {
        return engineName;
    }

    public List<SqlIndexColumnName> getDictColumns() {
        return dictColumns;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name,
            indexName,
            SqlUtil.wrapSqlNodeList(columns),
            SqlUtil.wrapSqlLiteralSymbol(constraintType),
            SqlUtil.wrapSqlLiteralSymbol(indexResiding),
            SqlUtil.wrapSqlLiteralSymbol(indexType),
            SqlUtil.wrapSqlNodeList(options),
            SqlUtil.wrapSqlLiteralSymbol(algorithm),
            SqlUtil.wrapSqlLiteralSymbol(lock),
            SqlUtil.wrapSqlNodeList(covering),
            dbPartitionBy,
            tbPartitionBy,
            tbPartitions);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        unparse(writer, leftPrec, rightPrec, this.indexResiding, false, false);
    }

    @Override
    public boolean createGsi() {
        return SqlUtil.isGlobal(this.indexResiding);
    }

    public boolean createClusteredIndex() {
        return clusteredIndex;
    }

    @Override
    public boolean createCci() {
        return columnarIndex;
    }

    public void unparse(SqlWriter writer, int leftPrec, int rightPrec, SqlIndexResiding indexResiding,
                        boolean withOriginTableName, boolean withOriginNames) {
        final boolean isGlobal = SqlUtil.isGlobal(indexResiding);

        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, "CREATE", "");

        if (null != constraintType) {
            SqlUtil.wrapSqlLiteralSymbol(constraintType).unparse(writer, leftPrec, rightPrec);
        }

        if (clusteredIndex) {
            writer.keyword("CLUSTERED");
        }

        if (columnarIndex) {
            writer.keyword("COLUMNAR");
        } else if (isGlobal) {
            SqlUtil.wrapSqlLiteralSymbol(indexResiding).unparse(writer, leftPrec, rightPrec);
        }

        writer.keyword("INDEX");
        if (withOriginNames) {
            originIndexName.unparse(writer, leftPrec, rightPrec);
        } else {
            indexName.unparse(writer, leftPrec, rightPrec);
        }

        if (null != indexType) {
            writer.keyword("USING");
            writer.keyword(indexType.name());
        }

        writer.keyword("ON");
        if (withOriginTableName) {
            originTableName.unparse(writer, leftPrec, rightPrec);
        } else {
            name.unparse(writer, leftPrec, rightPrec);
        }

        if (null != columns) {
            final Frame frame1 = writer.startList(FrameTypeEnum.FUN_CALL, "(", ")");
            SqlUtil.wrapSqlNodeList(columns).commaList(writer);
            writer.endList(frame1);
        }

        if (isGlobal) {
            final List<SqlIndexColumnName> coveringToShow = withOriginNames ? originCovering : covering;
            if (null != coveringToShow && !coveringToShow.isEmpty()) {
                writer.keyword("COVERING");
                final Frame frame2 = writer.startList(FrameTypeEnum.FUN_CALL, "(", ")");
                SqlUtil.wrapSqlNodeList(coveringToShow).commaList(writer);
                writer.endList(frame2);
            }
        }

        if (columnarIndex) {
            if (clusteredKeys != null && !clusteredKeys.isEmpty()) {
                writer.keyword("CLUSTERED KEY");
                final Frame frame2 = writer.startList(FrameTypeEnum.FUN_CALL, "(", ")");
                SqlUtil.wrapSqlNodeList(clusteredKeys).commaList(writer);
                writer.endList(frame2);
            }
        }

        if (isGlobal || columnarIndex) {
            final boolean quoteAllIdentifiers = writer.isQuoteAllIdentifiers();
            if (writer instanceof SqlPrettyWriter) {
                ((SqlPrettyWriter) writer).setQuoteAllIdentifiers(false);
            }

            if (null != dbPartitionBy) {
                writer.keyword("DBPARTITION BY");
                dbPartitionBy.unparse(writer, leftPrec, rightPrec);
            }

            if (null != tbPartitionBy) {
                writer.keyword("TBPARTITION BY");
                tbPartitionBy.unparse(writer, leftPrec, rightPrec);
            }

            if (null != tbPartitions) {
                writer.keyword("TBPARTITIONS");
                tbPartitions.unparse(writer, leftPrec, rightPrec);
            }

            final SqlNode partitioningToShow = withOriginNames ? originPartitioning : partitioning;
            if (null != partitioningToShow) {
                writer.print(" ");
                partitioningToShow.unparse(writer, leftPrec, rightPrec);
            }

            if (writer instanceof SqlPrettyWriter) {
                ((SqlPrettyWriter) writer).setQuoteAllIdentifiers(quoteAllIdentifiers);
            }
        }

        if (columnarIndex && null != engineName) {
            writer.keyword("ENGINE");
            writer.keyword("=");
            engineName.unparse(writer, leftPrec, rightPrec);
        }

        if (null != options) {
            for (SqlIndexOption option : options) {
                option.unparse(writer, leftPrec, rightPrec);
            }
        }

        if (null != algorithm) {
            writer.keyword("ALGORITHM");
            SqlUtil.wrapSqlLiteralSymbol(algorithm).unparse(writer, leftPrec, rightPrec);
        }

        if (null != lock) {
            writer.keyword("LOCK");
            SqlUtil.wrapSqlLiteralSymbol(lock).unparse(writer, leftPrec, rightPrec);
        }

        writer.endList(frame);
    }

    public boolean isVisible() {
        return visible;
    }

    public void setVisible(boolean visible) {
        this.visible = visible;
    }

    public void setTargetTable(SqlIdentifier sqlIdentifier) {
        this.name = sqlIdentifier;
    }

    private String prepare() {
        return prepare(false);
    }

    private String prepare(boolean withOriginNames) {
        String sqlForExecute = sourceSql;
        if (SqlUtil.isGlobal(indexResiding)) {
            // generate CREATE INDEX for executing on MySQL
            SqlPrettyWriter writer = new SqlPrettyWriter(MysqlSqlDialect.DEFAULT);
            writer.setAlwaysUseParentheses(true);
            writer.setSelectListItemsOnSeparateLines(false);
            writer.setIndentation(0);
            final int leftPrec = getOperator().getLeftPrec();
            final int rightPrec = getOperator().getRightPrec();
            unparse(writer, leftPrec, rightPrec, indexResiding, true, withOriginNames);
            sqlForExecute = writer.toSqlString().getSql();
        }

        List<SQLStatement> statementList =
            SQLUtils.parseStatementsWithDefaultFeatures(sqlForExecute, JdbcConstants.MYSQL);
        SQLCreateIndexStatement stmt = (SQLCreateIndexStatement) statementList.get(0);
        Set<String> shardKeys = new HashSet<>();
        if (this.name instanceof SqlDynamicParam) {
            StringBuilder sql = new StringBuilder();
            MySqlOutputVisitor questionarkTableSource = new MySqlOutputVisitor(sql) {
                @Override
                protected void printTableSourceExpr(SQLExpr expr) {
                    print("?");
                }
            };
            questionarkTableSource.visit(stmt);
            return sql.toString();
        }
        return stmt.toString();
    }

    public MySqlStatement rewrite() {
        List<SQLStatement> statementList = SQLUtils.parseStatementsWithDefaultFeatures(sourceSql, JdbcConstants.MYSQL);
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) statementList.get(0);
        return stmt;
    }

    @Override
    public String toString() {
        return prepare();
    }

    @Override
    public SqlString toSqlString(SqlDialect dialect) {
        String sql = prepare();
        return new SqlString(dialect, sql);
    }

    public String toString(boolean withOriginNames) {
        return prepare(withOriginNames);
    }

    public SqlNode getDbPartitionBy() {
        return dbPartitionBy;
    }

    public SqlNode getDbPartitions() {
        return dbPartitions;
    }

    public SqlNode getTbPartitionBy() {
        return tbPartitionBy;
    }

    public SqlNode getTbPartitions() {
        return tbPartitions;
    }

    public SqlIdentifier getIndexName() {
        return indexName;
    }

    public SqlIdentifier getOriginIndexName() {
        return originIndexName;
    }

    public List<SqlIndexColumnName> getColumns() {
        return columns;
    }

    public List<SqlIndexColumnName> getCovering() {
        return covering;
    }

    public List<SqlIndexColumnName> getOriginCovering() {
        return originCovering;
    }

    public List<SqlIndexColumnName> getClusteredKeys() {
        return clusteredKeys;
    }

    public SqlNode getTableGroupName() {
        return tableGroupName;
    }

    public String getPrimaryTableDefinition() {
        return primaryTableDefinition;
    }

    public void setPrimaryTableDefinition(String primaryTableDefinition) {
        this.primaryTableDefinition = primaryTableDefinition;
    }

    public SqlCreateTable getPrimaryTableNode() {
        return primaryTableNode;
    }

    public void setPrimaryTableNode(SqlCreateTable primaryTableNode) {
        this.primaryTableNode = primaryTableNode;
    }

    public SqlIdentifier getOriginTableName() {
        return originTableName;
    }

    public SqlIndexConstraintType getConstraintType() {
        return constraintType;
    }

    public SqlIndexResiding getIndexResiding() {
        return indexResiding;
    }

    public SqlIndexType getIndexType() {
        return indexType;
    }

    public List<SqlIndexOption> getOptions() {
        return options;
    }

    public SqlIndexAlgorithmType getAlgorithm() {
        return algorithm;
    }

    public SqlIndexLockType getLock() {
        return lock;
    }

    public String getSourceSql() {
        return sourceSql;
    }

    public String getOriginalSql() {
        return originalSql;
    }

    public void setOriginalSql(String originalSql) {
        this.originalSql = originalSql;
    }

    public SqlNode getPartitioning() {
        return partitioning;
    }

    public SqlNode getOriginPartitioning() {
        return originPartitioning;
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        return new SqlCreateIndex(pos,
            replace,
            ifNotExists,
            name,
            originTableName,
            indexName,
            originIndexName,
            columns,
            constraintType,
            indexResiding,
            indexType,
            options,
            algorithm,
            lock,
            covering,
            originCovering,
            dbPartitionBy,
            tbPartitionBy,
            tbPartitions,
            partitioning,
            originPartitioning,
            clusteredKeys,
            sourceSql,
            primaryTableDefinition,
            primaryTableNode,
            clusteredIndex,
            columnarIndex,
            tableGroupName,
            withImplicitTableGroup,
            engineName,
            dictColumns,
            visible,
            columnarOptions);
    }

    public SqlCreateIndex rebuildCovering(Collection<String> coveringColumns) {
        if (GeneralUtil.isEmpty(coveringColumns)) {
            return this;
        }

        final List<SqlIndexColumnName> newCovering = coveringColumns.stream().map(
            s -> new SqlIndexColumnName(SqlParserPos.ZERO, new SqlIdentifier(s,
                SqlParserPos.ZERO), null, null)).collect(Collectors.toList());

        return new SqlCreateIndex(pos,
            replace,
            ifNotExists,
            name,
            originTableName,
            indexName,
            originIndexName,
            columns,
            constraintType,
            indexResiding,
            indexType,
            options,
            algorithm,
            lock,
            newCovering,
            originCovering,
            dbPartitionBy,
            tbPartitionBy,
            tbPartitions,
            partitioning,
            originPartitioning,
            clusteredKeys,
            sourceSql,
            primaryTableDefinition,
            primaryTableNode,
            clusteredIndex,
            columnarIndex,
            tableGroupName,
            withImplicitTableGroup,
            engineName,
            dictColumns,
            this.visible,
            columnarOptions);
    }

    /**
     * Rebuild gsi definition with new index name and full partition definition
     *
     * @param newName New index name, with random suffix
     * @param dbPartition Update with full partition definition, with DBPARTITION BY appended
     * @return Copied SqlIndexDefinition
     */
    public SqlCreateIndex rebuildToGsi(SqlIdentifier newName, SqlNode dbPartition) {
        return new SqlCreateIndex(pos,
            null == newName ? indexName : newName,
            originIndexName,
            originTableName,
            columns,
            constraintType,
            SqlIndexResiding.GLOBAL,
            indexType,
            options,
            algorithm,
            lock,
            covering,
            originCovering,
            null == dbPartition ? dbPartitionBy : dbPartition,
            null == dbPartition ? tbPartitionBy : null,
            null == dbPartition ? tbPartitions : null,
            partitioning,
            originPartitioning,
            columnarIndex ? clusteredKeys : null,
            sourceSql,
            clusteredIndex,
            columnarIndex,
            tableGroupName,
            withImplicitTableGroup,
            engineName,
            dictColumns,
            this.visible,
            columnarOptions);
    }

    /**
     * Rebuild gsi definition with new index name and full partition definition
     *
     * @param newName New index name, with random suffix
     * @param newPartition Update with full partition definition, with PARTITION BY/PARTITIONS appended
     * @return Copied SqlIndexDefinition
     */
    public SqlCreateIndex rebuildToGsiNewPartition(SqlIdentifier newName, SqlNode newPartition) {
        return new SqlCreateIndex(pos,
            null == newName ? indexName : newName,
            originIndexName,
            originTableName,
            columns,
            constraintType,
            SqlIndexResiding.GLOBAL,
            indexType,
            options,
            algorithm,
            lock,
            covering,
            originCovering,
            null == newPartition ? dbPartitionBy : null,
            null == newPartition ? tbPartitionBy : null,
            null == newPartition ? tbPartitions : null,
            null == newPartition ? partitioning : newPartition,
            originPartitioning,
            columnarIndex ? clusteredKeys : null,
            sourceSql,
            clusteredIndex,
            columnarIndex,
            tableGroupName,
            withImplicitTableGroup,
            engineName,
            dictColumns,
            this.visible,
            columnarOptions);
    }

    public SqlCreateIndex rebuildToExplicitLocal(SqlIdentifier newName, String sql) {
        return new SqlCreateIndex(pos,
            null == newName ? indexName : newName,
            originIndexName,
            originTableName,
            columns,
            constraintType,
            SqlIndexResiding.LOCAL,
            indexType,
            options,
            algorithm,
            lock,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null == sql ? sourceSql : sql,
            false,
            false,
            tableGroupName,
            withImplicitTableGroup,
            engineName,
            dictColumns,
            this.visible,
            columnarOptions);
    }

    public SqlCreateIndex replaceTableName(SqlIdentifier newTableName) {
        return new SqlCreateIndex(pos,
            indexName,
            originIndexName,
            null == newTableName ? originTableName : newTableName,
            columns,
            constraintType,
            indexResiding,
            indexType,
            options,
            algorithm,
            lock,
            covering,
            originCovering,
            dbPartitionBy,
            tbPartitionBy,
            tbPartitions,
            partitioning,
            originPartitioning,
            clusteredKeys,
            sourceSql,
            clusteredIndex,
            columnarIndex,
            tableGroupName,
            withImplicitTableGroup,
            engineName,
            dictColumns,
            this.visible,
            columnarOptions);
    }

    public SqlCreateIndex replaceIndexName(SqlIdentifier newIndexName) {
        return new SqlCreateIndex(pos,
            null == newIndexName ? indexName : newIndexName,
            originIndexName,
            originTableName,
            columns,
            constraintType,
            indexResiding,
            indexType,
            options,
            algorithm,
            lock,
            covering,
            originCovering,
            dbPartitionBy,
            tbPartitionBy,
            tbPartitions,
            partitioning,
            originPartitioning,
            clusteredKeys,
            sourceSql,
            clusteredIndex,
            columnarIndex,
            tableGroupName,
            withImplicitTableGroup,
            engineName,
            dictColumns,
            this.visible,
            columnarOptions);
    }

    /**
     * columnar index options
     */
    public Map<String, String> getColumnarOptions() {
        // Origin map may be uppercase or lowercase, defined by user input.
        Map<String, String> options = new HashMap<>();
        for (Map.Entry<String, String> kv : columnarOptions.entrySet()) {
            // Normalize key and value.
            String key = kv.getKey();
            String value = kv.getValue();
            ColumnarOption option;
            if (null != (option = ColumnarConfig.get(key.toUpperCase()))) {
                switch (option.getCaseSensitive()) {
                case LOWERCASE_KEY:
                    key = key.toLowerCase();
                    break;
                case LOWERCASE_KEY_LOWERCASE_VALUE:
                    key = key.toLowerCase();
                    value = value.toLowerCase();
                    break;
                case UPPERCASE_KEY:
                    key = key.toUpperCase();
                    break;
                case UPPERCASE_KEY_UPPERCASE_VALUE:
                    key = key.toUpperCase();
                    value = value.toUpperCase();
                    break;
                case DEFAULT:
                default:
                }
            }
            options.put(key, value);
        }

        // Normalize dict columns.
        String dictColumnStr = options.get(ColumnarOptions.DICTIONARY_COLUMNS);
        if (null != dictColumnStr) {
            dictColumnStr = SQLUtils.splitNamesByComma(dictColumnStr).stream()
                .map(SqlIdentifier::surroundWithBacktick)
                .collect(Collectors.joining(","));
            options.put(ColumnarOptions.DICTIONARY_COLUMNS, dictColumnStr);
        }

        return options;
    }

    public static enum SqlIndexConstraintType {
        UNIQUE, FULLTEXT, SPATIAL;

    }

    public static enum SqlIndexAlgorithmType {
        DEFAULT, INPLACE, COPY;
    }

    public static enum SqlIndexLockType {
        DEFAULT, NONE, SHARED, EXCLUSIVE;
    }

    public boolean isWithImplicitTableGroup() {
        return withImplicitTableGroup;
    }
}
