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

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateIndexStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.common.utils.GeneralUtil;
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
import java.util.HashSet;
import java.util.List;
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
    private final List<SqlIndexColumnName> columns;
    private final SqlIndexConstraintType constraintType;
    private final SqlIndexResiding indexResiding;
    private final SqlIndexType indexType;
    private final List<SqlIndexOption> options;
    private final SqlIndexAlgorithmType algorithm;
    private final SqlIndexLockType lock;
    private final List<SqlIndexColumnName> covering;
    private final SqlNode dbPartitionBy;
    private final SqlNode dbPartitions = null;
    private final SqlNode tbPartitionBy;
    private final SqlNode tbPartitions;

    private final SqlNode partitioning;
    private final String sourceSql;
    //originalSql is the same as what user input, while sourceSql maybe rewrite
    private String originalSql;
    private String primaryTableDefinition;
    private SqlCreateTable primaryTableNode;
    private final boolean clusteredIndex;
    private final SqlNode tableGroupName;
    private boolean visible = true;

    private SqlCreateIndex(SqlParserPos pos, SqlIdentifier indexName, SqlIdentifier table,
                           List<SqlIndexColumnName> columns, SqlIndexConstraintType constraintType,
                           SqlIndexResiding indexResiding, SqlIndexType indexType, List<SqlIndexOption> options,
                           SqlIndexAlgorithmType algorithm, SqlIndexLockType lock, List<SqlIndexColumnName> covering,
                           SqlNode dbPartitionBy, SqlNode tbPartitionBy, SqlNode tbPartitions, SqlNode partitioning,
                           String sourceSql, boolean clusteredIndex, SqlNode tableGroupName, boolean visible) {
        super(OPERATOR, pos, false, false);
        this.indexName = indexName;
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
        this.dbPartitionBy = dbPartitionBy;
        this.tbPartitionBy = tbPartitionBy;
        this.tbPartitions = tbPartitions;
        this.partitioning = partitioning;
        this.sourceSql = sourceSql;
        this.clusteredIndex = clusteredIndex;
        this.tableGroupName = tableGroupName;
        this.visible = visible;
    }

    public SqlCreateIndex(SqlParserPos pos, boolean replace, boolean ifNotExists, SqlIdentifier originTableName,
                          SqlIdentifier indexName, List<SqlIndexColumnName> columns,
                          SqlIndexConstraintType constraintType, SqlIndexResiding indexResiding,
                          SqlIndexType indexType, List<SqlIndexOption> options, SqlIndexAlgorithmType algorithm,
                          SqlIndexLockType lock, List<SqlIndexColumnName> covering, SqlNode dbPartitionBy,
                          SqlNode tbPartitionBy, SqlNode tbPartitions, SqlNode partitioning,
                          String sourceSql, String primaryTableDefinition,
                          SqlCreateTable primaryTableNode, boolean clusteredIndex,
                          SqlNode tableGroupName, boolean visible) {
        super(OPERATOR, pos, replace, ifNotExists);
        this.originTableName = originTableName;
        this.indexName = indexName;
        this.columns = columns;
        this.constraintType = constraintType;
        this.indexResiding = indexResiding;
        this.indexType = indexType;
        this.options = options;
        this.algorithm = algorithm;
        this.lock = lock;
        this.covering = covering;
        this.dbPartitionBy = dbPartitionBy;
        this.tbPartitionBy = tbPartitionBy;
        this.tbPartitions = tbPartitions;
        this.sourceSql = sourceSql;
        this.primaryTableDefinition = primaryTableDefinition;
        this.primaryTableNode = primaryTableNode;
        this.clusteredIndex = clusteredIndex;
        this.partitioning = partitioning;
        this.tableGroupName = tableGroupName;
        this.visible = visible;
    }

    public static SqlCreateIndex createLocalIndex(SqlIdentifier indexName, SqlIdentifier tableName,
                                                  List<SqlIndexColumnName> columns,
                                                  SqlIndexConstraintType constraintType, boolean explicit,
                                                  SqlIndexType indexType, List<SqlIndexOption> options,
                                                  SqlIndexAlgorithmType algorithm, SqlIndexLockType lock, String sql,
                                                  SqlParserPos pos) {
        return new SqlCreateIndex(pos,
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
            sql,
            false,
            null,
            true);
    }

    public static SqlCreateIndex createGlobalIndex(SqlParserPos pos, SqlIdentifier indexName, SqlIdentifier table,
                                                   List<SqlIndexColumnName> columns,
                                                   SqlIndexConstraintType constraintType, SqlIndexType indexType,
                                                   List<SqlIndexOption> options, SqlIndexAlgorithmType algorithm,
                                                   SqlIndexLockType lock, List<SqlIndexColumnName> covering,
                                                   SqlNode dbPartitionBy, SqlNode tbPartitionBy, SqlNode tbPartitions,
                                                   SqlNode partitioning, String sourceSql, SqlNode tableGroupName,
                                                   boolean visible) {
        return new SqlCreateIndex(pos,
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
            dbPartitionBy,
            tbPartitionBy,
            tbPartitions,
            partitioning,
            sourceSql,
            false,
            tableGroupName,
            visible);
    }

    public static SqlCreateIndex createClusteredIndex(SqlParserPos pos, SqlIdentifier indexName, SqlIdentifier table,
                                                      List<SqlIndexColumnName> columns,
                                                      SqlIndexConstraintType constraintType, SqlIndexType indexType,
                                                      List<SqlIndexOption> options, SqlIndexAlgorithmType algorithm,
                                                      SqlIndexLockType lock, List<SqlIndexColumnName> covering,
                                                      SqlNode dbPartitionBy, SqlNode tbPartitionBy,
                                                      SqlNode tbPartitions,
                                                      SqlNode partitioning, String sourceSql,
                                                      SqlNode tableGroupName, boolean visible) {
        return new SqlCreateIndex(pos,
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
            dbPartitionBy,
            tbPartitionBy,
            tbPartitions,
            partitioning,
            sourceSql,
            true,
            tableGroupName,
            visible);
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
        unparse(writer, leftPrec, rightPrec, this.indexResiding, false);
    }

    @Override
    public boolean createGsi() {
        return SqlUtil.isGlobal(this.indexResiding);
    }

    public boolean createClusteredIndex() {
        return clusteredIndex;
    }

    public void unparse(SqlWriter writer, int leftPrec, int rightPrec, SqlIndexResiding indexResiding,
                        boolean withOriginTableName) {
        final boolean isGlobal = SqlUtil.isGlobal(indexResiding);

        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, "CREATE", "");

        if (null != constraintType) {
            SqlUtil.wrapSqlLiteralSymbol(constraintType).unparse(writer, leftPrec, rightPrec);
        }

        if (isGlobal) {
            SqlUtil.wrapSqlLiteralSymbol(indexResiding).unparse(writer, leftPrec, rightPrec);
        }

        writer.keyword("INDEX");
        indexName.unparse(writer, leftPrec, rightPrec);

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
            if (null != covering && !covering.isEmpty()) {
                writer.keyword("COVERING");
                final Frame frame2 = writer.startList(FrameTypeEnum.FUN_CALL, "(", ")");
                SqlUtil.wrapSqlNodeList(covering).commaList(writer);
                writer.endList(frame2);
            }

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

            if (writer instanceof SqlPrettyWriter) {
                ((SqlPrettyWriter) writer).setQuoteAllIdentifiers(quoteAllIdentifiers);
            }
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
        String sqlForExecute = sourceSql;
        if (SqlUtil.isGlobal(indexResiding)) {
            // generate CREATE INDEX for executing on MySQL
            SqlPrettyWriter writer = new SqlPrettyWriter(MysqlSqlDialect.DEFAULT);
            writer.setAlwaysUseParentheses(true);
            writer.setSelectListItemsOnSeparateLines(false);
            writer.setIndentation(0);
            final int leftPrec = getOperator().getLeftPrec();
            final int rightPrec = getOperator().getRightPrec();
            unparse(writer, leftPrec, rightPrec, indexResiding, true);
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

    public List<SqlIndexColumnName> getColumns() {
        return columns;
    }

    public List<SqlIndexColumnName> getCovering() {
        return covering;
    }

    public SqlNode getTableGroupName() {
        return tableGroupName;
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
            originTableName,
            indexName,
            columns,
            constraintType,
            indexResiding,
            indexType,
            options,
            algorithm,
            lock,
            newCovering,
            dbPartitionBy,
            tbPartitionBy,
            tbPartitions,
            partitioning,
            sourceSql,
            primaryTableDefinition,
            primaryTableNode,
            clusteredIndex,
            tableGroupName,
            this.visible);
    }

    public SqlCreateIndex rebuildToGsi(SqlIdentifier newName, SqlNode dbpartition, boolean clustered) {
        return new SqlCreateIndex(pos,
            null == newName ? indexName : newName,
            originTableName,
            columns,
            constraintType,
            SqlIndexResiding.GLOBAL,
            indexType,
            options,
            algorithm,
            lock,
            clustered ? null : covering,
            null == dbpartition ? dbPartitionBy : dbpartition,
            null == dbpartition ? tbPartitionBy : null,
            null == dbpartition ? tbPartitions : null,
            partitioning,
            sourceSql,
            clustered,
            tableGroupName,
            this.visible);
    }

    public SqlCreateIndex rebuildToGsiNewPartition(SqlIdentifier newName, SqlNode newPartition, boolean clustered) {
        return new SqlCreateIndex(pos,
            null == newName ? indexName : newName,
            originTableName,
            columns,
            constraintType,
            SqlIndexResiding.GLOBAL,
            indexType,
            options,
            algorithm,
            lock,
            clustered ? null : covering,
            null == newPartition ? dbPartitionBy : null,
            null == newPartition ? tbPartitionBy : null,
            null == newPartition ? tbPartitions : null,
            null == newPartition ? partitioning : newPartition,
            sourceSql,
            clustered,
            tableGroupName,
            this.visible);
    }

    public SqlCreateIndex rebuildToExplicitLocal(SqlIdentifier newName, String sql) {
        return new SqlCreateIndex(pos,
            null == newName ? indexName : newName,
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
            null == sql ? sourceSql : sql,
            false,
            tableGroupName,
            this.visible);
    }

    public SqlCreateIndex replaceTableName(SqlIdentifier newTableName) {
        return new SqlCreateIndex(pos,
            indexName,
            null == newTableName ? originTableName : newTableName,
            columns,
            constraintType,
            indexResiding,
            indexType,
            options,
            algorithm,
            lock,
            covering,
            dbPartitionBy,
            tbPartitionBy,
            tbPartitions,
            partitioning,
            sourceSql,
            clusteredIndex,
            tableGroupName,
            this.visible);
    }
}
