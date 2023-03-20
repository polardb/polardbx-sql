/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql;

import com.alibaba.polardbx.common.ArchiveMode;
import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLCommentHint;
import com.alibaba.polardbx.druid.sql.ast.SQLDataTypeImpl;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLIndexDefinition;
import com.alibaba.polardbx.druid.sql.ast.SQLOrderingSpecification;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionByRange;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLHexExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnPrimaryKey;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnUniqueKey;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLNotNullConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlUnique;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MysqlForeignKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.rule.MappingRule;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql2rel.InitializerExpressionFactory;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang.StringUtils;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.TddlConstants.IMPLICIT_COL_NAME;
import static com.alibaba.polardbx.common.TddlConstants.UGSI_PK_INDEX_NAME;

/**
 * Parse tree for {@code CREATE TABLE} statement.
 */
public class SqlCreateTable extends SqlCreate {

    private final SqlNodeList columnList;
    private final SqlNode query;
    private final SqlNode likeTableName;
    private SqlNode dbpartitionBy; // May changed by validator.
    private final SqlNode dbpartitions;
    private final SqlNode tbpartitionBy;
    private final SqlNode tbpartitions;
    private boolean autoPartition = false;
    private boolean broadcast;
    private boolean single = false;
    //sourceSql maybe change
    private String sourceSql;
    //originalSql is the same as what user input
    private String originalSql;
    private SequenceBean autoIncrement;

    private List<MappingRule> mappingRules;
    private boolean uniqueShardingKey = false;

    private boolean shadow;

    private boolean temporary;

    private List<Pair<SqlIdentifier, SqlColumnDeclaration>> colDefs;
    private SqlIndexDefinition primaryKey;
    private List<Pair<SqlIdentifier, SqlIndexDefinition>> uniqueKeys;
    private List<Pair<SqlIdentifier, SqlIndexDefinition>> globalKeys;
    private List<Pair<SqlIdentifier, SqlIndexDefinition>> globalUniqueKeys;
    private List<Pair<SqlIdentifier, SqlIndexDefinition>> clusteredKeys;
    private List<Pair<SqlIdentifier, SqlIndexDefinition>> clusteredUniqueKeys;
    private List<Pair<SqlIdentifier, SqlIndexDefinition>> keys;
    private List<Pair<SqlIdentifier, SqlIndexDefinition>> fullTextKeys;
    private List<Pair<SqlIdentifier, SqlIndexDefinition>> spatialKeys;
    private List<Pair<SqlIdentifier, SqlIndexDefinition>> foreignKeys;
    private List<SqlCall> checks;

    private List<String> logicalReferencedTables = null;
    private List<String> physicalReferencedTables = null;

    /**
     * 应该只有一个CONSTRAINT for primary key
     */
    private SqlIdentifier primaryKeyConstraint = null;
    private boolean hasPrimaryKeyConstraint = false;

    // default character set for create table
    private String defaultCharset = null;

    // default collation for create table
    private String defaultCollation = null;

    private String locality = "";

    // auto-split for partition-table
    private boolean autoSplit;

    // row format for create table
    private String rowFormat = null;

    private String comment = null;

    // for PolarDb-X partition management
    private SqlNode sqlPartition = null;
    private SqlNode localPartition = null;

    private SqlNode tableGroupName = null;
    private SqlNode joinGroupName = null;

    private SQLPartitionByRange localPartitionSuffix;

    private Engine engine = null;
    private ArchiveMode archiveMode;
    private String loadTableSchema = null;
    public String getLoadTableSchema() {
        return loadTableSchema;
    }
    public void setLoadTableSchema(String loadTableSchema) {
        this.loadTableSchema = loadTableSchema;
    }
    private String loadTableName = null;
    public String getLoadTableName() {
        return loadTableName;
    }
    public void setLoadTableName(String loadTableName) {
        this.loadTableName = loadTableName;
    }
    public Engine getEngine() {
        return engine;
    }
    public void setEngine(Engine engine) {
        this.engine = engine;
    }
    public void setArchiveMode(ArchiveMode archiveMode) {
        this.archiveMode = archiveMode;
    }
    public ArchiveMode getArchiveMode() {
        return this.archiveMode;
    }

    public void setDefaultCharset(String dc) {
        defaultCharset = dc;
    }

    public String getDefaultCharset() {
        return defaultCharset;
    }

    public String getDefaultCollation() {
        return defaultCollation;
    }

    public void setDefaultCollation(String defaultCollation) {
        this.defaultCollation = defaultCollation;
    }

    public void setRowFormat(String rf) {
        rowFormat = rf;
    }

    public String getRowFormat() {
        return rowFormat;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public SequenceBean getAutoIncrement() {
        return autoIncrement;
    }

    public void setAutoIncrement(SequenceBean autoIncrement) {
        this.autoIncrement = autoIncrement;
    }

    public SQLPartitionByRange getLocalPartitionSuffix() {
        return this.localPartitionSuffix;
    }

    public void setLocalPartitionSuffix(final SQLPartitionByRange localPartitionSuffix) {
        this.localPartitionSuffix = localPartitionSuffix;
    }

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("CREATE TABLE", SqlKind.CREATE_TABLE);

    /**
     * Creates a SqlCreateTable.
     */
    SqlCreateTable(SqlParserPos pos, boolean replace, boolean ifNotExists, SqlNode name, SqlNode likeTableName,
                   SqlNodeList columnList, SqlNode query, SqlNode dbpartitionBy, SqlNode dbpartitions,
                   SqlNode tbpartitionBy, SqlNode tbpartitions, String sourceSql, boolean broadcast,
                   SequenceBean autoIncrement, SqlNode sqlPartition, SqlNode localPartition, SqlNode tableGroupName,
                   SqlNode joinGroupName,
                   SQLPartitionByRange localPartitionSuffix) {
        super(OPERATOR, pos, replace, ifNotExists);
        this.name = Preconditions.checkNotNull(name);
        this.likeTableName = likeTableName;
        this.columnList = columnList; // may be null
        this.query = query; // for "CREATE TABLE ... AS query"; may be null
        this.dbpartitionBy = dbpartitionBy;
        this.tbpartitionBy = tbpartitionBy;
        this.dbpartitions = dbpartitions;
        this.tbpartitions = tbpartitions;
        this.sourceSql = sourceSql;
        this.broadcast = broadcast;
        this.autoIncrement = autoIncrement;
        this.sqlPartition = sqlPartition;
        this.localPartition = localPartition;
        this.tableGroupName = tableGroupName;
        this.joinGroupName = joinGroupName;
        this.localPartitionSuffix = localPartitionSuffix;
    }

    public SqlCreateTable(SqlParserPos pos, boolean replace, boolean ifNotExists, SqlNode name, SqlNode likeTableName,
                          SqlNodeList columnList, SqlNode query, SqlNode dbpartitionBy, SqlNode dbpartitions,
                          SqlNode tbpartitionBy, SqlNode tbpartitions, boolean autoPartition, boolean broadcast,
                          boolean single, String sourceSql, SequenceBean autoIncrement, boolean shadow,
                          boolean temporary, List<Pair<SqlIdentifier, SqlColumnDeclaration>> colDefs,
                          SqlIndexDefinition primaryKey, List<Pair<SqlIdentifier, SqlIndexDefinition>> uniqueKeys,
                          List<Pair<SqlIdentifier, SqlIndexDefinition>> globalKeys,
                          List<Pair<SqlIdentifier, SqlIndexDefinition>> globalUniqueKeys,
                          List<Pair<SqlIdentifier, SqlIndexDefinition>> keys,
                          List<Pair<SqlIdentifier, SqlIndexDefinition>> fullTextKeys,
                          List<Pair<SqlIdentifier, SqlIndexDefinition>> spatialKeys,
                          List<Pair<SqlIdentifier, SqlIndexDefinition>> foreignKeys, List<SqlCall> checks,
                          SqlIdentifier primaryKeyConstraint, boolean hasPrimaryKeyConstraint, SqlNode sqlPartition,
                          SqlNode localPartition,
                          SqlNode tableGroupName,
                          SqlNode joinGroupName) {
        super(OPERATOR, pos, replace, ifNotExists);
        this.name = name;
        this.likeTableName = likeTableName;
        this.columnList = columnList;
        this.query = query;
        this.dbpartitionBy = dbpartitionBy;
        this.dbpartitions = dbpartitions;
        this.tbpartitionBy = tbpartitionBy;
        this.tbpartitions = tbpartitions;
        this.autoPartition = autoPartition;
        this.broadcast = broadcast;
        this.single = single;
        this.sourceSql = sourceSql;
        this.autoIncrement = autoIncrement;
        this.shadow = shadow;
        this.temporary = temporary;
        this.colDefs = colDefs;
        this.primaryKey = primaryKey;
        this.uniqueKeys = uniqueKeys;
        this.globalKeys = globalKeys;
        this.globalUniqueKeys = globalUniqueKeys;
        this.keys = keys;
        this.fullTextKeys = fullTextKeys;
        this.spatialKeys = spatialKeys;
        this.foreignKeys = foreignKeys;
        this.checks = checks;
        this.primaryKeyConstraint = primaryKeyConstraint;
        this.hasPrimaryKeyConstraint = hasPrimaryKeyConstraint;
        this.sqlPartition = sqlPartition;
        this.localPartition = localPartition;
        this.tableGroupName = tableGroupName;
        this.joinGroupName = joinGroupName;
    }

    public boolean shouldLoad() {
        if (!Engine.isFileStore(engine)) {
            return false;
        }
        if (archiveMode != null) {
            return this.archiveMode == ArchiveMode.LOADING;
        }
        if (comment != null) {
            // compatible to old syntax
            String trimmedComment = StringUtils.strip(comment, "'");
            return trimmedComment.equalsIgnoreCase("load_oss")
                || trimmedComment.equalsIgnoreCase("load_s3")
                || trimmedComment.equalsIgnoreCase("load_local_disk")
                || trimmedComment.equalsIgnoreCase("load_nfs")
                || trimmedComment.equalsIgnoreCase("load_external_disk");
        }
        return false;
    }
    public boolean shouldBind() {
        return Engine.isFileStore(engine) && archiveMode == ArchiveMode.TTL;
    }

    public SqlNode getLikeTableName() {
        return likeTableName;
    }

    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, columnList, query);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        writer.keyword("TABLE");
        if (ifNotExists) {
            writer.keyword("IF NOT EXISTS");
        }
        name.unparse(writer, leftPrec, rightPrec);
        if (columnList != null) {
            SqlWriter.Frame frame = writer.startList("(", ")");
            for (SqlNode c : columnList) {
                writer.sep(",");
                c.unparse(writer, 0, 0);
            }
            writer.endList(frame);
        }
        if (query != null) {
            writer.keyword("AS");
            writer.newlineAndIndent();
            query.unparse(writer, 0, 0);
        }

        if (dbpartitionBy != null) {
            writer.keyword("DBPARTITION BY");
            dbpartitionBy.unparse(writer, 0, 0);
            if (dbpartitions != null) {
                writer.keyword("DBPARTITIONS");
                dbpartitions.unparse(writer, 0, 0);
            }
        }

        if (tbpartitionBy != null) {
            writer.keyword("TBPARTITION BY");
            tbpartitionBy.unparse(writer, 0, 0);
            if (tbpartitions != null) {
                writer.keyword("TBPARTITIONS");
                tbpartitions.unparse(writer, 0, 0);
            }
        }

    }

    public boolean isBroadCast() {
        return broadcast;
    }

    public List<MappingRule> getMappingRules() {
        return mappingRules;
    }

    public void setMappingRules(List<MappingRule> mappingRules) {
        this.mappingRules = mappingRules;
    }

    public void setSqlPartition(SqlNode sqlPartition) {
        this.sqlPartition = sqlPartition;
    }

    public SqlNode getLocalPartition() {
        return this.localPartition;
    }

    public void setLocalPartition(final SqlNode localPartition) {
        this.localPartition = localPartition;
    }

    public SqlNode getTableGroupName() {
        return tableGroupName;
    }

    public void setTableGroupName(SqlNode tableGroupName) {
        this.tableGroupName = tableGroupName;
    }

    public SqlNode getJoinGroupName() {
        return joinGroupName;
    }

    public void setJoinGroupName(SqlNode joinGroupName) {
        this.joinGroupName = joinGroupName;
    }

    /**
     * Column definition.
     */
    private static class ColumnDef {

        final SqlNode expr;
        final RelDataType type;
        final ColumnStrategy strategy;

        private ColumnDef(SqlNode expr, RelDataType type, ColumnStrategy strategy) {
            this.expr = expr;
            this.type = type;
            this.strategy = Preconditions.checkNotNull(strategy);
            Preconditions.checkArgument(strategy == ColumnStrategy.NULLABLE || strategy == ColumnStrategy.NOT_NULLABLE
                || expr != null);
        }

        static ColumnDef of(SqlNode expr, RelDataType type, ColumnStrategy strategy) {
            return new ColumnDef(expr, type, strategy);
        }
    }

    /**
     * Abstract base class for implementations of {@link ModifiableTable}.
     */
    abstract static class AbstractModifiableTable extends AbstractTable implements ModifiableTable {

        AbstractModifiableTable(String tableName) {
            super();
        }

        public TableModify toModificationRel(RelOptCluster cluster, RelOptTable table,
                                             Prepare.CatalogReader catalogReader, RelNode child,
                                             TableModify.Operation operation, List<String> updateColumnList,
                                             List<RexNode> sourceExpressionList, boolean flattened) {
            return LogicalTableModify.create(table,
                catalogReader,
                child,
                operation,
                updateColumnList,
                sourceExpressionList,
                flattened);
        }
    }

    /**
     * Table backed by a Java list.
     */
    static class MutableArrayTable extends AbstractModifiableTable implements Wrapper {

        final List rows = new ArrayList();
        private final RelProtoDataType protoStoredRowType;
        private final RelProtoDataType protoRowType;
        private final InitializerExpressionFactory initializerExpressionFactory;

        /**
         * Creates a MutableArrayTable.
         *
         * @param name Name of table within its schema
         * @param protoStoredRowType Prototype of row type of stored columns
         * (all columns except virtual columns)
         * @param protoRowType Prototype of row type (all columns)
         * @param initializerExpressionFactory How columns are populated
         */
        MutableArrayTable(String name, RelProtoDataType protoStoredRowType, RelProtoDataType protoRowType,
                          InitializerExpressionFactory initializerExpressionFactory) {
            super(name);
            this.protoStoredRowType = Preconditions.checkNotNull(protoStoredRowType);
            this.protoRowType = Preconditions.checkNotNull(protoRowType);
            this.initializerExpressionFactory = Preconditions.checkNotNull(initializerExpressionFactory);
        }

        public Collection getModifiableCollection() {
            return rows;
        }

        public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
            return new AbstractTableQueryable<T>(queryProvider, schema, this, tableName) {

                public Enumerator<T> enumerator() {
                    // noinspection unchecked
                    return (Enumerator<T>) Linq4j.enumerator(rows);
                }
            };
        }

        public Type getElementType() {
            return Object[].class;
        }

        public Expression getExpression(SchemaPlus schema, String tableName, Class clazz) {
            return Schemas.tableExpression(schema, getElementType(), tableName, clazz);
        }

        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return protoRowType.apply(typeFactory);
        }

        @Override
        public <C> C unwrap(Class<C> aClass) {
            if (aClass.isInstance(initializerExpressionFactory)) {
                return aClass.cast(initializerExpressionFactory);
            }
            return super.unwrap(aClass);
        }
    }

    public SqlNodeList getColumnList() {
        return columnList;
    }

    public SqlNode getDbpartitionBy() {
        return dbpartitionBy;
    }

    public void setDbpartitionBy(SqlNode dbpartitionBy) {
        this.dbpartitionBy = dbpartitionBy;
    }

    public SqlNode getDbpartitions() {
        return dbpartitions;
    }

    public SqlNode getTbpartitionBy() {
        return tbpartitionBy;
    }

    public SqlNode getTbpartitions() {
        return tbpartitions;
    }

    public boolean isAutoPartition() {
        return autoPartition;
    }

    public void setAutoPartition(boolean autoPartition) {
        this.autoPartition = autoPartition;
    }

    public boolean isSingle() {
        return single;
    }

    public void setSingle(boolean single) {
        this.single = single;
    }

    public SqlNode getQuery() {
        return query;
    }

    public String getSourceSql() {
        return sourceSql;
    }

    public void setSourceSql(String sourceSql) {
        this.sourceSql = sourceSql;
    }

    public String getOriginalSql() {
        return originalSql;
    }

    public void setOriginalSql(String originalSql) {
        this.originalSql = originalSql;
    }

    public String getLocality() {
        return this.locality;
    }

    public void setLocality(String locality) {
        this.locality = locality;
    }

    public void setAutoSplit(boolean autoSplit) {
        this.autoSplit = autoSplit;
    }

    public boolean isAutoSplit() {
        return this.autoSplit;
    }

    @Override
    public SqlCreateTable clone(SqlParserPos pos) {
        return new SqlCreateTable(SqlParserPos.ZERO,
            replace,
            ifNotExists,
            name,
            likeTableName,
            columnList,
            query,
            dbpartitionBy,
            dbpartitions,
            tbpartitionBy,
            tbpartitions,
            autoPartition,
            broadcast,
            single,
            sourceSql,
            autoIncrement,
            shadow,
            temporary,
            colDefs,
            primaryKey,
            uniqueKeys,
            globalKeys,
            globalUniqueKeys,
            keys,
            fullTextKeys,
            spatialKeys,
            foreignKeys,
            checks,
            primaryKeyConstraint,
            hasPrimaryKeyConstraint,
            sqlPartition,
            localPartition,
            tableGroupName,
            joinGroupName);
    }

    public MySqlStatement rewriteForGsi() {
        final List<SQLStatement> statementList = SQLUtils.parseStatements(sourceSql, JdbcConstants.MYSQL);
        final MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) statementList.get(0);
        Set<String> shardKeys = new LinkedHashSet<>();
        if (dbpartitionBy != null) {
            shardKeys = getShardingKeys(dbpartitionBy, shardKeys);
        }

        if (tbpartitionBy != null) {
            getShardingKeys(tbpartitionBy, shardKeys);
        }
        if (shardKeys.size() > 0) {
            // Patch for implicit pk if needed.
            colDefs.stream()
                .filter(pair -> pair.left.getLastName().equalsIgnoreCase(IMPLICIT_COL_NAME))
                .map(pair -> pair.left.getLastName())
                .forEach(name -> {
                    // Fake one if not exists in col list.
                    if (stmt.getColumnDefinitions().stream()
                        .anyMatch(
                            col -> SQLUtils.normalizeNoTrim(col.getName().getSimpleName()).equalsIgnoreCase(name))) {
                        return;
                    }
                    final SQLColumnDefinition sqlColumnDefinition = new SQLColumnDefinition();
                    sqlColumnDefinition.setDbType(DbType.mysql);
                    sqlColumnDefinition.setName(name);
                    final SQLDataTypeImpl sqlDataType = new SQLDataTypeImpl("bigint");
                    sqlDataType.addArgument(new SQLIntegerExpr(20));
                    sqlColumnDefinition.setDataType(sqlDataType);
                    sqlColumnDefinition.addConstraint(new SQLNotNullConstraint());
                    sqlColumnDefinition.addConstraint(new SQLColumnPrimaryKey());
                    stmt.addColumn(sqlColumnDefinition);
                });

            addIndex(shardKeys, stmt, uniqueShardingKey);
        } else if (sqlPartition != null) {
            // Patch for implicit pk if needed.
            colDefs.stream()
                .filter(pair -> pair.left.getLastName().equalsIgnoreCase(IMPLICIT_COL_NAME))
                .forEach(pair -> {
                    // Fake one if not exists in col list.
                    if (stmt.getColumnDefinitions().stream()
                        .anyMatch(col -> SQLUtils.normalizeNoTrim(col.getName().getSimpleName())
                            .equalsIgnoreCase(pair.left.getLastName()))) {
                        return;
                    }
                    final SQLColumnDefinition sqlColumnDefinition = new SQLColumnDefinition();
                    sqlColumnDefinition.setDbType(DbType.mysql);
                    sqlColumnDefinition.setName(pair.left.getLastName());
                    final SQLDataTypeImpl sqlDataType = new SQLDataTypeImpl("bigint");
                    sqlDataType.addArgument(new SQLIntegerExpr(20));
                    sqlColumnDefinition.setDataType(sqlDataType);
                    sqlColumnDefinition.addConstraint(new SQLNotNullConstraint());
                    sqlColumnDefinition.addConstraint(new SQLColumnPrimaryKey());
                    stmt.addColumn(sqlColumnDefinition);
                });
        }
        if (localPartitionSuffix != null) {
            addLocalPartitionSuffix(stmt);
        }
        stmt.setBroadCast(false);
        removeSequenceAndGsi(stmt);

        stmt.setDbPartitionBy(null);
        stmt.setDbPartitions(null);
        stmt.setTablePartitionBy(null);
        stmt.setTablePartitions(null);
        stmt.setPrefixPartition(false);

        return stmt;
    }

    public MySqlStatement rewrite() {
        List<SQLStatement> statementList = SQLUtils.parseStatements(sourceSql, JdbcConstants.MYSQL);
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) statementList.get(0);
        Set<String> shardKeys = new LinkedHashSet<>();
        if (dbpartitionBy != null) {
            shardKeys = getShardingKeys(dbpartitionBy, shardKeys);
        }

        if (tbpartitionBy != null) {
            getShardingKeys(tbpartitionBy, shardKeys);
        }

        if (sqlPartition != null) {
            getPartitionKeys(sqlPartition, shardKeys);
        }

        // Remove implicit pk in shard keys, because it must be primary key.
        shardKeys.removeIf(SqlValidatorImpl::isImplicitKey);

        if (shardKeys.size() > 0) {
            if (sqlPartition == null || shardKeys.size() == 1) {
                addIndex(shardKeys, stmt, uniqueShardingKey);
            } else {
                // create composite indexes for key/range column/list column partitions
                addCompositeIndex(shardKeys, stmt);

            }
        }
        stmt.setBroadCast(false);
        // remove locality on mysql
        stmt.setLocality(null);
        removeSequenceAndGsi(stmt);

        for (Pair<SqlIdentifier, SqlColumnDeclaration> pair : colDefs) {
            String columnName = pair.left.getSimple();
            if (stmt.getColumn(columnName) == null && SqlValidatorImpl.isImplicitKey(columnName)) {
                // add column
                SQLColumnDefinition sqlColumnDefinition = new SQLColumnDefinition();
                sqlColumnDefinition.setName(pair.left.getSimple());
                sqlColumnDefinition.setDbType(DbType.mysql);
                sqlColumnDefinition.setDataType(new SQLDataTypeImpl("bigint"));
                sqlColumnDefinition.setAutoIncrement(true);
                stmt.addColumn(sqlColumnDefinition);

                // add pk
                MySqlPrimaryKey implicitKey = new MySqlPrimaryKey();
                SQLIndexDefinition indexDefinition = implicitKey.getIndexDefinition();
                indexDefinition.setKey(true);
                indexDefinition.setType("PRIMARY");

                indexDefinition.setName(new SQLIdentifierExpr(this.getPrimaryKey().getIndexName().getSimple()));
                indexDefinition.getColumns().add(new SQLSelectOrderByItem(sqlColumnDefinition.getName()));
                stmt.getTableElementList().add(implicitKey);
            }
        }
        return stmt;
    }

    public void addLocalPartitionSuffix(MySqlCreateTableStatement stmt) {
        if (stmt == null || localPartitionSuffix == null) {
            return;
        }
        stmt.setOptionHints(
            Lists.newArrayList(new SQLCommentHint("!50500 PARTITION BY " + localPartitionSuffix.toString())));
    }

    private static void removeSequenceAndGsi(MySqlCreateTableStatement stmt) {
        final Iterator<SQLTableElement> iterator = stmt.getTableElementList().iterator();
        while (iterator.hasNext()) {
            final SQLTableElement tableElement = iterator.next();
            if (tableElement instanceof SQLColumnDefinition) {
                // remove sequence definition
                final SQLColumnDefinition sqlColumnDefinition = (SQLColumnDefinition) tableElement;
                sqlColumnDefinition.setSequenceType(null);
                sqlColumnDefinition.setStep(null);
                sqlColumnDefinition.setUnitCount(null);
                sqlColumnDefinition.setUnitIndex(null);
            } else if ((tableElement instanceof MySqlTableIndex && (((MySqlTableIndex) tableElement).isGlobal()
                || ((MySqlTableIndex) tableElement).isClustered()))
                || (tableElement instanceof MySqlUnique && (((MySqlUnique) tableElement).isGlobal()
                || ((MySqlUnique) tableElement).isClustered()))) {
                // remove gsi definition
                iterator.remove();
            }
        }
    }

    private String prepare() {
        List<SQLStatement> statementList = SQLUtils.parseStatements(sourceSql, JdbcConstants.MYSQL);
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) statementList.get(0);
        Set<String> shardKeys = new HashSet<>();
        if (dbpartitions == null) {
            stmt.setDbPartitions(null);
        }

        if (tbpartitions == null) {
            stmt.setTablePartitions(null);
        }

        if (dbpartitionBy == null) {
            stmt.setDbPartitionBy(null);
        }

        if (tbpartitionBy == null) {
            stmt.setTablePartitionBy(null);
        }

        // 热点映射
        if (mappingRules == null) {
            stmt.setExPartition(null);
        }

        // For Partition Table
        if (sqlPartition == null) {
            stmt.setPartitioning(null);
        }

        if (localPartitionSuffix != null) {
            addLocalPartitionSuffix(stmt);
        }

        if (tableGroupName == null) {
            stmt.setTableGroup(null);
        }

        if (joinGroupName == null) {
            stmt.setJoinGroup(null);
        }

        if (!autoSplit) {
            stmt.setAutoSplit(null);
        }

        // set engine name to create table statement.
        if (engine != null) {
            stmt.setEngine(engine.name());
        }

        // Handle default binary value
        for (Pair<SqlIdentifier, SqlColumnDeclaration> colDef : GeneralUtil.emptyIfNull(colDefs)) {
            if (colDef.getValue().getDefaultVal() instanceof SqlBinaryStringLiteral) {
                final String hexString =
                    ((SqlBinaryStringLiteral) colDef.getValue().getDefaultVal()).getBitString().toHexString();
                final Iterator<SQLTableElement> it = stmt.getTableElementList().iterator();
                final String binaryColumnName = colDef.getKey().getLastName();
                while (it.hasNext()) {
                    final SQLTableElement tableElement = it.next();
                    if (tableElement instanceof SQLColumnDefinition) {
                        final SQLColumnDefinition columnDefinition = (SQLColumnDefinition) tableElement;
                        final String columnName = SQLUtils.normalizeNoTrim(columnDefinition.getName().getSimpleName());
                        if (binaryColumnName.equalsIgnoreCase(columnName)) {
                            SQLHexExpr newDefaultVal = new SQLHexExpr(hexString);
                            columnDefinition.setDefaultExpr(newDefaultVal);
                            break;
                        }
                    }
                }
            }
        }

        final SQLExprTableSource tableSource = stmt.getTableSource();
        if (this.name instanceof SqlDynamicParam) {
            StringBuilder sql = new StringBuilder();
            MySqlOutputVisitor questionMarkTableSource = new MySqlOutputVisitor(sql) {

                @Override
                protected void printTableSourceExpr(SQLExpr expr) {
                    if (!ConfigDataMode.isFastMock()) {
                        print("?");
                    } else {
                        // validator not support ? for tableName
                        super.printTableSourceExpr(expr);
                    }
                }

                @Override
                protected void printReferencedTableName(SQLExpr expr) {
                    if (!ConfigDataMode.isFastMock() && logicalReferencedTables != null) {
                        String referencedTableName = null;
                        if (expr instanceof SQLIdentifierExpr) {
                            referencedTableName = ((SQLIdentifierExpr) expr).getSimpleName();
                        } else if (expr instanceof SQLPropertyExpr) {
                            referencedTableName = ((SQLPropertyExpr) expr).getSimpleName();
                        }
                        if (TStringUtil.isNotEmpty(referencedTableName) &&
                            logicalReferencedTables.contains(referencedTableName)) {
                            print("?");
                        } else {
                            super.printReferencedTableName(expr);
                        }
                    } else {
                        super.printReferencedTableName(expr);
                    }
                }
            };
            questionMarkTableSource.visit(stmt);
            return sql.toString();
        }
        return stmt.toString();
    }

    public static void addIndex(Set<String> shardKeys, MySqlCreateTableStatement stmt, boolean isUniqueIndex) {
        final Map<String, SqlIndexColumnName> indexColumnNameMap = new LinkedHashMap<>();
        for (String columnName : shardKeys) {
            indexColumnNameMap.put(columnName, new SqlIndexColumnName(SqlParserPos.ZERO, new SqlIdentifier(columnName,
                SqlParserPos.ZERO), null, null));
        }
        addIndex(indexColumnNameMap, stmt, isUniqueIndex, ImmutableList.<SqlIndexOption>of(), false, shardKeys);
    }

    public static String getIndexColumnName(final SQLSelectOrderByItem indexColumnDef) {
        final SQLExpr expr = indexColumnDef.getExpr();

        if (expr instanceof SQLIdentifierExpr) {
            return SQLUtils.normalizeNoTrim(((SQLIdentifierExpr) expr).getSimpleName());
        } else if (expr instanceof SQLMethodInvokeExpr) {
            final SQLMethodInvokeExpr columnCall = (SQLMethodInvokeExpr) expr;
            return SQLUtils.normalizeNoTrim(columnCall.getMethodName());
        }
        return null;
    }

    public static void addIndex(Map<String, SqlIndexColumnName> indexColumnDefMap, MySqlCreateTableStatement stmt,
                                boolean isUniqueIndex, List<SqlIndexOption> options, boolean gsi,
                                Collection<String> shardingKey) {
        if (GeneralUtil.isEmpty(indexColumnDefMap)) {
            return;
        }

        final Set<String> indexColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        indexColumns.addAll(indexColumnDefMap.keySet());

        boolean needAddIndexColumns = gsi ? true : false;

        // Columns to be singly indexed
        final Set<String> unindexedColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        unindexedColumns.addAll(shardingKey);

        final Set<String> existingIndexNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        final Map<String, SQLColumnDefinition> columnDefMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        String indexType = "BTREE";
        if (GeneralUtil.isNotEmpty(options)) {
            for (SqlIndexOption option : options) {
                if (null != option.getIndexType()) {
                    indexType = option.getIndexType().name();
                }
            }
        }

        final Iterator<SQLTableElement> it = stmt.getTableElementList().iterator();
        while (it.hasNext()) {
            final SQLTableElement sqlTableElement = it.next();
            if (sqlTableElement instanceof MysqlForeignKey) {
                final SQLExpr expr = ((MysqlForeignKey) sqlTableElement).getReferencingColumns().get(0);
                if (expr instanceof SQLIdentifierExpr) {
                    final String k = SQLUtils.normalizeNoTrim(((SQLIdentifierExpr) expr).getSimpleName());

                    // Do not add local index for foreign key column
                    unindexedColumns.remove(k);
                }
            } else if (sqlTableElement instanceof MySqlTableIndex && !((MySqlTableIndex) sqlTableElement).isGlobal()) {
                final String k = getIndexColumnName(((MySqlTableIndex) sqlTableElement).getColumns().get(0));

                // Do not add local index for first column of existing local index
                unindexedColumns.remove(k);

                // Store existing index name
                Optional.ofNullable(((MySqlTableIndex) sqlTableElement).getName())
                    .map(in -> SQLUtils.normalizeNoTrim(in.getSimpleName())).ifPresent(existingIndexNames::add);
                if (gsi && indexColumns.contains(k) && needAddIndexColumns) {  // Filter preliminarily
                    final Set<String> elementColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                    for (SQLSelectOrderByItem column : ((MySqlTableIndex) sqlTableElement).getColumns()) {
                        final String one = getIndexColumnName(column);
                        elementColumns.add(one);
                    }
                    if (indexColumns.equals(elementColumns)) {
                        if (isUniqueIndex) {
                            if (!((MySqlTableIndex) sqlTableElement).getName().getSimpleName()
                                .equalsIgnoreCase(UGSI_PK_INDEX_NAME)) {
                                it.remove();    // Need to be replaced with MySqlUnique
                            }
                        } else {
                            needAddIndexColumns = false;
                            ((MySqlTableIndex) sqlTableElement).setIndexType(indexType);
                        }
                    }
                }
            } else if (sqlTableElement instanceof MySqlKey && !(sqlTableElement instanceof MySqlUnique
                && ((MySqlUnique) sqlTableElement).isGlobal())) {
                final String k = getIndexColumnName(((MySqlKey) sqlTableElement).getColumns().get(0));

                // Do not add local index for first column of existing local index
                unindexedColumns.remove(k);

                // Store existing index name
                Optional.ofNullable(((MySqlKey) sqlTableElement).getName())
                    .map(in -> SQLUtils.normalizeNoTrim(in.getSimpleName())).ifPresent(existingIndexNames::add);

                if (gsi && indexColumns.contains(k) && needAddIndexColumns) {  // Filter preliminarily
                    final Set<String> elementColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                    for (SQLSelectOrderByItem column : ((MySqlKey) sqlTableElement).getColumns()) {
                        final String one = getIndexColumnName(column);
                        elementColumns.add(one);
                    }
                    if (indexColumns.equals(elementColumns)) {
                        if (isUniqueIndex && !(sqlTableElement instanceof MySqlUnique)
                            && !(sqlTableElement instanceof MySqlPrimaryKey)) {
                            // never remove implicit pk info
                            if (!((MySqlKey) sqlTableElement).getName().getSimpleName()
                                .equalsIgnoreCase(UGSI_PK_INDEX_NAME)) {
                                it.remove();  //  Need to be replaced with MySqlUnique
                            }
                        } else {
                            needAddIndexColumns = false;
                            ((MySqlKey) sqlTableElement).setIndexType(indexType);
                        }
                    }
                }
            } else if (sqlTableElement instanceof SQLColumnDefinition) {
                final SQLColumnDefinition columnDef = (SQLColumnDefinition) sqlTableElement;
                final String columnName = SQLUtils.normalizeNoTrim(columnDef.getName().getSimpleName());
                if (stmt.isOnlyPrimaryKey(columnDef.nameHashCode64())) {
                    // Do not add local index for single primary key
                    unindexedColumns.remove(columnName);
                } else {
                    // Do not add local index for unique column or primary key
                    Optional.ofNullable(columnDef.getConstraints()).ifPresent(
                        constraints -> constraints.stream()
                            .filter(c -> c instanceof SQLColumnUniqueKey || c instanceof SQLColumnPrimaryKey)
                            .forEach(c -> unindexedColumns.remove(columnName)));
                }

                // Store index column def
                if (indexColumns.contains(columnName)) {
                    columnDefMap.put(columnName, columnDef);
                }
            }
        }

        // Remove columns which not in def map of index columns. Because we concat PK after index columns,
        // `unindexedColumns` may contain PK columns which no need to add local `auto_shard` index.
        unindexedColumns.removeIf(col -> !columnDefMap.containsKey(col));

        if (columnDefMap.size() != indexColumns.size()) {
            indexColumns.removeAll(columnDefMap.keySet());
            throw new IllegalArgumentException("Unknown index column " + String.join(",", indexColumns));
        }

        if (isUniqueIndex) {
            // For unique index, create unified index with all index columns included
            // Max length of MySQL index name is 64
            if (needAddIndexColumns) {
                // Should keep the order. indexColumnDefMap is a LinkedHashMap(keys in order)
                final Set<String> orderedIndexColumnNames = indexColumnDefMap.keySet();
                final String suffix = buildUnifyIndexName(orderedIndexColumnNames, 45);
                final String indexName = buildIndexName(existingIndexNames, suffix);

                final MySqlUnique uniqueIndex = new MySqlUnique();
                uniqueIndex.getIndexDefinition().setType("UNIQUE");
                uniqueIndex.getIndexDefinition().setKey(true);
                uniqueIndex.setIndexType(indexType);
                uniqueIndex.setName(new SQLIdentifierExpr(indexName));

                final List<SQLSelectOrderByItem> indexColumnList =
                    buildUnifiedIndexColumnDef(orderedIndexColumnNames, indexColumnDefMap, columnDefMap);

                uniqueIndex.getIndexDefinition().getColumns().addAll(indexColumnList);

                if (GeneralUtil.isNotEmpty(uniqueIndex.getColumns())) {
                    stmt.getTableElementList().add(uniqueIndex);
                }
            }
        } else {
            // For normal index, create single column index for each unindexed column
            for (String columnName : unindexedColumns) {
                final String indexName = buildIndexName(existingIndexNames, columnName);

                final MySqlTableIndex mySqlTableIndex = new MySqlTableIndex();
                mySqlTableIndex.getIndexDefinition().setKey(true);
                // get/setIndexType of MySqlTableIndex is ambiguous since refactor of DDL, because of legacy code.
                // Use getIndexDefinition().getOptions().setIndexType.
                mySqlTableIndex.getIndexDefinition().getOptions().setIndexType(indexType);
                mySqlTableIndex.setName(new SQLIdentifierExpr(indexName));

                final SQLSelectOrderByItem sqlSelectOrderByItem =
                    buildIndexColumnDef(columnName, indexColumnDefMap, columnDefMap, gsi, 191);

                if (sqlSelectOrderByItem != null) {
                    mySqlTableIndex.addColumn(sqlSelectOrderByItem);
                    stmt.getTableElementList().add(mySqlTableIndex);
                }
            }

            if (gsi && indexColumns.size() > 1 && needAddIndexColumns) {
                // For index table, create unified index with all index columns included
                // Max length of MySQL index name is 64
                final String surfix = buildUnifyIndexName(indexColumnDefMap.keySet(), 58);
                final String indexName = buildIndexName(existingIndexNames, "i_", surfix);

                final MySqlUnique uniqueIndex = new MySqlUnique();
                uniqueIndex.getIndexDefinition().setKey(true);
                uniqueIndex.setIndexType(indexType);
                uniqueIndex.setName(new SQLIdentifierExpr(indexName));

                final List<SQLSelectOrderByItem> indexColumnList =
                    buildUnifiedIndexColumnDef(indexColumnDefMap.keySet(), indexColumnDefMap, columnDefMap);

                uniqueIndex.getIndexDefinition().getColumns().addAll(indexColumnList);

                if (GeneralUtil.isNotEmpty(uniqueIndex.getColumns())) {
                    stmt.getTableElementList().add(uniqueIndex);
                }
            }
        }

    }

    public static String buildUnifyIndexName(Set<String> indexColumns, int maxLength) {

        String result = String.join("_", indexColumns);

        if (TStringUtil.isNotBlank(result) && result.length() < maxLength) {
            return result;
        }

        final Iterator<String> it = indexColumns.iterator();

        final String first = it.next();
        final String second = it.hasNext() ? it.next() : "";

        result = String.join("_", first, second);

        if (TStringUtil.isNotBlank(result) && result.length() < maxLength) {
            return result;
        }

        result = first;

        if (TStringUtil.isNotBlank(result) && result.length() < maxLength) {
            return result;
        }

        result = UUID.randomUUID().toString();

        if (result.length() < maxLength) {
            return result;
        }

        return result.substring(0, Math.min(maxLength, 16));
    }

    public static SQLSelectOrderByItem buildIndexColumnDef(String columnName,
                                                           Map<String, SqlIndexColumnName> indexColumnDefMap,
                                                           Map<String, SQLColumnDefinition> columnDefMap,
                                                           boolean gsi,
                                                           int maxLen) {
        final SQLColumnDefinition sqlColumnDefinition = columnDefMap.get(columnName);
        final String s = sqlColumnDefinition.getDataType().getName().toUpperCase();
        final List<SQLExpr> arguments = sqlColumnDefinition.getDataType().getArguments();
        final SqlIndexColumnName indexColumnDef = indexColumnDefMap.get(columnName);
        int length = Optional.ofNullable(indexColumnDef).map(SqlIndexColumnName::getLength).map(l -> l.intValue(true))
            .orElse(-1);
        final Boolean asc = indexColumnDef == null ? Boolean.TRUE : indexColumnDef.isAsc();

        switch (s) {
        case "TEXT":
        case "BLOB":
        case "LONGBLOB":
        case "LONGTEXT":
        case "MEDIUMBLOB":
        case "TINYBLOB":
        case "MEDIUMTEXT":
        case "TINYTEXT":
            // do not create index on columns with type above
            return null;
        case "VARCHAR":
            if (!gsi && arguments != null && arguments.size() > 0
                && arguments.get(0) instanceof SQLIntegerExpr) {
                length = ((SQLIntegerExpr) arguments.get(0)).getNumber().intValue();

                if (length > maxLen) {
                    length = maxLen;
                }
            }
            break;
        //case "BINARY":
        case "GEOMETRY":
        case "POINT":
        case "LINESTRING":
        case "POLYGON":
        case "MULTIPOINT":
        case "MULTILINESTRING":
        case "MULTIPOLYGON":
        case "GEOMETRYCOLLECTION":
        case "SET":
            throw new UnsupportedOperationException("Invalid type for a sharding key.");
        default:
            break;
        }

        final SQLSelectOrderByItem sqlSelectOrderByItem = new SQLSelectOrderByItem();
        if (length > 0) {
            final SQLMethodInvokeExpr expr = new SQLMethodInvokeExpr();
            // Note: columnName should be normalized before, so just surround with backtick.
            columnName = SqlIdentifier.surroundWithBacktick(columnName);
            expr.setMethodName(columnName);
            SQLIntegerExpr sqlIntegerExpr = new SQLIntegerExpr(length);
            expr.addArgument(sqlIntegerExpr);
            sqlSelectOrderByItem.setExpr(expr);
        } else {
            // Note: columnName should be normalized before, so just surround with backtick.
            columnName = SqlIdentifier.surroundWithBacktick(columnName);
            sqlSelectOrderByItem.setExpr(new SQLIdentifierExpr(columnName));
        }

        if (null != asc) {
            sqlSelectOrderByItem.setType(asc ? SQLOrderingSpecification.ASC : SQLOrderingSpecification.DESC);
        }

        return sqlSelectOrderByItem;
    }

    public static List<SQLSelectOrderByItem> buildUnifiedIndexColumnDef(Set<String> indexColumns,
                                                                        Map<String, SqlIndexColumnName> indexColumnDefMap,
                                                                        Map<String, SQLColumnDefinition> columnDefMap) {
        final List<SQLSelectOrderByItem> indexColumnList = new ArrayList<>();
        for (String indexColumn : indexColumns) {
            final SQLColumnDefinition columnDef = columnDefMap.get(indexColumn);
            final String indexColumnType = columnDef.getDataType().getName().toUpperCase();
            final SqlIndexColumnName indexColumnDef = indexColumnDefMap.get(indexColumn);
            final int length =
                Optional.ofNullable(indexColumnDef).map(SqlIndexColumnName::getLength).map(l -> l.intValue(true))
                    .orElse(-1);
            final Boolean asc = (null == indexColumnDef ? Boolean.TRUE : indexColumnDef.isAsc());

            switch (indexColumnType) {
            case "GEOMETRY":
            case "POINT":
            case "LINESTRING":
            case "POLYGON":
            case "MULTIPOINT":
            case "MULTILINESTRING":
            case "MULTIPOLYGON":
            case "GEOMETRYCOLLECTION":
            case "BINARY":
                throw new UnsupportedOperationException("Invalid type for index column.");
            default:
            }

            final SQLSelectOrderByItem sqlSelectOrderByItem = new SQLSelectOrderByItem();
            if (length > 0) {
                final SQLMethodInvokeExpr expr = new SQLMethodInvokeExpr(indexColumn);
                expr.addArgument(new SQLIntegerExpr(length));

                sqlSelectOrderByItem.setExpr(expr);
            } else {
                // Note: columnName should be normalized before, so just surround with backtick.
                indexColumn = SqlIdentifier.surroundWithBacktick(indexColumn);
                sqlSelectOrderByItem.setExpr(new SQLIdentifierExpr(indexColumn));
            }

            if (null != asc) {
                sqlSelectOrderByItem.setType(asc ? SQLOrderingSpecification.ASC : SQLOrderingSpecification.DESC);
            }

            indexColumnList.add(sqlSelectOrderByItem);
        }
        return indexColumnList;
    }

    private static String buildIndexName(Set<String> existingIndexes, String suffix) {
        return buildIndexName(existingIndexes, TddlConstants.AUTO_SHARD_KEY_PREFIX, suffix);
    }

    private static String buildIndexName(Set<String> existingIndexes, String prefix, String suffix) {
        StringBuilder indexName = new StringBuilder(prefix);
        indexName.append(suffix.toLowerCase());
        int tryTime = 0;

        // 检查indexName统一以大写为准
        while (existingIndexes.contains(indexName.toString().toUpperCase())) {
            if (tryTime == 0) {
                indexName.append("_").append(tryTime++);
                continue;
            }
            int i = indexName.lastIndexOf("_");
            indexName.delete(i, indexName.length());
            indexName.append('_').append(tryTime++);
        }
        return SqlIdentifier.surroundWithBacktick(indexName.toString().toLowerCase());
    }

    public static Set<String> getShardingKeys(SqlNode partitionBy, Set<String> shardings) {
        return getShardingKeys(partitionBy, shardings, true);
    }

    public static Set<String> getShardingKeys(SqlNode partitionBy, Set<String> shardings, boolean toUpperCase) {
        if (partitionBy == null) {
            return shardings;
        }
        final SqlBasicCall basicCallFunc = (SqlBasicCall) partitionBy;
        final String dbFunName = basicCallFunc.getOperator().getName();
        final List<SqlNode> operandList = basicCallFunc.getOperandList();
        final List<String> paramNames = new ArrayList<>();
        for (int i = 0; i < operandList.size(); i++) {
            final SqlNode sqlNode = operandList.get(i);
            if (sqlNode instanceof SqlIdentifier) {
                final String simple = ((SqlIdentifier) sqlNode).getSimple();
                paramNames.add(toUpperCase ? simple.toUpperCase() : simple);
            }
        }
        shardings.addAll(paramNames);
        return shardings;
    }

    public static Set<String> getPartitionKeys(SqlNode partitionBy, Set<String> shardings) {
        return getPartitionKeys(partitionBy, shardings, true);
    }

    public static class PartitionColumnFinder extends SqlShuttle {

        protected SqlIdentifier partColumn;
        protected boolean containConstExpr = false;
        protected boolean containPartFunc = false;
        protected boolean useNestingPartFunc = false;

        public PartitionColumnFinder() {
        }

        public boolean find(SqlNode partExpr) {
            partExpr.accept(this);
            return partColumn != null;
        }

        @Override
        public SqlNode visit(SqlCall call) {
            containPartFunc = true;
            List<SqlNode> operandList = call.getOperandList();
            for (int i = 0; i < operandList.size(); i++) {
                if (operandList.get(i) instanceof  SqlCall) {
                    useNestingPartFunc = true;
                }
            }
            return super.visit(call);
        }

        @Override
        public SqlNode visit(SqlLiteral literal) {
            containConstExpr =  true;
            return super.visit(literal);
        }

        @Override
        public SqlNode visit(SqlIdentifier id) {
            partColumn = id;
            return id;
        }

        public SqlIdentifier getPartColumn() {
            return partColumn;
        }

        public boolean isContainConstExpr() {
            return containConstExpr;
        }

        public boolean isContainPartFunc() {
            return containPartFunc;
        }

        public boolean isUseNestingPartFunc() {
            return useNestingPartFunc;
        }
    }

    public static Set<String> getPartitionKeys(SqlNode partitionBy, Set<String> shardingKeys, boolean toUpperCase) {

        SqlPartitionBy partitionByVal = (SqlPartitionBy) partitionBy;
        List<SqlNode> partCols = partitionByVal.getColumns();
        final List<String> skNames = new ArrayList<>();
        for (int i = 0; i < partCols.size(); i++) {
            SqlNode partCol = partCols.get(i);
            PartitionColumnFinder columnFinder = new PartitionColumnFinder();
            boolean findPartCol = columnFinder.find(partCol);
            if (findPartCol) {
                SqlIdentifier colName = columnFinder.getPartColumn();
                final String simple = colName.getSimple();
                skNames.add(toUpperCase ? simple.toUpperCase() : simple);
            }
        }
        shardingKeys.addAll(skNames);
        return shardingKeys;
    }

    @Override
    public String toString() {
        return prepare();
    }

    public SqlString toSqlString(SqlDialect dialect) {
        String sql = prepare();
        return new SqlString(dialect, sql);
    }

    public boolean isShadow() {
        return shadow;
    }

    public void setShadow(boolean shadow) {
        this.shadow = shadow;
    }

    public boolean isTemporary() {
        return temporary;
    }

    public void setTemporary(boolean temporary) {
        this.temporary = temporary;
    }

    public List<Pair<SqlIdentifier, SqlColumnDeclaration>> getColDefs() {
        return colDefs;
    }

    public void setColDefs(List<Pair<SqlIdentifier, SqlColumnDeclaration>> colDefs) {
        this.colDefs = colDefs;
    }

    public SqlIndexDefinition getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(SqlIndexDefinition primaryKey) {
        this.primaryKey = primaryKey;
    }

    public List<Pair<SqlIdentifier, SqlIndexDefinition>> getUniqueKeys() {
        return uniqueKeys;
    }

    public void setUniqueKeys(List<Pair<SqlIdentifier, SqlIndexDefinition>> uniqueKeys) {
        this.uniqueKeys = uniqueKeys;
    }

    public List<Pair<SqlIdentifier, SqlIndexDefinition>> getGlobalKeys() {
        return globalKeys;
    }

    public void setGlobalKeys(List<Pair<SqlIdentifier, SqlIndexDefinition>> globalKeys) {
        this.globalKeys = globalKeys;
    }

    public List<Pair<SqlIdentifier, SqlIndexDefinition>> getGlobalUniqueKeys() {
        return globalUniqueKeys;
    }

    public void setGlobalUniqueKeys(List<Pair<SqlIdentifier, SqlIndexDefinition>> globalUniqueKeys) {
        this.globalUniqueKeys = globalUniqueKeys;
    }

    public List<Pair<SqlIdentifier, SqlIndexDefinition>> getClusteredKeys() {
        return clusteredKeys;
    }

    public void setClusteredKeys(List<Pair<SqlIdentifier, SqlIndexDefinition>> clusteredKeys) {
        this.clusteredKeys = clusteredKeys;
    }

    public List<Pair<SqlIdentifier, SqlIndexDefinition>> getClusteredUniqueKeys() {
        return clusteredUniqueKeys;
    }

    public void setClusteredUniqueKeys(List<Pair<SqlIdentifier, SqlIndexDefinition>> clusteredUniqueKeys) {
        this.clusteredUniqueKeys = clusteredUniqueKeys;
    }

    public List<Pair<SqlIdentifier, SqlIndexDefinition>> getKeys() {
        return keys;
    }

    public void setKeys(List<Pair<SqlIdentifier, SqlIndexDefinition>> keys) {
        this.keys = keys;
    }

    public List<Pair<SqlIdentifier, SqlIndexDefinition>> getFullTextKeys() {
        return fullTextKeys;
    }

    public void setFullTextKeys(List<Pair<SqlIdentifier, SqlIndexDefinition>> fullTextKeys) {
        this.fullTextKeys = fullTextKeys;
    }

    public List<Pair<SqlIdentifier, SqlIndexDefinition>> getSpatialKeys() {
        return spatialKeys;
    }

    public void setSpatialKeys(List<Pair<SqlIdentifier, SqlIndexDefinition>> spatialKeys) {
        this.spatialKeys = spatialKeys;
    }

    public List<Pair<SqlIdentifier, SqlIndexDefinition>> getForeignKeys() {
        return foreignKeys;
    }

    public void setForeignKeys(List<Pair<SqlIdentifier, SqlIndexDefinition>> foreignKeys) {
        this.foreignKeys = foreignKeys;
    }

    public List<SqlCall> getChecks() {
        return checks;
    }

    public void setChecks(List<SqlCall> checks) {
        this.checks = checks;
    }

    public List<String> getLogicalReferencedTables() {
        return logicalReferencedTables;
    }

    public void setLogicalReferencedTables(List<String> logicalReferencedTables) {
        this.logicalReferencedTables = logicalReferencedTables;
    }

    public List<String> getPhysicalReferencedTables() {
        return physicalReferencedTables;
    }

    public void setPhysicalReferencedTables(List<String> physicalReferencedTables) {
        this.physicalReferencedTables = physicalReferencedTables;
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        super.validate(validator, scope);

        // Validate all fields

        // Valicate sql

    }

    @Override
    public boolean createGsi() {
        return GeneralUtil.isNotEmpty(globalKeys) || GeneralUtil.isNotEmpty(globalUniqueKeys)
            || GeneralUtil.isNotEmpty(clusteredKeys) || GeneralUtil.isNotEmpty(clusteredUniqueKeys);
    }

    public boolean createClusteredIndex() {
        return GeneralUtil.isNotEmpty(clusteredKeys) || GeneralUtil.isNotEmpty(clusteredUniqueKeys);
    }

    public boolean isUniqueShardingKey() {
        return uniqueShardingKey;
    }

    public void setUniqueShardingKey(boolean uniqueShardingKey) {
        this.uniqueShardingKey = uniqueShardingKey;
    }

    public SqlNode getSqlPartition() {
        return sqlPartition;
    }

    public static void addCompositeIndex(Set<String> shardKeys, MySqlCreateTableStatement stmt) {
        final Map<String, SqlIndexColumnName> indexColumnNameMap = new LinkedHashMap<>();
        for (String columnName : shardKeys) {
            indexColumnNameMap.put(columnName, new SqlIndexColumnName(SqlParserPos.ZERO, new SqlIdentifier(columnName,
                SqlParserPos.ZERO), null, null));
        }
        addCompositeIndex(indexColumnNameMap, stmt, shardKeys.stream().collect(Collectors.toList()));
    }

    public static void addCompositeIndex(Map<String, SqlIndexColumnName> indexColumnDefMap,
                                         MySqlCreateTableStatement stmt,
                                         List<String> shardingKey) {

        final Set<String> indexColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        indexColumns.addAll(indexColumnDefMap.keySet());

        // Columns to be singly indexed
        final Set<String> unindexedColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        unindexedColumns.addAll(shardingKey);

        final Set<String> existingIndexNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        final Map<String, SQLColumnDefinition> columnDefMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        final List<SQLTableElement> existIndexList = new ArrayList<>();

        String indexType = "BTREE";

        final Iterator<SQLTableElement> it = stmt.getTableElementList().iterator();
        while (it.hasNext()) {
            final SQLTableElement sqlTableElement = it.next();
            if (sqlTableElement instanceof SQLColumnDefinition) {
                final SQLColumnDefinition columnDef = (SQLColumnDefinition) sqlTableElement;
                final String columnName = SQLUtils.normalizeNoTrim(columnDef.getName().getSimpleName());
                // Store index column def
                if (indexColumns.contains(columnName)) {
                    columnDefMap.put(columnName, columnDef);
                }
            } else if (sqlTableElement instanceof MySqlTableIndex && !((MySqlTableIndex) sqlTableElement).isGlobal()) {
                if (((MySqlTableIndex) sqlTableElement).getColumns().size() > 1) {
                    existIndexList.add(sqlTableElement);
                }
            } else if (sqlTableElement instanceof MySqlKey) {
                if (((MySqlKey) sqlTableElement).getColumns().size() > 1) {
                    existIndexList.add(sqlTableElement);
                }
            }
        }

        if (columnDefMap.size() != indexColumns.size()) {
            indexColumns.removeAll(columnDefMap.keySet());
            throw new IllegalArgumentException("Unknown index column " + String.join(",", indexColumns));
        }

        boolean indexAlreadExists = false;
        for (SQLTableElement sqlTableElement : existIndexList) {
            List<SQLSelectOrderByItem> indexingColumns = null;
            if (sqlTableElement instanceof MySqlTableIndex) {
                indexingColumns = ((MySqlTableIndex) sqlTableElement).getColumns();
            } else if (sqlTableElement instanceof MySqlKey) {
                indexingColumns = ((MySqlKey) sqlTableElement).getColumns();
            }
            if (GeneralUtil.isNotEmpty(indexColumns) && indexingColumns.size() == shardingKey.size()) {
                int i = 0;
                for (SQLSelectOrderByItem item : indexingColumns) {
                    String colName = getIndexColumnName(item);
                    if (!colName.equalsIgnoreCase(shardingKey.get(i))) {
                        break;
                    }
                    i++;
                }
                if (i == indexingColumns.size()) {
                    indexAlreadExists = true;
                }
                if (indexAlreadExists) {
                    break;
                }
            } else {
                continue;
            }
        }
        if (!indexAlreadExists) {
            List<IndexColumnInfo> indexColumnInfos =
                preparAutoCompositeIndexs(shardingKey, columnDefMap, 191);

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < indexColumnInfos.size(); i++) {
                if (i > 0) {
                    sb.append("_");
                }
                sb.append(indexColumnInfos.get(i).getColName());
            }
            final String indexName = buildIndexName(existingIndexNames, sb.toString());
            final MySqlTableIndex mySqlTableIndex = new MySqlTableIndex();
            mySqlTableIndex.getIndexDefinition().setKey(true);
            // get/setIndexType of MySqlTableIndex is ambiguous since refactor of DDL, because of legacy code.
            // Use getIndexDefinition().getOptions().setIndexType.
            mySqlTableIndex.getIndexDefinition().getOptions().setIndexType(indexType);
            mySqlTableIndex.setName(new SQLIdentifierExpr(indexName));

            for (int i = 0; i < indexColumnInfos.size(); i++) {
                final SQLSelectOrderByItem sqlSelectOrderByItem =
                    buildIndexColumnDef(indexColumnInfos.get(i).getColName(), indexColumnDefMap, columnDefMap,
                        false,
                        indexColumnInfos.get(i).getIndexLen());

                if (sqlSelectOrderByItem != null) {
                    mySqlTableIndex.addColumn(sqlSelectOrderByItem);
                }
            }
            stmt.getTableElementList().add(mySqlTableIndex);
            existingIndexNames.add(indexName);
        }
    }

    private static List<IndexColumnInfo> preparAutoCompositeIndexs(List<String> shardKeys,
                                                                   Map<String, SQLColumnDefinition> columnDefMap,
                                                                   int maxLen) {
        assert shardKeys.size() <= 3;
        int totalIndexLen = 0;
        int totalCharTypeIndexLen = 0;
        int charTypeCount = 0;
        List<IndexColumnInfo> indexColumnInfos = new ArrayList<>(3);
        for (String shardKey : shardKeys) {
            SQLColumnDefinition sqlColumnDefinition = columnDefMap.get(shardKey);
            final String dataType = sqlColumnDefinition.getDataType().getName().toUpperCase();
            final List<SQLExpr> arguments = sqlColumnDefinition.getDataType().getArguments();
            int typeLen = 4;
            boolean isCharType = false;
            switch (dataType) {
            case "TINYINT":
                typeLen = 1;
                break;
            case "SMALLINT":
                typeLen = 2;
                break;
            case "MEDIUMINT":
                typeLen = 3;
                break;
            case "INTEGER":
            case "INT":
            case "NUMBER":
                typeLen = 4;
                break;
            case "BIGINT":
            case "REAL":
            case "DOUBLE":
                typeLen = 8;
                break;
            case "FLOAT":
                typeLen = 4;
                if (arguments != null && arguments.size() > 0
                    && arguments.get(0) instanceof SQLIntegerExpr) {
                    int precision = ((SQLIntegerExpr) arguments.get(0)).getNumber().intValue();

                    if (precision > 24) {
                        typeLen = 8;
                    }
                }
                break;
            case "DECIMAL":
            case "NUMERIC":
                if (arguments != null && arguments.size() > 0
                    && arguments.get(0) instanceof SQLIntegerExpr) {
                    int precision = ((SQLIntegerExpr) arguments.get(0)).getNumber().intValue();
                    int scale = ((SQLIntegerExpr) arguments.get(1)).getNumber().intValue();

                    if (precision < scale) {
                        typeLen = scale + 2;
                    } else {
                        typeLen = precision;
                    }
                }
                break;
            case "DATE":
            case "TIME":
                typeLen = 3;
                break;
            case "DATETIME":
                typeLen = 8;
                break;
            case "YEAR":
                typeLen = 1;
                break;
            case "TIMESTAMP":
                typeLen = 4;
                break;
            case "CHAR":
            case "VARCHAR":
            case "NVARCHAR":
            case "NCHAR":
            case "VARBINARY":
            case "BINARY":
                if (arguments != null && arguments.size() > 0
                    && arguments.get(0) instanceof SQLIntegerExpr) {
                    typeLen = ((SQLIntegerExpr) arguments.get(0)).getNumber().intValue();
                }
                isCharType = true;
                break;
            default:
                throw new UnsupportedOperationException("Invalid type for a sharding key.");
            }
            totalIndexLen += typeLen;
            if (isCharType) {
                totalCharTypeIndexLen += typeLen;
                charTypeCount++;
            }
            IndexColumnInfo indexColumnInfo = new IndexColumnInfo(shardKey, typeLen, isCharType, typeLen);
            indexColumnInfos.add(indexColumnInfo);
        }
        if (totalIndexLen > maxLen) {
            int remainingLen = maxLen - (totalIndexLen - totalCharTypeIndexLen);
            int allocLen = 0;
            for (int i = 0; i < indexColumnInfos.size(); i++) {
                if (indexColumnInfos.get(i).isCharType()) {
                    int calcIndexLen = indexColumnInfos.get(i).indexLen * (remainingLen / totalCharTypeIndexLen);
                    charTypeCount--;
                    if (remainingLen - calcIndexLen - allocLen < IndexColumnInfo.MIN_INDEX_LEN * charTypeCount) {
                        for (int j = i + 1; j < indexColumnInfos.size(); j++) {
                            if (indexColumnInfos.get(j).isCharType()) {
                                indexColumnInfos.get(j).setIndexLen(IndexColumnInfo.MIN_INDEX_LEN);
                                allocLen += indexColumnInfos.get(j).getIndexLen();
                            }
                        }
                        indexColumnInfos.get(i).setIndexLen(remainingLen - allocLen);
                        break;
                    } else {
                        indexColumnInfos.get(i).setIndexLen(calcIndexLen);
                        allocLen += indexColumnInfos.get(i).getIndexLen();
                    }
                }
            }
        }
        return indexColumnInfos;
    }
}

class IndexColumnInfo {
    final String colName;

    final int colTypeLen;
    final boolean isCharType;
    int indexLen;
    final static int MIN_INDEX_LEN = 16;

    public IndexColumnInfo(String colName, int colTypeLen, boolean isCharType, int indexLen) {
        this.colName = colName;
        this.colTypeLen = colTypeLen;
        this.isCharType = isCharType;
        this.indexLen = indexLen;
    }

    public int getIndexLen() {
        return indexLen;
    }

    public int getMixIndexLen() {
        return Math.min(indexLen, MIN_INDEX_LEN);
    }

    public void setIndexLen(int indexLen) {
        this.indexLen = Math.min(Math.max(indexLen, MIN_INDEX_LEN), colTypeLen);
    }

    public String getColName() {
        return colName;
    }

    public int getColTypeLen() {
        return colTypeLen;
    }

    public boolean isCharType() {
        return isCharType;
    }
}

// End SqlCreateTable.java
