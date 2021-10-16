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
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLIndexDefinition;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddIndex;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropIndex;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateIndexStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUnique;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlAddIndex;
import org.apache.calcite.sql.SqlAlterSpecification;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableDropIndex;
import org.apache.calcite.sql.SqlCreateIndex;
import org.apache.calcite.sql.SqlDdlNodes;
import org.apache.calcite.sql.SqlDropIndex;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;

/**
 * Helper method for index building, such as generate index name, generate SqlNode
 *
 * @author moyi
 * @since 2021/07
 */
public class IndexBuilderHelper {

    /**
     * Generate implicit local index name for ALTER TABLE ADD/DROP INDEX
     */
    public static String genImplicitLocalIndexName(SqlAlterTable alterTable) {
        SqlAlterSpecification alter = alterTable.getAlters().get(0);
        String indexName;
        if (alter instanceof SqlAddIndex) {
            SqlAddIndex addIndex = (SqlAddIndex) alter;
            indexName = addIndex.getIndexName().getLastName();
        } else if (alter instanceof SqlAlterTableDropIndex) {
            SqlAlterTableDropIndex dropIndex = (SqlAlterTableDropIndex) alter;
            indexName = dropIndex.getIndexName().getLastName();
        } else {
            throw new UnsupportedOperationException(alter.toString());
        }

        return TddlConstants.AUTO_LOCAL_INDEX_PREFIX + TddlSqlToRelConverter.unwrapGsiName(indexName);
    }

    public static String genImplicitLocalIndexName(SqlCreateIndex sqlCreateIndex) {
        String indexName = sqlCreateIndex.getIndexName().getLastName();
        return TddlConstants.AUTO_LOCAL_INDEX_PREFIX + TddlSqlToRelConverter.unwrapGsiName(indexName);
    }

    public static String genImplicitLocalIndexName(SqlDropIndex sqlDropIndex) {
        String indexName = sqlDropIndex.getIndexName().getLastName();
        return TddlConstants.AUTO_LOCAL_INDEX_PREFIX + TddlSqlToRelConverter.unwrapGsiName(indexName);
    }

    /**
     * build SqlNode for implicit ALTER TABLE ADD/DROP INDEX
     */
    public static SqlAlterTable buildImplicitLocalIndexSql(SqlAlterTable alterTable) {
        final String localIndexNameString = genImplicitLocalIndexName(alterTable);
        final SqlIdentifier localIndexName = new SqlIdentifier(localIndexNameString, SqlParserPos.ZERO);

        // replace index name in origin SQL
        final String orgSql = alterTable.getSourceSql();
        final List<SQLStatement> stmts = SQLUtils.parseStatements(orgSql, JdbcConstants.MYSQL);
        final SQLAlterTableStatement stmt = ((SQLAlterTableStatement) stmts.get(0));
        if (stmt.getItems().get(0) instanceof SQLAlterTableAddIndex) {
            SQLAlterTableAddIndex alter = (SQLAlterTableAddIndex) stmt.getItems().get(0);
            alter.getIndexDefinition()
                .setName(new SQLIdentifierExpr(SqlIdentifier.surroundWithBacktick(localIndexNameString)));
            alter.getIndexDefinition().setCovering(Collections.emptyList());
            resetIndexPartition(alter.getIndexDefinition());
        } else if (stmt.getItems().get(0) instanceof SQLAlterTableDropIndex) {
            ((SQLAlterTableDropIndex) stmt.getItems().get(0)).setIndexName(new SQLIdentifierExpr(localIndexNameString));
        } else if (stmt.getItems().get(0) instanceof SQLAlterTableAddConstraint &&
            ((SQLAlterTableAddConstraint) stmt.getItems().get(0)).getConstraint() instanceof SQLUnique) {
            final SQLIndexDefinition indexDefinition =
                ((SQLUnique) ((SQLAlterTableAddConstraint) stmt.getItems().get(0)).getConstraint())
                    .getIndexDefinition();
            indexDefinition.setName(new SQLIdentifierExpr(SqlIdentifier.surroundWithBacktick(localIndexNameString)));
            indexDefinition.setCovering(Collections.emptyList());
            resetIndexPartition(indexDefinition);
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                "Unknown create GSI.");
        }

        // TODO(moyi) extract it to a method
        SqlAlterSpecification alter = alterTable.getAlters().get(0);
        SqlAlterSpecification newAlter;
        if (alter instanceof SqlAddIndex) {
            SqlAddIndex addIndex = (SqlAddIndex) alter;
            SqlAddIndex newAddIndex =
                new SqlAddIndex(SqlParserPos.ZERO, localIndexName,
                    addIndex.getIndexDef().rebuildToExplicitLocal(localIndexName));
            newAlter = newAddIndex;
        } else if (alter instanceof SqlAlterTableDropIndex) {
            SqlAlterTableDropIndex dropIndex = (SqlAlterTableDropIndex) alter;
            // TODO(moyi) prefixed with schema name like schema.table??
            String dropIndexSql =
                "DROP INDEX " + SqlIdentifier.surroundWithBacktick(localIndexNameString)
                    + " ON " + SqlIdentifier.surroundWithBacktick(dropIndex.getOriginTableName().getLastName());
            SqlAlterTableDropIndex newDropIndex =
                new SqlAlterTableDropIndex(dropIndex.getOriginTableName(), localIndexName, dropIndexSql,
                    SqlParserPos.ZERO);
            newAlter = newDropIndex;
        } else {
            throw new UnsupportedOperationException("not supported");
        }

        List<SqlAlterSpecification> alters = ImmutableList.of(newAlter);
        SqlAlterTable result = new SqlAlterTable(
            alterTable.getOriginTableName(),
            alterTable.getColumnOpts(),
            stmt.toString(),
            alterTable.getTableOptions(),
            alters,
            SqlParserPos.ZERO);
        return result;
    }

    /*
     * build SqlNode for implicit CREATE INDEX
     */
    public static SqlCreateIndex buildImplicitLocalIndexSql(SqlCreateIndex sqlCreateIndex) {
        String localIndexName = genImplicitLocalIndexName(sqlCreateIndex);
        SqlIdentifier localIndexIdentifier = new SqlIdentifier(localIndexName, SqlParserPos.ZERO);

        final String orgSql = sqlCreateIndex.getSourceSql();
        final List<SQLStatement> stmts = SQLUtils.parseStatements(orgSql, JdbcConstants.MYSQL);
        final SQLCreateIndexStatement stmt = ((SQLCreateIndexStatement) stmts.get(0));
        stmt.getIndexDefinition().setName(new SQLIdentifierExpr(SqlIdentifier.surroundWithBacktick(localIndexName)));
        resetIndexPartition(stmt.getIndexDefinition());

        return sqlCreateIndex.rebuildToExplicitLocal(localIndexIdentifier, stmt.toString());
    }

    public static SqlDropIndex buildImplicitLocalIndexSql(SqlDropIndex sqlDropIndex) {
        String localIndexName = genImplicitLocalIndexName(sqlDropIndex);
        SqlIdentifier localIndexIdentifier = new SqlIdentifier(localIndexName, SqlParserPos.ZERO);

        String dropIndexSql =
            "DROP INDEX " + SqlIdentifier.surroundWithBacktick(localIndexName)
                + " ON " + SqlIdentifier.surroundWithBacktick(sqlDropIndex.getOriginTableName().getLastName());
        SqlDropIndex dropIndex = SqlDdlNodes.dropIndex(localIndexIdentifier,
            new SqlIdentifier(localIndexName, SqlParserPos.ZERO), dropIndexSql, SqlParserPos.ZERO);

        return dropIndex;
    }

    /**
     * Turn this index into a local index
     *
     * @param def the index
     */
    public static void resetIndexPartition(SQLIndexDefinition def) {
        def.setLocal(false);
        def.setGlobal(false);
        def.setClustered(false);
        def.setDbPartitionBy(null);
        def.setTbPartitionBy(null);
        def.setTbPartitions(null);
        def.setPartitioning(null);
        def.setCovering(Collections.emptyList());
    }

}
