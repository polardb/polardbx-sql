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

import com.alibaba.polardbx.druid.sql.ast.SQLName;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlString;

import java.util.Arrays;
import java.util.List;

/**
 * Created by wumu.
 *
 * @author wumu
 */
public class SqlAlterTableRepartition extends SqlAlterTable {
    private String sourceSql;

    private SqlNode sqlPartition = null;
    private SqlIdentifier originTableName;
    private boolean broadcast;
    private boolean single;
    private boolean alignToTableGroup = false;
    private SqlIdentifier tableGroupName;

    public SqlNode getLocality() {
        return locality;
    }

    public void setLocality(SqlNode locality) {
        this.locality = locality;
    }

    private SqlNode locality;

    public SqlAlterTableRepartition(SqlIdentifier tableName,
                                    String sql, List<SqlAlterSpecification> alters,
                                    SqlNode sqlPartition,
                                    boolean alignToTableGroup,
                                    SqlIdentifier tableGroupName,
                                    SqlNode locality) {
        super(null, tableName, null, sql, null, alters, SqlParserPos.ZERO);
        this.sourceSql = sql;
        this.sqlPartition = sqlPartition;
        this.originTableName = tableName;
        this.tableGroupName = tableGroupName;
        this.alignToTableGroup = alignToTableGroup;
        this.locality = locality;
    }

    static public SqlAlterTableRepartition create(SqlAlterTablePartitionKey sqlAlterTablePartitionKey) {
        SqlAlterTableRepartition sqlAlterPartitionTableRepartition =
            new SqlAlterTableRepartition(sqlAlterTablePartitionKey.getOriginTableName(),
                sqlAlterTablePartitionKey.getSourceSql() ,
                sqlAlterTablePartitionKey.getAlters(), null, false, null, null);
        sqlAlterPartitionTableRepartition.setBroadcast(sqlAlterTablePartitionKey.isBroadcast());
        sqlAlterPartitionTableRepartition.setSingle(sqlAlterTablePartitionKey.isSingle());
        return sqlAlterPartitionTableRepartition;
    }

    public SqlNode getSqlPartition() {
        return sqlPartition;
    }

    public void setSqlPartition(SqlNode sqlPartition) {
        this.sqlPartition = sqlPartition;
    }

    @Override
    public SqlIdentifier getOriginTableName() {
        return originTableName;
    }

    @Override
    public String getSourceSql() {
        return sourceSql;
    }

    public void setSourceSql(String sourceSql) {
        this.sourceSql = sourceSql;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Arrays.asList(name);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        unparse(writer, leftPrec, rightPrec, false);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec, boolean withOriginTableName) {
        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, "ALTER TABLE", "");

        name.unparse(writer, leftPrec, rightPrec);

        writer.endList(frame);
    }

    @Override
    public void setTargetTable(SqlIdentifier sqlIdentifier) {
        this.name = sqlIdentifier;
    }

    private String prepare() {
        return sourceSql;
    }

    public String getSchemaName() {
        return originTableName.getComponent(0).getLastName();
    }

    public String getPrimaryTableName() {
        return originTableName.getComponent(1).getLastName();
    }

    @Override
    public SqlNode getTargetTable() {
        return super.getTargetTable();
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

    @Override
    public boolean createGsi() {
        return true;
    }

    public boolean isBroadcast() {
        return this.broadcast;
    }

    public void setBroadcast(final boolean broadcast) {
        this.broadcast = broadcast;
    }

    public boolean isSingle() {
        return this.single;
    }

    public void setSingle(final boolean single) {
        this.single = single;
    }

    public boolean isSingleOrBroadcast() {
        return this.single || this.broadcast;
    }

    public boolean isAlignToTableGroup() {
        return alignToTableGroup;
    }

    public void setAlignToTableGroup(boolean alignToTableGroup) {
        this.alignToTableGroup = alignToTableGroup;
    }

    public SqlIdentifier getTableGroupName() {
        return tableGroupName;
    }

    public void setTableGroupName(SqlIdentifier tableGroupName) {
        this.tableGroupName = tableGroupName;
    }
}
