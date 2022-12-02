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

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * DESCRIPTION
 *
 * @author
 */
public class SqlAlterTablePartitionKey extends SqlAlterTable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlAlterTablePartitionKey.class);

    private final SqlIdentifier originTableName;

    private final SqlNode dbPartitionBy;
    private final SqlNode dbpartitions;
    private final SqlNode tablePartitionBy;
    private final SqlNode tbpartitions;
    private boolean broadcast;
    private boolean single;

    private String logicalSecondaryTableName;

    public SqlAlterTablePartitionKey(SqlIdentifier tableName,
                                     String sql,
                                     SqlNode dbPartitionBy,
                                     SqlNode dbpartitions,
                                     SqlNode tablePartitionBy,
                                     SqlNode tbpartitions) {

        super(null, tableName, null, sql, null, new ArrayList<>(), SqlParserPos.ZERO);
        this.name = tableName;
        this.originTableName = tableName;
        this.dbPartitionBy = dbPartitionBy;
        this.dbpartitions = dbpartitions;
        this.tablePartitionBy = tablePartitionBy;
        this.tbpartitions = tbpartitions;
    }

    @Override
    public SqlIdentifier getOriginTableName() {
        return originTableName;
    }

    public SqlNode getDbPartitionBy() {
        return dbPartitionBy;
    }

    public SqlNode getDbpartitions() {
        return dbpartitions;
    }

    public SqlNode getTablePartitionBy() {
        return tablePartitionBy;
    }

    public SqlNode getTbpartitions() {
        return tbpartitions;
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
        return getSourceSql();
    }

    public String getSchemaName() {
        return originTableName.getComponent(0).getLastName();
    }

    public String getPrimaryTableName() {
        return originTableName.getComponent(1).getLastName();
    }

    public String getLogicalSecondaryTableName() {
        return logicalSecondaryTableName;
    }

    public void setLogicalSecondaryTableName(String logicalSecondaryTableName) {
        this.logicalSecondaryTableName = logicalSecondaryTableName;
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
}
