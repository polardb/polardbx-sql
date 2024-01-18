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

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlString;

import java.util.ArrayList;

public class SqlAlterTableRepartitionLocalPartition extends SqlAlterTable {

    protected SqlNode parent;

    protected SqlPartitionByRange localPartition;

    public SqlAlterTableRepartitionLocalPartition(SqlIdentifier tableName, SqlPartitionByRange localPartition) {
        super(null, tableName, null, "", null, new ArrayList<>(), SqlParserPos.ZERO);
        this.name = tableName;
        this.localPartition = localPartition;
    }

    public SqlNode getParent() {
        return parent;
    }

    public void setParent(SqlNode parent) {
        this.parent = parent;
    }

    public SqlPartitionByRange getLocalPartition() {
        return this.localPartition;
    }

    public void setLocalPartition(final SqlPartitionByRange localPartition) {
        this.localPartition = localPartition;
    }

    @Override
    public String toString() {
        return "LOCAL PARTITION";
    }

    @Override
    public SqlString toSqlString(SqlDialect dialect) {
        return new SqlString(dialect ,toString());
    }
}
