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
import java.util.List;

/**
 * Created by taojinkun.
 *
 * @author taojinkun
 */
public class SqlAlterStoragePool extends SqlCreate {

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("ALTER STORAGE POOL", SqlKind.ALTER_STORAGE_POOL);

    private final SqlNode storagePool;
    private final SqlNode dnList;

    private final SqlNode operation;

    private Boolean isLogical;

    private String tableName;

    public Boolean getLogical() {
        return isLogical;
    }


    public void setLogical(Boolean logical) {
        isLogical = logical;
    }

    public SqlAlterStoragePool(SqlParserPos pos, SqlNode storagePool, SqlNode dnList, SqlNode operation) {
        super(OPERATOR, SqlParserPos.ZERO, false, false);
        this.storagePool = storagePool;
        this.dnList = dnList;
        this.operation = operation;
        this.tableName = "_NONE_";
        this.name = new SqlIdentifier(tableName, SqlParserPos.ZERO);
        this.isLogical = true;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public SqlNode clone(SqlParserPos pos){
        return new SqlAlterStoragePool(pos, storagePool, dnList, operation);
    }

    @Override
    public List<SqlNode> getOperandList() {
        return new ArrayList<>();
    }


    public SqlNode getStoragePool(){
        return storagePool;
    }

    public SqlNode getDnList() {
        return dnList;
    }

    public SqlNode getOperation(){
        return operation;
    }

    @Override
    public String toString(){
        return String.format(" alter storage pool  %s %s node '%s'", getStoragePool(), getOperation(), getDnList());
    }

    @Override
    public SqlString toSqlString(SqlDialect dialect, boolean forceParens) {
        String sql = toString();
        return new SqlString(dialect, sql);
    }

}
