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
public class SqlDropStoragePool extends SqlCreate {

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("DROP STORAGE POOL", SqlKind.DROP_STORAGE_POOL);

    private final SqlNode storagePool;
    private Boolean isLogical;

    private String tableName;

    public Boolean getLogical() {
        return isLogical;
    }


    public void setLogical(Boolean logical) {
        isLogical = logical;
    }

    public SqlDropStoragePool(SqlParserPos pos, SqlNode storagePool){
        super(OPERATOR, SqlParserPos.ZERO, false, false);
        this.storagePool = storagePool;
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
        return new SqlDropStoragePool(pos, storagePool);
    }

    @Override
    public List<SqlNode> getOperandList() {
        return new ArrayList<>();
    }


    public SqlNode getStoragePool(){
        return storagePool;
    }

    @Override
    public String toString(){
        return String.format(" drop storage pool  %s ", getStoragePool());
    }

    @Override
    public SqlString toSqlString(SqlDialect dialect, boolean forceParens) {
        String sql = toString();
        return new SqlString(dialect, sql);
    }

}
