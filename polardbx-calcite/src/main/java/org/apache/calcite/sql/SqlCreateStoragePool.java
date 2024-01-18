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

import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlString;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Created by taojinkun.
 *
 * @author taojinkun
 */
public class SqlCreateStoragePool extends SqlCreate {

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("CREATE STORAGE POOL", SqlKind.CREATE_STORAGE_POOL);

    private final SqlNode storagePool;
    private final SqlNode dnList;

    private final SqlNode undeletableDn;
    private Boolean isLogical;

    private String tableName;


    public Boolean getLogical() {
        return isLogical;
    }


    public void setLogical(Boolean logical) {
        isLogical = logical;
    }

    public SqlCreateStoragePool(SqlParserPos pos, SqlNode storagePool, SqlNode dnList, SqlNode undeletableDn) {
        super(OPERATOR, SqlParserPos.ZERO, false, false);
        this.storagePool = storagePool;
        this.dnList = dnList;
        this.tableName = "_NONE_";
        this.name = new SqlIdentifier(tableName, SqlParserPos.ZERO);
        this.undeletableDn = undeletableDn;
        this.isLogical = true;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public SqlNode clone(SqlParserPos pos){
        return new SqlCreateStoragePool(pos, storagePool, dnList, undeletableDn);
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

    public SqlNode getUndeletableDn() {
        return undeletableDn;
    }

    @Override
    public String toString(){
        String dnListString = "";
        String undeletableDnString = "";
        if(getDnList() != null){
            dnListString = getDnList().toString();
        }
        if(getUndeletableDn() != null){
            undeletableDnString = getUndeletableDn().toString();
        }
        return String.format(" create storage pool  %s dn_list='%s' undeletable_dn='%s'", getStoragePool(), dnListString, undeletableDnString);
    }

    @Override
    public SqlString toSqlString(SqlDialect dialect, boolean forceParens) {
        String sql = toString();
        return new SqlString(dialect, sql);
    }

}
