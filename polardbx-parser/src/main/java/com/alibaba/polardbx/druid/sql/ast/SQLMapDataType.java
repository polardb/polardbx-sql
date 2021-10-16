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

package com.alibaba.polardbx.druid.sql.ast;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;
import com.alibaba.polardbx.druid.util.FnvHash;

import java.sql.Types;
import java.util.Collections;
import java.util.List;

public class SQLMapDataType extends SQLObjectImpl implements SQLDataType {
    public static final SQLMapDataType MAP_CHAR_CHAR = new SQLMapDataType(SQLCharExpr.DATA_TYPE, SQLCharExpr.DATA_TYPE);

    private DbType dbType;
    private SQLDataType keyType;
    private SQLDataType valueType;

    public SQLMapDataType() {

    }

    public SQLMapDataType(SQLDataType keyType, SQLDataType valueType) {
        this.setKeyType(keyType);
        this.setValueType(valueType);
    }

    public SQLMapDataType(SQLDataType keyType, SQLDataType valueType, DbType dbType) {
        this.setKeyType(keyType);
        this.setValueType(valueType);
        this.dbType = dbType;
    }

    @Override
    public String getName() {
        return "MAP";
    }

    @Override
    public long nameHashCode64() {
        return FnvHash.Constants.MAP;
    }

    @Override
    public void setName(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<SQLExpr> getArguments() {
        return Collections.emptyList();
    }

    @Override
    public Boolean getWithTimeZone() {
        return null;
    }

    @Override
    public void setWithTimeZone(Boolean value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isWithLocalTimeZone() {
        return false;
    }

    @Override
    public void setWithLocalTimeZone(boolean value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setDbType(DbType dbType) {
        dbType = dbType;
    }

    @Override
    public DbType getDbType() {
        return dbType;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, keyType);
            acceptChild(visitor, valueType);
        }
        visitor.endVisit(this);
    }

    public SQLMapDataType clone() {
        SQLMapDataType x = new SQLMapDataType();
        x.dbType = dbType;

        if (keyType != null) {
            x.setKeyType(keyType.clone());
        }

        if (valueType != null) {
            x.setValueType(valueType.clone());
        }

        return x;
    }

    public SQLDataType getKeyType() {
        return keyType;
    }

    public void setKeyType(SQLDataType x) {
        if (x != null) {
            x.setParent(this);
        }
        this.keyType = x;
    }

    public SQLDataType getValueType() {
        return valueType;
    }

    public void setValueType(SQLDataType x) {
        if (x != null) {
            x.setParent(this);
        }
        this.valueType = x;
    }

    public int jdbcType() {
        return Types.OTHER;
    }

    @Override
    public boolean isInt() {
        return false;
    }

    @Override
    public boolean isNumberic() {
        return false;
    }

    @Override
    public boolean isString() {
        return false;
    }

    @Override
    public boolean hasKeyLength() {
        return false;
    }
}
