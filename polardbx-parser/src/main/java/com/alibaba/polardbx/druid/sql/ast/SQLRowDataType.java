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
import com.alibaba.polardbx.druid.sql.ast.SQLStructDataType.Field;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;
import com.alibaba.polardbx.druid.util.FnvHash;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SQLRowDataType extends SQLObjectImpl implements SQLDataType {
    private DbType dbType;
    private List<Field> fields = new ArrayList<Field>();

    public SQLRowDataType() {

    }

    public SQLRowDataType(DbType dbType) {
        this.dbType = dbType;
    }

    @Override
    public String getName() {
        return "ROW";
    }

    @Override
    public long nameHashCode64() {
        return FnvHash.Constants.ROW;
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
        this.dbType = dbType;
    }

    @Override
    public DbType getDbType() {
        return dbType;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, fields);
        }
        visitor.endVisit(this);
    }

    public SQLRowDataType clone() {
        SQLRowDataType x = new SQLRowDataType(dbType);

        for (Field field : fields) {
            x.addField(field.getName(), field.getDataType().clone());
        }

        return x;
    }

    public List<Field> getFields() {
        return fields;
    }

    public Field addField(SQLName name, SQLDataType dataType) {
        Field field = new Field(name, dataType);
        field.setParent(this);
        fields.add(field);
        return field;
    }

    public int jdbcType() {
        return Types.STRUCT;
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
