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

package com.alibaba.polardbx.druid.sql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLDataType;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLObjectWithDataType;
import com.alibaba.polardbx.druid.sql.ast.SQLStatementImpl;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

import java.util.List;

public class SQLCreateJavaFunctionStatement extends SQLStatementImpl
    implements SQLCreateStatement, SQLObjectWithDataType {
    public SQLCreateJavaFunctionStatement() {

    }

    protected SQLName name;
    protected String javaCode;
    protected SQLDataType returnType;
    protected List<SQLDataType> inputType;

    protected boolean noState = false;

    @Override
    public void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, name);
        }
        visitor.endVisit(this);
    }

    public String getJavaCode() {
        return javaCode;
    }

    public void setJavaCode(String javaCode) {
        this.javaCode = javaCode;
    }

    public SQLName getName() {
        return name;
    }

    public void setName(SQLName name) {
        this.name = name;
    }

    public SQLDataType getReturnType() {
        return returnType;
    }

    public void setReturnType(SQLDataType returnType) {
        this.returnType = returnType;
    }

    public List<SQLDataType> getInputTypes() {
        return inputType;
    }

    public void setInputTypes(List<SQLDataType> inputType) {
        this.inputType = inputType;
    }

    public boolean isNoState() {
        return noState;
    }

    public void setNoState(boolean noState) {
        this.noState = noState;
    }

    @Override
    public SQLDataType getDataType() {
        return null;
    }

    @Override
    public void setDataType(SQLDataType dataType) {

    }
}
