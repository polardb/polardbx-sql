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

package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

public class DrdsConvertAllSequencesStatement extends MySqlStatementImpl implements SQLStatement {

    private SQLName fromType;
    private SQLName toType;
    private SQLName schemaName;
    private boolean allSchemata;

    @Override
    public void accept0(MySqlASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    public SQLName getFromType() {
        return fromType;
    }

    public void setFromType(SQLName fromType) {
        this.fromType = fromType;
    }

    public SQLName getToType() {
        return toType;
    }

    public void setToType(SQLName toType) {
        this.toType = toType;
    }

    public SQLName getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(SQLName schemaName) {
        this.schemaName = schemaName;
    }

    public boolean isAllSchemata() {
        return allSchemata;
    }

    public void setAllSchemata(boolean allSchemata) {
        this.allSchemata = allSchemata;
    }

    @Override
    public SqlType getSqlType() {
        return null;
    }
}
