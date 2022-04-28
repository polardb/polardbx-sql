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

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlObjectImpl;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

public class DrdsAlterTableAsOfTimeStamp  extends MySqlObjectImpl implements SQLAlterTableItem {
    SQLExpr expr;
    public DrdsAlterTableAsOfTimeStamp(SQLExpr expr){
        this.expr = expr;
    }

    public SQLExpr getExpr() {
        return expr;
    }
    @Override
    public void accept0(MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
        }
        visitor.endVisit(this);
    }
}
