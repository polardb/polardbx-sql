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

package com.alibaba.polardbx.optimizer.core.rel.ddl;

import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLPropertyExpr;
import org.apache.calcite.rel.ddl.DropProcedure;
import org.apache.calcite.sql.SqlCreateProcedure;
import org.apache.calcite.sql.SqlDropProcedure;

public class LogicalDropProcedure extends BaseDdlOperation {

    private SqlDropProcedure sqlDropProcedure;

    public LogicalDropProcedure(DropProcedure dropProcedure) {
        super(dropProcedure);
        resetSchemaIfNeed(((SqlDropProcedure) relDdl.sqlNode).getProcedureName());
        this.sqlDropProcedure = (SqlDropProcedure) relDdl.sqlNode;
    }

    public static LogicalDropProcedure create(DropProcedure dropProcedure) {
        return new LogicalDropProcedure(dropProcedure);
    }

    public SqlDropProcedure getSqlDropProcedure() {
        return sqlDropProcedure;
    }

    private void resetSchemaIfNeed(SQLName procedureName) {
        if (procedureName instanceof SQLPropertyExpr) {
            setSchemaName(((SQLPropertyExpr) procedureName).getOwnerName());
        }
    }

    @Override
    public boolean isSupportedByFileStorage() {
        return true;
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        return true;
    }
}
