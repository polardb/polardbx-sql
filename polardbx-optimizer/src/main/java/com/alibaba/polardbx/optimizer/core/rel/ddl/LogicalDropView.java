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

import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.DropView;
import org.apache.calcite.sql.SqlDropView;

/**
 * @author dylan
 */
public class LogicalDropView extends BaseDdlOperation {
    private final boolean ifExists;

    public LogicalDropView(DDL ddl) {
        super(ddl);
        SqlDropView sqlDropView = (SqlDropView) ddl.getSqlNode();
        this.ifExists = sqlDropView.isIfExists();
    }

    public String getViewName() {
        return tableName;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    public static LogicalDropView create(DropView dropView) {
        return new LogicalDropView(dropView);
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("DROP VIEW ");
        if (ifExists) {
            sqlBuilder.append("IF EXISTS ");
        }
        sqlBuilder.append("`").append(schemaName).append("`.`").append(tableName).append("`");
        return pw.item("sql", sqlBuilder.toString());
    }
}

