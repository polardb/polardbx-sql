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
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

import java.util.List;

/**
 * @author pangzhaoxing
 */
public class DrdsDropSecurityLabelStatement extends MySqlStatementImpl implements SQLDropStatement {

    private List<SQLName> labelNames;

    public DrdsDropSecurityLabelStatement() {
    }

    @Override
    public void accept0(final MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            if (null != this.labelNames) {
                for (SQLName labelName : labelNames) {
                    labelName.accept(visitor);
                }
            }

        }
        visitor.endVisit(this);
    }

    public List<SQLName> getLabelNames() {
        return labelNames;
    }

    public void setLabelNames(List<SQLName> labelNames) {
        this.labelNames = labelNames;
    }
}
