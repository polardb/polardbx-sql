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
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

/**
 * @author pangzhaoxing
 */
public class DrdsCreateSecurityLabelComponentStatement extends MySqlStatementImpl implements SQLCreateStatement {

    private SQLExpr componentName;
    private SQLExpr componentType;
    private SQLExpr componentContent;

    @Override
    public void accept0(final MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            if (null != this.componentName) {
                componentName.accept(visitor);
            }
            if (null != this.componentType) {
                componentType.accept(visitor);
            }
            if (null != this.componentContent) {
                componentContent.accept(visitor);
            }
        }
        visitor.endVisit(this);
    }

    public SQLExpr getComponentName() {
        return componentName;
    }

    public void setComponentName(SQLExpr componentName) {
        this.componentName = componentName;
    }

    public SQLExpr getComponentType() {
        return componentType;
    }

    public void setComponentType(SQLExpr componentType) {
        this.componentType = componentType;
    }

    public SQLExpr getComponentContent() {
        return componentContent;
    }

    public void setComponentContent(SQLExpr componentContent) {
        this.componentContent = componentContent;
    }
}
