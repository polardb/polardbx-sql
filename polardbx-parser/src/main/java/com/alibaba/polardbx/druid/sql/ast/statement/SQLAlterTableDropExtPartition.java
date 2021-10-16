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

import com.alibaba.polardbx.druid.sql.ast.SQLObjectImpl;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlObject;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlExtPartition;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

/**
 * @author shicai.xsc 2018/9/17 上午10:35
 * @since 5.0.0.0
 */
public class SQLAlterTableDropExtPartition  extends SQLObjectImpl implements SQLAlterTableItem, MySqlObject {
    private MySqlExtPartition extPartition;

    @Override
    public void accept0(MySqlASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    public void setExPartition(MySqlExtPartition x) {
        if (x != null) {
            x.setParent(this);
        }
        this.extPartition = x;
    }

    public MySqlExtPartition getExtPartition() {
        return extPartition;
    }
}