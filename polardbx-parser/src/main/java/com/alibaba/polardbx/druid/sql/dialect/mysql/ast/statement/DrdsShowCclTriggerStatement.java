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
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

import java.util.List;

/**
 * @author busu
 * date: 2021/4/16 2:11 下午
 */
public class DrdsShowCclTriggerStatement extends MySqlStatementImpl implements SQLCreateStatement {
    private boolean all;

    private List<SQLName> triggerNames;

    public DrdsShowCclTriggerStatement() {

    }

    public void accept0(MySqlASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    public boolean isAll() {
        return all;
    }

    public void setAll(boolean all) {
        this.all = all;
    }

    public List<SQLName> getTriggerNames() {
        return triggerNames;
    }

    public void setTriggerNames(List<SQLName> triggerNames) {
        this.triggerNames = triggerNames;
    }
}
