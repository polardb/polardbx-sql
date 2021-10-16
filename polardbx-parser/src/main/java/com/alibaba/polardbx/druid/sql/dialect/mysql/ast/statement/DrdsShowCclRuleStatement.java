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
import com.google.common.base.Objects;

import java.util.ArrayList;
import java.util.List;

/**
 * @author busu
 */
public class DrdsShowCclRuleStatement extends MySqlStatementImpl implements SQLCreateStatement {

    private boolean allRules;

    private List<SQLName> ruleNames;

    public DrdsShowCclRuleStatement() {

    }

    public void accept0(MySqlASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    @Override
    public DrdsShowCclRuleStatement clone() {
        DrdsShowCclRuleStatement x = new DrdsShowCclRuleStatement();
        x.allRules = this.allRules;
        if (this.ruleNames != null) {
            List<SQLName> xRuleNames = new ArrayList<SQLName>(this.ruleNames.size());
            for (SQLName sqlName : ruleNames) {
                xRuleNames.add(sqlName.clone());
            }
            x.setRuleNames(xRuleNames);
        }
        return x;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DrdsShowCclRuleStatement that = (DrdsShowCclRuleStatement) o;
        return Objects.equal(this.allRules, that.allRules) && Objects.equal(this.ruleNames, that.ruleNames);
    }

    @Override
    public int hashCode() {
        int result = Boolean.valueOf(allRules).hashCode();
        result = 31 * result + (ruleNames != null ? ruleNames.hashCode() : 0);
        return result;
    }

    public boolean isAllRules() {
        return allRules;
    }

    public void setAllRules(boolean allRules) {
        this.allRules = allRules;
    }

    public List<SQLName> getRuleNames() {
        return ruleNames;
    }

    public void setRuleNames(List<SQLName> ruleNames) {
        this.ruleNames = ruleNames;
    }

}
