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

import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

/**
 * @author guxu
 */
public class DrdsPauseScheduleStatement extends MySqlStatementImpl implements SQLCreateStatement {

    private boolean ifExist;

    private Long scheduleId;

    private boolean forLocalPartition;

    public DrdsPauseScheduleStatement() {

    }

    public void accept0(MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
        }
        visitor.endVisit(this);
    }

    @Override
    public DrdsPauseScheduleStatement clone() {
        DrdsPauseScheduleStatement x = new DrdsPauseScheduleStatement();
        x.ifExist = this.ifExist;
        x.forLocalPartition = this.forLocalPartition;
        if (this.scheduleId != null) {
            x.setScheduleId(this.scheduleId);
        }
        return x;
    }

    public boolean isIfExist() {
        return this.ifExist;
    }

    public void setIfExist(final boolean ifExist) {
        this.ifExist = ifExist;
    }

    public boolean isForLocalPartition() {
        return forLocalPartition;
    }

    public void setForLocalPartition(boolean forLocalPartition) {
        this.forLocalPartition = forLocalPartition;
    }

    public Long getScheduleId() {
        return this.scheduleId;
    }

    public void setScheduleId(final Long scheduleId) {
        this.scheduleId = scheduleId;
    }
}
