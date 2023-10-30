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

package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.EqualsContext;
import org.apache.calcite.util.Litmus;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class SqlPartitionByRange extends SqlPartitionBy {

    protected SqlBasicCall interval;
    protected SqlCharStringLiteral startWith;
    protected SqlNumericLiteral lifeCycleNum;
    protected SqlNumericLiteral expireAfter;
    protected SqlNumericLiteral preAllocate;
    protected SqlNode pivotDateExpr;
    protected boolean isColumns;
    protected boolean disableSchedule;

    public SqlPartitionByRange(SqlParserPos pos){
        super(pos);
    }

    public SqlBasicCall getInterval() {
        return this.interval;
    }

    public void setInterval(final SqlBasicCall interval) {
        this.interval = interval;
    }

    public SqlCharStringLiteral getStartWith() {
        return this.startWith;
    }

    public void setStartWith(final SqlCharStringLiteral startWith) {
        this.startWith = startWith;
    }

    public SqlNumericLiteral getLifeCycleNum() {
        return this.lifeCycleNum;
    }

    public void setLifeCycleNum(final SqlNumericLiteral lifeCycleNum) {
        this.lifeCycleNum = lifeCycleNum;
    }

    public SqlNumericLiteral getExpireAfter() {
        return this.expireAfter;
    }

    public void setExpireAfter(final SqlNumericLiteral expireAfter) {
        this.expireAfter = expireAfter;
    }

    public SqlNumericLiteral getPreAllocate() {
        return this.preAllocate;
    }

    public void setPreAllocate(final SqlNumericLiteral preAllocate) {
        this.preAllocate = preAllocate;
    }

    public SqlNode getPivotDateExpr() {
        return this.pivotDateExpr;
    }

    public void setPivotDateExpr(final SqlNode pivotDateExpr) {
        this.pivotDateExpr = pivotDateExpr;
    }

    public SqlNumericLiteral getIntervalNum(){
        return (SqlNumericLiteral) interval.getOperands()[0];
    }

    public SqlIntervalQualifier getIntervalQualifier(){
        return (SqlIntervalQualifier) interval.getOperands()[1];
    }

    @Override
    public SqlNode getSqlTemplate() {
        return this;
    }

    public boolean isColumns() {
        return isColumns;
    }

    public void setColumns(boolean columns) {
        isColumns = columns;
    }

    @Override
    public boolean equalsDeep(SqlNode node, Litmus litmus, EqualsContext context) {
        if (!super.equalsDeep(node, litmus, context)) {
            return false;
        }
        SqlPartitionByRange objPartBy = (SqlPartitionByRange) node;

        return isColumns == objPartBy.isColumns && equalDeep(interval, objPartBy.interval, litmus, context);
    }

    public boolean isDisableSchedule() {
        return this.disableSchedule;
    }

    public void setDisableSchedule(final boolean disableSchedule) {
        this.disableSchedule = disableSchedule;
    }
}
