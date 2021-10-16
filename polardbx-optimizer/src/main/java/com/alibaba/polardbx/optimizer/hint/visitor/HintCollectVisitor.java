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

package com.alibaba.polardbx.optimizer.hint.visitor;

import java.util.List;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.hint.util.HintUtil;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlReplace;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.TDDLSqlSelect;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdOperator;
import com.alibaba.polardbx.optimizer.hint.util.HintConverter.HintCollection;

/**
 * @author chenmo.cm
 */
public class HintCollectVisitor extends SqlBasicVisitor<SqlNode> {

    private final ExecutionContext ec;
    private boolean testMode;
    private HintCollection collection = new HintCollection();

    public HintCollectVisitor(boolean testMode, ExecutionContext ec) {
        this.testMode = testMode;
        this.ec = ec;
    }

    public HintCollection getCollection() {
        return collection;
    }

    public List<HintCmdOperator> getCmdOperator() {
        return collection.cmdHintResult;
    }

    @Override
    public SqlNode visit(SqlCall call) {
        SqlNode result = super.visit(call);

        SqlNodeList hints = null;
        if (call instanceof TDDLSqlSelect) {
            TDDLSqlSelect select = (TDDLSqlSelect) call;
            hints = select.getHints();
        } else if (call instanceof SqlDelete) {
            SqlDelete delete = (SqlDelete) call;
            hints = delete.getHints();
        } else if (call instanceof SqlUpdate) {
            SqlUpdate update = (SqlUpdate) call;
            hints = update.getHints();
        } else if (call instanceof SqlInsert) {
            SqlInsert insert = (SqlInsert) call;
            hints = insert.getHints();
        }
        if (call instanceof SqlReplace) {
            SqlReplace replace = (SqlReplace) call;
            hints = replace.getHints();
        }
        if (call instanceof SqlHint) {
            SqlHint sqlSupportHint = (SqlHint) call;
            hints = sqlSupportHint.getHints();
        }

        HintUtil.collectHint(hints, collection, testMode, ec);

        return result;
    }

}
