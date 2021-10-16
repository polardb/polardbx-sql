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

package com.alibaba.polardbx.optimizer.hint.operator;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class HintCmdJoin extends BaseHintOperator implements HintCmdOperator {

    private HintType joinType;

    private Set<String> left;

    private Set<String> right;

    public HintCmdJoin(SqlBasicCall hint, ExecutionContext ec) {
        super(hint, ec);
        this.joinType = HintType.of(hint.getOperator().toString());
        if (hint.getOperands().length >= 1) {
            left = operandsToSet(hint.getOperands()[0]);
        }
        if (hint.getOperands().length >= 2) {
            right = operandsToSet(hint.getOperands()[1]);
        }
    }

    private Set<String> operandsToSet(SqlNode sqlNode) {
        TreeSet<String> set = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
        if (sqlNode.toString().equalsIgnoreCase("any")) {
            // just pass
        } else if (sqlNode instanceof SqlIdentifier) {
            set = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
            set.add(sqlNode.toString());
        } else if (sqlNode instanceof SqlBasicCall && sqlNode.getKind() == SqlKind.ROW) {
            set = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
            for (SqlNode table : ((SqlBasicCall) sqlNode).getOperands()) {
                set.add(table.toString());
            }
        }
        return set;
    }

    @Override
    protected List<HintArgKey> getArgKeys() {
        return HintArgKey.JOIN_HINT;
    }

    @Override
    public CmdBean handle(CmdBean current) {
        Object obj = current.getExtraCmd().get(ConnectionProperties.JOIN_HINT);
        if (obj == null || !(obj instanceof List)) {
            ArrayList<JoinHint> joinHints = new ArrayList<>();
            joinHints.add(new JoinHint(joinType, left, right));
            current.getExtraCmd().put(ConnectionProperties.JOIN_HINT, joinHints);
        } else {
            ArrayList<JoinHint> joinHints = (ArrayList<JoinHint>) obj;
            joinHints.add(new JoinHint(joinType, left, right));
        }
        return current;
    }
}
