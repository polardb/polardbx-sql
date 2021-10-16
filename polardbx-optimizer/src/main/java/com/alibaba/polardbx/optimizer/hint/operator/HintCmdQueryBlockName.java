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

import java.util.List;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;

/**
 * @author chenmo.cm
 */
public class HintCmdQueryBlockName extends BaseHintOperator implements HintCmdOperator {

    public final String qbName;

    public HintCmdQueryBlockName(SqlBasicCall hint, ExecutionContext ec) {
        super(hint, ec);

        String tmpQbName = "";

        for (HintArgKey arg : getArgKeys()) {
            SqlNode value = this.argMap.get(arg);

            if (null == value) {
                continue;
            }

            switch (arg.ordinal) {
            case 0:
                tmpQbName = stringValue(value);
                break;
            default:
                break;
            } // end of switch
        } // end of for

        this.qbName = tmpQbName;
    }

    @Override
    public CmdBean handle(CmdBean current) {
        return current;
    }

    @Override
    public HintType getType() {
        return HintType.CMD_QUERY_BLOCK_NAME;
    }

    @Override
    protected List<HintArgKey> getArgKeys() {
        return HintArgKey.QUERY_BLOCK_NAME_HINT;
    }
}
