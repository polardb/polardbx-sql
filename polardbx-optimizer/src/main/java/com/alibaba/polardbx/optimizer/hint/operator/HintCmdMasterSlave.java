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

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

/**
 * @author chenmo.cm
 */
public class HintCmdMasterSlave extends BaseHintOperator implements HintCmdOperator {

    private long delayCutoff = Long.MIN_VALUE;

    public HintCmdMasterSlave(SqlBasicCall hint, ExecutionContext ec) {
        super(hint, ec);

        long tmpDelayCutoff = 0;

        for (HintArgKey key : getArgKeys()) {
            SqlNode value = this.argMap.get(key);

            if (null == value) {
                continue;
            }

            switch (key.ordinal) {
            case 0:
                tmpDelayCutoff = RelUtils.longValue(value);
                break;
            default:
                break;
            } // end of switch
        } // end of for

        this.delayCutoff = tmpDelayCutoff;
    }

    @Override
    protected List<HintArgKey> getArgKeys() {
        return HintArgKey.MASTER_SLAVE_HINT;
    }

    @Override
    public CmdBean handle(CmdBean current) {
        switch (type) {
        case CMD_MASTER:
            current.getExtraCmd().put("MASTER", true);
            break;
        case CMD_SLAVE:
            current.getExtraCmd().put("SLAVE", true);
            break;
        default:
            break;
        }
        return current;
    }
}
