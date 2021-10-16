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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;

/**
 * @author chenmo.cm
 */
public class HintCmdExtraOperator extends BaseHintOperator implements HintCmdOperator {

    private final Map<String, Object> extraCmd;

    public HintCmdExtraOperator(SqlBasicCall hint, ExecutionContext ec) {
        super(hint, ec);

        Map<String, Object> tmpExtraCmd = new HashMap<>();
        for (Entry<HintArgKey, SqlNode> argEntry : this.argMap.entrySet()) {
            if (TStringUtil.isNotBlank(argEntry.getKey().getName())) {
                tmpExtraCmd.put(argEntry.getKey().getName().toUpperCase(), RelUtils.stringValue(argEntry.getValue()));
            }
        }

        this.extraCmd = tmpExtraCmd;
    }

    @Override
    protected List<HintArgKey> getArgKeys() {
        return HintArgKey.CMD_EXTRA_HINT;
    }

    @Override
    public CmdBean handle(CmdBean current) {
        current.getExtraCmd().putAll(extraCmd);
        return current;
    }
}
