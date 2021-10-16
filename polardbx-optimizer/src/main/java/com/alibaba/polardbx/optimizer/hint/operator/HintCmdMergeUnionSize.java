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

import com.alibaba.polardbx.common.properties.ConnectionProperties;

/**
 * @author lingce.ldm 2018-06-19 14:58
 */
public class HintCmdMergeUnionSize extends BaseHintOperator implements HintCmdOperator {

    private final int mergeUnionSize;

    public HintCmdMergeUnionSize(SqlBasicCall hint, ExecutionContext ec) {
        super(hint, ec);

        int size = 1;
        for (HintArgKey key : getArgKeys()) {
            SqlNode value = this.argMap.get(key);
            if (null == value) {
                continue;
            }

            switch (key.ordinal) {
            case 0:
                size = integerValue(value);
                break;
            default:
                break;
            }
        }

        this.mergeUnionSize = size;
    }

    @Override
    protected List<HintArgKey> getArgKeys() {
        return HintArgKey.MERGE_UNION_SIZE_HINT;
    }

    @Override
    public CmdBean handle(CmdBean current) {
        current.getExtraCmd().put(ConnectionProperties.MERGE_UNION_SIZE, mergeUnionSize);
        return current;
    }
}
