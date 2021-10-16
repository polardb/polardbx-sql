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
import org.apache.calcite.sql.SqlBasicCall;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HintInventoryOperator extends BaseHintOperator {

    private Map<HintType, Integer> hints = new HashMap<>();

    public HintInventoryOperator(SqlBasicCall hint, ExecutionContext ec) {
        super(hint, ec);
    }

    public Map<HintType, Integer> getHints() {
        return hints;
    }

    @Override
    protected List<HintArgKey> getArgKeys() {
        return HintArgKey.INVENTORY_HINT;
    }

    @Override
    public String toString() {
        if (hints.size() == 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder("+");
        for (Map.Entry<HintType, Integer> entry : hints.entrySet()) {
            sb.append(entry.getKey().getValue());
            if (entry.getKey().equals(HintType.INVENTORY_TARGET_AFFECT_ROW)) {
                sb.append('(').append(entry.getValue()).append(')');
            }
            sb.append(' ');
        }
        return sb.toString();
    }
}
