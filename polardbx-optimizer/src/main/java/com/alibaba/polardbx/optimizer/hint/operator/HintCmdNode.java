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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.hint.util.HintUtil;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;

import com.alibaba.fastjson.JSON;

import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.utils.TStringUtil;

/**
 * @author chenmo.cm
 */
public class HintCmdNode extends BaseHintOperator implements HintCmdOperator {

    private final List<String> groups;

    public HintCmdNode(SqlBasicCall hint, ExecutionContext ec) {
        super(hint, ec);

        final List<String> tmpGroups = new ArrayList<>();

        if (this.argMap.size() == 1) {
            final SqlNode value = this.argMap.entrySet().iterator().next().getValue();
            if (null != value) {
                String str = RelUtils.stringValue(value);
                if (TStringUtil.isNotBlank(str)) {
                    tmpGroups.addAll(HintUtil.splitAndTrim(str, ","));
                }
            }
        } else if (this.argMap.size() > 1) {
            this.argMap.forEach((k, v) -> {
                if (null != v) {
                    String str = RelUtils.stringValue(v);
                    if (TStringUtil.isNotBlank(str)) {
                        tmpGroups.add(str);
                    }
                }
            });
        }

        this.groups = ImmutableList.copyOf(tmpGroups);
    }

    @Override
    public CmdBean handle(CmdBean current) {
        if (this.groups.size() > 0) {
            final List<String> result = convertGroupIndex(current.getSchemaName(), this.groups, current);

            current.setGroups(result);
        }
        return current;
    }

    @Override
    public String oldStyle() {
        if (groups.size() <= 0) {
            return "";
        }

        Map<String, Object> oldHint = new HashMap<>();

        oldHint.put("type", "direct");
        oldHint.put("dbid", TStringUtil.join(groups, ","));
        oldHint.put("dbindex", "true");

        StringBuilder result = new StringBuilder("/*+TDDL(");
        result.append(JSON.toJSONString(oldHint));
        result.append(")*/");

        return result.toString();
    }

    @Override
    protected List<HintArgKey> getArgKeys() {
        return HintArgKey.NODE_HINT;
    }

    public boolean isNodeZero() {
        if (this.groups.size() == 1) {
            try {
                int groupIndex = Integer.valueOf(this.groups.get(0));
                return groupIndex == 0;
            } catch (Exception e) {
                return false;
            }
        }
        return false;
    }
}
