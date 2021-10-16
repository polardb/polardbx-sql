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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.hint.util.HintUtil;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;

import com.alibaba.fastjson.JSON;

import com.alibaba.polardbx.common.utils.TStringUtil;

/**
 * @author chenmo.cm
 */
public class HintCmdScan extends BaseHintOperator implements HintCmdOperator {

    private final String table;
    private final List<String> groups;
    private final String condition;
    private final List<List<String>> realTables;

    public HintCmdScan(SqlBasicCall hint, ExecutionContext ec) {
        super(hint, ec);

        String tmpTable = "";
        List<String> tmpGroups = new LinkedList<>();
        String tmpCondition = "";
        List<List<String>> tmpRealTables = new LinkedList<>();

        for (HintArgKey arg : getArgKeys()) {
            SqlNode value = this.argMap.get(arg);

            if (null == value) {
                continue;
            }

            switch (arg.ordinal) {
            case 0:
                tmpTable = RelUtils.stringValue(value);
                break;
            case 1:
                String str = RelUtils.stringValue(value);
                if (TStringUtil.isNotBlank(str)) {
                    tmpGroups = HintUtil.splitAndTrim(str, ",");
                }
                break;
            case 2:
                tmpCondition = RelUtils.stringValue(value);
                break;
            case 3:
                List<String> stringList = RelUtils.stringListValue(value);
                for (String realTables : stringList) {
                    if (TStringUtil.isNotBlank(realTables)) {
                        tmpRealTables.add(HintUtil.splitAndTrim(realTables, ","));
                    }
                }
                break;
            default:
                break;
            } // end of switch
        } // end of for

        this.table = tmpTable;
        this.groups = tmpGroups;
        this.condition = tmpCondition;
        this.realTables = tmpRealTables;
    }

    @Override
    protected List<HintArgKey> getArgKeys() {
        return HintArgKey.SCAN_HINT;
    }

    @Override
    public CmdBean handle(CmdBean current) {
        current.setScan(true);

        if (fullScan()) {
            List<String> result = HintUtil.allGroup();
            current.setGroups(result);
            return current;
        }

        current.setTable(this.table);

        if (realTables.size() > 0) {
            if (this.groups.size() > 0) {
                List<String> result = convertGroupIndex(current.getSchemaName(), this.groups, current);
                current.setGroups(result);
            } else {
                current.setGroups(HintUtil.allGroup());
            }

            current.setRealTable(realTables);
        } else if (this.groups.size() > 0) {
            List<String> result = convertGroupIndex(current.getSchemaName(), this.groups, current);

            current.setGroups(result);
        } else {
            current.setGroups(new LinkedList<String>());
            current.setCondition(this.condition);
        }

        return current;
    }

    private boolean fullScan() {
        return this.groups.size() <= 0 && TStringUtil.isBlank(this.table);
    }

    @Override
    public String oldStyle() {
        Map<String, Object> oldHint = new HashMap<>();
        if (fullScan()) {
            oldHint.put("type", "full");
        } else {
            List<String> groupList = this.groups;
            if (null == groupList || groupList.size() <= 0) {
                groupList = HintUtil.allGroup();
            }

            // scan(node="A_GROUP,B_GROUP")
            if (groupList.size() > 0) {
                oldHint.put("type", "direct");
                oldHint.put("dbid", TStringUtil.join(groupList, ","));
                oldHint.put("dbindex", "true");
                if (TStringUtil.isNotBlank(this.table) && this.realTables.size() > 0) {
                    oldHint.put("vtab", this.table);
                    List<String> realTabs = new LinkedList<>();
                    for (List<String> realTab : this.realTables) {
                        realTabs.add(TStringUtil.join(realTab, ","));
                    }
                    oldHint.put("realtabs", realTabs);
                }
            }
        }

        StringBuilder result = new StringBuilder("/*+TDDL(");
        result.append(JSON.toJSONString(oldHint));
        result.append(")*/");

        return result.toString();
    }
}
