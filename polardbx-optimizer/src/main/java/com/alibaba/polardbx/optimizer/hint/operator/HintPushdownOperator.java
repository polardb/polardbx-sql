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
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.hint.HintPlanner;
import com.alibaba.polardbx.optimizer.hint.util.HintUtil;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.utils.TStringUtil;

/**
 * @author chenmo.cm
 */
public class HintPushdownOperator extends BaseHintOperator {
    public final String table;
    public final List<String> groups;
    public final String condition;
    public final List<List<String>> realTables;
    public final boolean crossSingleTable;
    public final String qbName;

    public HintPushdownOperator(SqlBasicCall hint, ExecutionContext ec) {
        super(hint, ec);

        String tmpTable = null;
        String tmpCondition = null;
        List<String> tmpGroups = new LinkedList<>();
        List<List<String>> tmpRealTables = new LinkedList<>();
        boolean tmpCrossSingleTable = false;
        String tmpQbName = HintUtil.gerenateQbName();

        for (HintArgKey key : getArgKeys()) {
            SqlNode value = this.argMap.get(key);

            if (null == value) {
                continue;
            }

            switch (key.ordinal) {
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
            case 4:
                tmpCrossSingleTable = RelUtils.booleanValue(value);
                break;
            case 5:
                tmpQbName = RelUtils.stringValue(value);
                break;
            default:
                break;
            } // end of switch
        } // end of for

        this.table = tmpTable;
        this.groups = tmpGroups;
        this.condition = tmpCondition;
        this.realTables = tmpRealTables;
        this.crossSingleTable = tmpCrossSingleTable;
        this.qbName = tmpQbName;
    }

    @Override
    protected List<HintArgKey> getArgKeys() {
        return HintArgKey.PUSHDOWN_HINT;
    }

    public LogicalView handle(LogicalView lv,
                              Map<Integer, ParameterContext> param, ExecutionContext ec) {
        lv.setCrossSingleTable(crossSingleTable);

        if (TStringUtil.isNotBlank(condition)) {
            lv.setComparativeHintCache(buildComparative());
        } else if (realTables.size() > 0 || groups.size() > 0) {
            List<String> selectedGroups = convertGroupIndex(lv.getSchemaName(), groups);
            if (selectedGroups.size() <= 0) {
                selectedGroups = HintUtil.allGroup();
            }

            final Map<String, List<List<String>>> targetTable = new LinkedHashMap<>();
            if (realTables.size() > 0) {
                // real_table specified
                final List<String> vtNames = HintUtil.splitAndTrim(this.table, ",");
                final List<List<String>> realTables = new LinkedList<>();
                for (List<String> rtNames : this.realTables) {
                    List<String> phyTables = HintUtil.mergeTableNames(lv.getTableNames(), vtNames, rtNames);
                    realTables.add(phyTables);
                }

                for (String group : selectedGroups) {
                    targetTable.put(group, realTables);
                }
            } else {
                // real_table not specified, generate physical table name
                final Map<String, Map<String, Comparative>> comparatives = new HashMap<>();
                for (String tableName : lv.getTableNames()) {
                    comparatives.put(tableName, new HashMap<String, Comparative>());
                }

                final Map<String, List<List<String>>> tmpTargetTable =
                    HintUtil.buildTargetTables(lv.getTableNames(), comparatives,
                        param, lv.getSchemaName(), ec);
                for (String group : selectedGroups) {
                    if (tmpTargetTable.containsKey(group)) {
                        targetTable.put(group, tmpTargetTable.get(group));
                    }
                }
            }

            lv.setTargetTables(targetTable);
        } else {
            HintPlanner.rebuildComparative(lv, param, ec);
        }

        return lv;
    }

    public Map<String, Map<String, Comparative>> buildComparative() {
        return buildComparative(this.table, this.condition, this.paramIndexMap, null, ec);
    }
}
