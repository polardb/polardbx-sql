/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.hint.operator;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.hint.util.HintUtil;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.base.Preconditions;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Push query to random selected node list
 *
 * <pre>
 * RANDOM_NODE() : randomly select one group from all groups
 * RANDOM_NODE("2") : equals to NODE("2")
 * RANDOM_NODE(lst="0,1,2")
 * RANDOM_NODE("0,1,2") : randomly select one group from groups [0,1,2]
 * RANDOM_NODE(lst="0,1,2", num=2)
 * RANDOM_NODE("0,1,2", 2) : randomly select two group from groups [0,1,2]
 * RANDOM_NODE(num=2) : randomly select two group from all groups
 * </pre>
 * <p>
 * The actually selected node count might be smaller than specified because following reasons:
 * <pre>
 * A value more than the number of groups is specified
 * For partition table, skip single group by default otherwise may cause "Table doesn't exist" exception on DN.
 * For single table, there might be only one candidate group
 * </pre>
 */
public class HintCmdRandomNode extends BaseHintOperator implements HintCmdOperator {

    private final List<String> lst;
    private final int num;
    private List<String> lstConverted;
    private List<String> groups;
    private String currentSchema;

    public HintCmdRandomNode(SqlBasicCall hint, ExecutionContext ec) {
        super(hint, ec);

        List<String> tmpNodeList = null;
        int tmpReturnNumber = 0;

        for (HintArgKey arg : getArgKeys()) {
            SqlNode value = this.argMap.get(arg);

            if (null == value) {
                continue;
            }

            switch (arg.ordinal) {
            case 0:
                String str = RelUtils.stringValue(value);
                if (TStringUtil.isNotBlank(str)) {
                    tmpNodeList = HintUtil.splitAndTrim(str, ",");
                }
                break;
            case 1:
                tmpReturnNumber = RelUtils.integerValue((SqlLiteral) value);
                break;
            default:
                break;
            } // end of switch
        } // end of for

        this.lst = tmpNodeList;
        this.num = tmpReturnNumber;
    }

    @Override
    protected List<HintArgKey> getArgKeys() {
        return HintArgKey.RANDOM_NODE_HINT;
    }

    @Override
    public CmdBean handle(CmdBean current) {
        if (null == this.currentSchema || !TStringUtil.equals(current.getSchemaName(), this.currentSchema)) {
            reinit(current.getSchemaName(), false);
        }

        // Do not support group name in ${ExecutorAttribute}
        current.setGroups(this.groups);

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

    public void reinit(String schemaName, boolean forSingleTable) {
        final List<String> allGroups = HintUtil.allGroup(schemaName);
        this.lstConverted = Optional.ofNullable(this.lst)
            .map(l -> l
                .stream()
                .map(group ->
                    // Convert group index to group name, so that we can find out whether it's a single group
                    convertGroupIndexToName(allGroups::get, group)).collect(Collectors.toList()))
            .orElse(null);

        this.groups = reselectGroups(this.lstConverted, schemaName, forSingleTable);
        this.currentSchema = schemaName;
    }

    public List<String> reselectGroups(List<String> specifiedGroups, String schemaName, boolean forSingleTable) {
        final List<String> groupCandidates = Optional
            .ofNullable(specifiedGroups)
            .map(l -> l
                .stream()
                .filter(group ->
                    forSingleTable == GroupInfoUtil.isSingleGroup(group))
                .collect(Collectors.toList()))
            .orElseGet(() ->
                HintUtil.allGroup(schemaName, forSingleTable));

        // Make sure 0 < num <= groupCandidates.size()
        final int returnNumber = Optional.of(this.num).map(num -> {
            if (num < 1) {
                num = 1;
            }
            if (num > groupCandidates.size()) {
                return groupCandidates.size();
            } else {
                return num;
            }
        }).get();

        // Build random group list
        final Random random = new Random();
        final Set<Integer> selected = new HashSet<>();
        while (selected.size() < returnNumber) {
            selected.add(random.nextInt(groupCandidates.size()));
        }

        // Store selected groups
        return selected.stream()
            .map(groupCandidates::get)
            .collect(Collectors.toList());
    }

    public boolean isGroupSpecified() {
        Preconditions.checkArgument(null != currentSchema);
        return !Optional
            .ofNullable(this.lstConverted)
            .map(List::isEmpty)
            .orElse(true);
    }

    /**
     * Whether user specified groups are all single group
     *
     * @return true if user has specified groups and all groups are single group
     */
    public boolean isAllGroupSingle() {
        Preconditions.checkArgument(null != currentSchema);
        return Optional
            .ofNullable(this.lstConverted)
            .map(g -> g
                .stream()
                .allMatch(GroupInfoUtil::isSingleGroup))
            .orElse(false);
    }
}
