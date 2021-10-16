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

package com.alibaba.polardbx.optimizer.utils.newrule;

import com.alibaba.polardbx.optimizer.parse.bean.DBPartitionDefinition;
import com.alibaba.polardbx.optimizer.parse.bean.TBPartitionDefinition;
import com.alibaba.polardbx.rule.ddl.PartitionByType;

/**
 * @author agapple 2015年3月10日 下午10:26:45
 * @since 5.1.18
 */
public class BinaryRuleGen implements IRuleGen {

    @Override
    public String generateDbRuleArrayStr(PartitionByType type, String idText, int start, int groups, int tablesPerGroup,
                                         DBPartitionDefinition dbPartitionDefinition) {
        if (type != PartitionByType.HASH) {
            throw new IllegalArgumentException("Binary column only support hash method");
        }

        int totalTables = groups * tablesPerGroup;

        StringBuffer rule = new StringBuffer();

        rule.append("(Arrays.hashCode(");
        rule.append("#").append(idText).append(",");
        rule.append(start).append(",");
        rule.append(totalTables).append("#");
        rule.append(").abs().longValue() % ").append(totalTables).append(")");
        rule.append(".intdiv(").append(tablesPerGroup).append(")");
        return rule.toString();
    }

    @Override
    public String generateStandAloneDbRuleArrayStr(PartitionByType type, String idText, int start, int groups,
                                                   DBPartitionDefinition dbPartitionDefinition) {
        if (type != PartitionByType.HASH) {
            throw new IllegalArgumentException("Binary column only support hash method");
        }

        StringBuffer rule = new StringBuffer();

        rule.append("(Arrays.hashCode(");
        rule.append("#").append(idText).append(",");
        rule.append(start).append(",");
        rule.append(groups).append("#");
        rule.append(").abs().longValue() % ").append(groups).append(")");
        return rule.toString();
    }

    /**
     * Math.abs(#key,start,物理总表数#.hashCode()) % 物理总表数
     * ((#key,1,物理总表数#.hashCode().abs().longValue() % 物理总表数)
     */
    @Override
    public String generateTbRuleArrayStr(PartitionByType type, String idText, int groups, int start, int tablesPerGroup,
                                         TBPartitionDefinition tbPartitionDefinition) {
        if (type != PartitionByType.HASH) {
            throw new IllegalArgumentException("Binary column only support hash method");
        }

        StringBuffer rule = new StringBuffer();

        int totalTables = tablesPerGroup * groups;
        rule.append("(Arrays.hashCode(");
        rule.append("#").append(idText).append(",");
        rule.append(start).append(",");
        rule.append(totalTables).append("#");
        rule.append(").abs().longValue() % ").append(totalTables).append(")");
        return rule.toString();
    }

    @Override
    public String generateStandAloneTbRuleArrayStr(PartitionByType type, String idText, int start, int tablesPerGroup,
                                                   TBPartitionDefinition tbPartitionDefinition) {
        if (type != PartitionByType.HASH) {
            throw new IllegalArgumentException("Binary column only support hash method");
        }

        StringBuffer rule = new StringBuffer();

        rule.append("(Arrays.hashCode(");
        rule.append("#").append(idText).append(",");
        rule.append(start).append(",");
        rule.append(tablesPerGroup).append("#");
        rule.append(").abs().longValue() % ").append(tablesPerGroup).append(")");
        return rule.toString();
    }
}
