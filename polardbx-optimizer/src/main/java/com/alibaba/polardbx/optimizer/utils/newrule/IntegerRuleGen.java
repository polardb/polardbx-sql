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
 * Created by simiao on 14-12-2.
 */
public class IntegerRuleGen implements IRuleGen {

    /**
     * ((#key,1,物理总表数#).longValue() % 物理总表数).intdiv(每个库的分表数) 分库必须用<property
     * name="dbRuleArray" value="(#pk,1,20#.longValue().abs() % 20).intdiv(5)"/>
     * 而不能用<property name="dbRuleArray" value="#pk,1,4#.longValue().abs() % 4"/>
     * 第一种的拓扑是0库放0001,0002,0003,0004 1库放0005,0006,0007,0008
     * 第二种的拓扑是0库放0001,0004,0007,0010 1库放0002,0005,0008,0011
     * 因为要与内部分的拓扑不同所以一定要用第一种分法
     */
    @Override
    public String generateDbRuleArrayStr(PartitionByType type, String idText, int start, int groups, int tablesPerGroup,
                                         DBPartitionDefinition dbPartitionDefinition) {
        if (type != PartitionByType.HASH) {
            throw new IllegalArgumentException("Integer column only support hash method");
        }

        int totalTables = groups * tablesPerGroup;

        StringBuffer rule = new StringBuffer();

        rule.append("((");
        rule.append("#").append(idText).append(",");
        rule.append(start).append(",");
        rule.append(totalTables).append("#");
        rule.append(").longValue().abs() % ").append(totalTables).append(")");
        rule.append(".intdiv(").append(tablesPerGroup).append(")");
        return rule.toString();
    }

    @Override
    public String generateStandAloneDbRuleArrayStr(PartitionByType type, String idText, int start, int groups,
                                                   DBPartitionDefinition dbPartitionDefinition) {
        if (type != PartitionByType.HASH) {
            throw new IllegalArgumentException("Integer column only support hash method");
        }

        StringBuffer rule = new StringBuffer();

        rule.append("((");
        rule.append("#").append(idText).append(",");
        rule.append(start).append(",");
        rule.append(groups).append("#");
        rule.append(").longValue().abs() % ").append(groups).append(")");
        return rule.toString();
    }

    /**
     * ((#key,1,物理总表数#.longValue() % 物理总表数)
     */
    @Override
    public String generateTbRuleArrayStr(PartitionByType type, String idText, int start, int groups, int tablesPerGroup,
                                         TBPartitionDefinition tbPartitionDefinition) {
        if (type != PartitionByType.HASH) {
            throw new IllegalArgumentException("Integer column only support hash method");
        }

        StringBuffer rule = new StringBuffer();
        int totalTables = tablesPerGroup * groups;

        rule.append("((");
        rule.append("#").append(idText).append(",");
        rule.append(start).append(",");
        rule.append(totalTables).append("#");
        rule.append(").longValue().abs() % ").append(totalTables).append(")");
        return rule.toString();
    }

    @Override
    public String generateStandAloneTbRuleArrayStr(PartitionByType type, String idText, int start, int tablesPerGroup,
                                                   TBPartitionDefinition tbPartitionDefinition) {
        if (type != PartitionByType.HASH) {
            throw new IllegalArgumentException("Integer column only support hash method");
        }

        StringBuffer rule = new StringBuffer();

        rule.append("((");
        rule.append("#").append(idText).append(",");
        rule.append(start).append(",");
        rule.append(tablesPerGroup).append("#");
        rule.append(").longValue().abs() % ").append(tablesPerGroup).append(")");
        return rule.toString();
    }
}
