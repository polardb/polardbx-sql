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
public interface IRuleGen {

    // 用于标记分库规则是否用了了standAlone模式
    public static final int GROUP_COUNT_OF_USE_STANT_ALONE = -1;

    // 用于标记分表规则是否用了了standAlone模式
    public static final int TABLE_COUNT_OF_USE_STANT_ALONE = -1;

    public String generateDbRuleArrayStr(PartitionByType type, String idText, int start, int groups, int tablesPerGroup,
                                         DBPartitionDefinition dbPartitionDefinition);

    public String generateStandAloneDbRuleArrayStr(PartitionByType type, String idText, int start, int groups,
                                                   DBPartitionDefinition dbPartitionDefinition);

    public String generateTbRuleArrayStr(PartitionByType type, String idText, int start, int groups, int tablesPerGroup,
                                         TBPartitionDefinition tbPartitionDefinition);

    public String generateStandAloneTbRuleArrayStr(PartitionByType type, String idText, int start, int tablesPerGroup,
                                                   TBPartitionDefinition tbPartitionDefinition);

}
