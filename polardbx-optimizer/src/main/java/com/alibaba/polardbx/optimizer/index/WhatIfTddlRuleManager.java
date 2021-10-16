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

package com.alibaba.polardbx.optimizer.index;

import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.TddlRule;
import com.alibaba.polardbx.rule.VirtualTableRoot;

/**
 * @author dylan
 */
public class WhatIfTddlRuleManager extends TddlRuleManager {

    public WhatIfTddlRuleManager(TddlRule tddlRule, WhatIfPartitionInfoManager whatIfPartitionInfoManager,
                                 WhatIfTableGroupInfoManager whatIfTableGroupInfoManager, String schemaName) {
        super(tddlRule, whatIfPartitionInfoManager, whatIfTableGroupInfoManager, schemaName);
        tddlRule.setAllowEmptyRule(true);
    }

    @Override
    public TableRule getTableRule(String logicTable) {
        return super.getTableRule(logicTable);
    }

    public void addTableRule(String logicalTable, TableRule tableRule) {
        if (ConfigDataMode.isMock()) {
            VirtualTableRoot.setTestRule(logicalTable, tableRule);
        } else {
            getTddlRule().getCurrentRule().getTableRules().put(logicalTable, tableRule);
        }
    }
}
