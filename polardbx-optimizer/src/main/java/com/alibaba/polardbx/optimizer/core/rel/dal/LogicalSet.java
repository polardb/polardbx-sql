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

package com.alibaba.polardbx.optimizer.core.rel.dal;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.dal.Dal;

import java.util.List;

/**
 * @author chenmo.cm
 * @date 2018/6/18 下午3:36
 */
public class LogicalSet extends LogicalDal {

    private LogicalSet(Dal dal, String dbIndex,
                      String phyTable){
        super(dal, dbIndex, phyTable, null);
    }

    public static LogicalSet create(Dal dal, String dbIndex, String phyTable) {
        return new LogicalSet(dal, dbIndex, phyTable);
    }

    @Override
    protected String getExplainName() {
        return "LogicalSet";
    }

    @Override
    public LogicalSet copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return create(dal.copy(traitSet, inputs), dbIndex, phyTable);
    }
}
