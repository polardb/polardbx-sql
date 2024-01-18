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
import org.apache.calcite.rel.dal.Show;

import java.util.List;

/**
 * @author chenmo.cm
 */
public class LogicalShow extends LogicalDal {

    private boolean showForTruncateTable = false;

    private LogicalShow(Show show, String dbIndex,
                        String phyTable, String schemaName) {
        super(show, dbIndex, phyTable, schemaName);
    }

    public static LogicalShow create(Show show, String dbIndex, String phyTable, String schemaName) {
        return new LogicalShow(show, dbIndex, phyTable, schemaName);
    }

    @Override
    protected String getExplainName() {
        return "LogicalShow";
    }

    @Override
    public LogicalShow copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return create((Show) dal.copy(traitSet, inputs), dbIndex, phyTable, schemaName);
    }

    public boolean isShowForTruncateTable() {
        return showForTruncateTable;
    }

    public void setShowForTruncateTable(boolean showForTruncateTable) {
        this.showForTruncateTable = showForTruncateTable;
    }
}
