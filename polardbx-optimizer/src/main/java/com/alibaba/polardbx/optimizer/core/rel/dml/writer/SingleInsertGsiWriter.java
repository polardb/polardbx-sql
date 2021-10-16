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

package com.alibaba.polardbx.optimizer.core.rel.dml.writer;

import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.dml.GsiWriter;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Writer for INSERT or REPLACE on single table
 *
 * @author chenmo.cm
 */
public class SingleInsertGsiWriter extends SingleInsertWriter implements GsiWriter {

    public SingleInsertGsiWriter(RelOptTable targetTable, LogicalInsert insert, TableMeta tableMeta,
                                 TableRule tableRule) {
        super(targetTable, insert, tableMeta, tableRule);
    }

    @Override
    public List<RelNode> getInput(ExecutionContext executionContext) {
        if (GlobalIndexMeta.canWrite(executionContext, this.tableMeta)) {
            return super.getInput(executionContext);
        }

        return new ArrayList<>();
    }

    @Override
    public TableMeta getGsiMeta() {
        return tableMeta;
    }

    @Override
    public boolean isGsiPublished(ExecutionContext ec) {
        return GlobalIndexMeta.isPublished(ec, getGsiMeta());
    }
}
