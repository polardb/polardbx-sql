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

package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.ddl.job.meta.AlterRuleMetaChanger;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlAlterRule;

public class LogicalAlterRuleHandler extends HandlerCommon {

    public LogicalAlterRuleHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        SqlAlterRule alterRule = (SqlAlterRule) ((LogicalAlterRule) logicalPlan).getNativeSqlNode();
        AlterRuleMetaChanger.alterRule(executionContext.getSchemaName(), alterRule);
        return new AffectRowCursor(new int[] {0});
    }

}
