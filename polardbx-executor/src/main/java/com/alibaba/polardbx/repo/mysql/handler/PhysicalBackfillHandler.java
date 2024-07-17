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

package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.physicalbackfill.PhysicalBackfillExecutor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalBackfill;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
import org.apache.calcite.rel.RelNode;

import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.executor.utils.ExecUtils.getQueryConcurrencyPolicy;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class PhysicalBackfillHandler extends HandlerCommon {

    public PhysicalBackfillHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        PhysicalBackfill backfill = (PhysicalBackfill) logicalPlan;
        String schemaName = backfill.getSchemaName();
        String logicalTable = backfill.getLogicalTableName();

        PhysicalBackfillExecutor backfillExecutor = new PhysicalBackfillExecutor();

        executionContext = clearSqlMode(executionContext.copy());

        upgradeEncoding(executionContext, schemaName, logicalTable);

        PhyTableOperationUtil.disableIntraGroupParallelism(schemaName, executionContext);

        Map<String, Set<String>> sourcePhyTables = backfill.getSourcePhyTables();
        Map<String, Set<String>> targetPhyTables = backfill.getTargetPhyTables();
        Map<String, String> sourceTargetGroup = backfill.getSourceTargetGroup();
        boolean isBroadcast = backfill.getBroadcast();
        backfillExecutor.backfill(schemaName, logicalTable, sourcePhyTables, targetPhyTables, sourceTargetGroup,
            isBroadcast, executionContext);
        return new AffectRowCursor(0);
    }
}
