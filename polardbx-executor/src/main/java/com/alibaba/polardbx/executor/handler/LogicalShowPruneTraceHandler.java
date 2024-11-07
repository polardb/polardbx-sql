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

package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.statis.ColumnarPruneRecord;
import com.alibaba.polardbx.optimizer.statis.ColumnarTracer;
import org.apache.calcite.rel.RelNode;

import java.util.Collection;

/**
 * @author jilong.ljl
 */
public class LogicalShowPruneTraceHandler extends HandlerCommon {
    public LogicalShowPruneTraceHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        ArrayResultCursor result = new ArrayResultCursor("TRACE");
        result.addColumn("INSTANCE_ID", DataTypes.StringType);
        result.addColumn("TABLE_NAME", DataTypes.StringType);
        result.addColumn("FILTER", DataTypes.StringType);
        result.addColumn("INIT_TIME(NS)", DataTypes.StringType);
        result.addColumn("PRUNE_TIME(NS)", DataTypes.StringType);
        result.addColumn("FILE_NUM", DataTypes.StringType);
        result.addColumn("STRIPE_NUM", DataTypes.StringType);
        result.addColumn("RG_NUM", DataTypes.StringType);
        result.addColumn("RG_LEFT_NUM", DataTypes.StringType);
        result.addColumn("SORT_KEY_PRUNE_NUM", DataTypes.StringType);
        result.addColumn("ZONE_MAP_PRUNE_NUM", DataTypes.StringType);
        result.addColumn("BITMAP_PRUNE_NUM", DataTypes.StringType);

        result.initMeta();

        Collection<ColumnarPruneRecord> ops = null;
        ColumnarTracer columnarTracer = executionContext.getColumnarTracer();
        if (columnarTracer != null) {
            ops = columnarTracer.pruneRecords();
            for (ColumnarPruneRecord op : ops) {
                result.addRow(new Object[] {
                    columnarTracer.getInstanceId(),
                    op.getTableName(),
                    op.getFilter(),
                    op.initIndexTime,
                    op.indexPruneTime,
                    op.fileNum,
                    op.stripeNum,
                    op.rgNum,
                    op.rgLeftNum,
                    op.sortKeyPruneNum,
                    op.zoneMapPruneNum,
                    op.bitMapPruneNum});
            }
        }
        return result;
    }
}
