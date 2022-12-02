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

import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.utils.AddressUtils;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.statis.SQLOperation;
import org.apache.calcite.rel.RelNode;

import java.text.DecimalFormat;
import java.util.List;

/**
 * @author chenmo.cm
 */
public class LogicalShowTraceHandler extends HandlerCommon {
    public LogicalShowTraceHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        ArrayResultCursor result = new ArrayResultCursor("TRACE");
        result.addColumn("ID", DataTypes.IntegerType);
        result.addColumn("NODE_IP", DataTypes.StringType);
        result.addColumn("TIMESTAMP", DataTypes.DoubleType);
        result.addColumn("TYPE", DataTypes.StringType);
        result.addColumn("GROUP_NAME", DataTypes.StringType);
        result.addColumn("DBKEY_NAME", DataTypes.StringType);
        // result.addColumn("THREAD", DataType.StringType);

        result.addColumn("TIME_COST(ms)", DataTypes.StringType);
        result.addColumn("CONNECTION_TIME_COST(ms)", DataTypes.StringType);
        result.addColumn("TOTAL_TIME_COST(ms)", DataTypes.StringType);
        result.addColumn("CLOSE_TIME_COST(ms)", DataTypes.StringType);
        result.addColumn("ROWS", DataTypes.LongType);

        result.addColumn("STATEMENT", DataTypes.StringType);
        result.addColumn("PARAMS", DataTypes.StringType);
        result.addColumn("GROUP_CONN_ID", DataTypes.IntegerType);
        result.addColumn("TRACE_ID", DataTypes.StringType);
        result.initMeta();
        int index = 0;

        java.text.DecimalFormat df = new java.text.DecimalFormat("0.00");
        List<SQLOperation> ops = null;
        Long startTimestamp = null;
        if (executionContext.getTracer() != null) {
            ops = ImmutableList.copyOf(executionContext.getTracer().getOperations());
            String hostIp = AddressUtils.getHostIp();
            for (SQLOperation op : ops) {
                if (startTimestamp == null) {
                    startTimestamp = op.getTimestamp();
                }
                result.addRow(new Object[] {
                    index++,
                    hostIp,
                    new DecimalFormat("0.000").format((op.getTimestamp() - startTimestamp) / (float) (1000 * 1000)),
                    op.getOperationType(), op.getGroupName(), op.getDbKeyName(), op.getTimeCost(),
                    df.format(op.getGetConnectionTimeCost()),
                    op.getTotalTimeCost(),
                    op.getPhysicalCloseCost(),
                    op.getRowsCount(), op.getSqlOrResult(),
                    op.getParamsStr(),
                    op.getGrpConnId(),
                    op.getTraceId()});
            }
        }

        return result;
    }
}
