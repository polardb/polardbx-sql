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

package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.utils.AddressUtils;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.statis.SQLOperation;
import com.alibaba.polardbx.optimizer.view.InformationSchemaTrace;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import com.google.common.collect.ImmutableList;

import java.text.DecimalFormat;
import java.util.List;

public class InformationSchemaTraceHandler extends BaseVirtualViewSubClassHandler {

    private static final Logger logger = LoggerFactory.getLogger(InformationSchemaTraceHandler.class);

    public InformationSchemaTraceHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return (virtualView instanceof InformationSchemaTrace);

    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {

        List<SQLOperation> ops = null;
        Long startTimestamp = null;
        int index = 0;
        if (executionContext.getTracer() != null) {
            ops = ImmutableList.copyOf(executionContext.getTracer().getOperations());
            String hostIp = AddressUtils.getHostIp();
            for (SQLOperation op : ops) {
                if (startTimestamp == null) {
                    startTimestamp = op.getTimestamp();
                }

                int id = index++;
                String nodeIp = hostIp;
                Long timestamp = op.getTimestamp() - startTimestamp;
                String opType = op.getOperationType();
                String groupName = op.getGroupName();
                String dbKeyName = op.getDbKeyName();

                Long timeCost = op.getTimeCost();
                Float phyCloseTimeCost = op.getGetConnectionTimeCost();
                Long totalTimeCost = op.getTotalTimeCost();
                Long closeTimeCost = op.getPhysicalCloseCost();
                Long rows = op.getRowsCount();

                String stmt = op.getSqlOrResult();
                String params = op.getParamsStr();
                Long grpConnId = op.getGrpConnId();
                String traceId = op.getTraceId();

                cursor.addRow(new Object[] {

                    id,
                    nodeIp,
                    timestamp,
                    opType,
                    groupName,
                    dbKeyName,

                    timeCost,
                    scale(phyCloseTimeCost),
                    totalTimeCost,
                    closeTimeCost,
                    rows,

                    stmt,
                    params,
                    grpConnId,
                    traceId
                });
            }
        }

        return cursor;
    }

    private float scale(Float floatValue) {
        DecimalFormat format = new DecimalFormat("#.00");
        String scaled = format.format(floatValue);
        return Float.parseFloat(scaled);
    }

    private double scale(Double doubleValue) {
        DecimalFormat format = new DecimalFormat("#.00");
        String scaled = format.format(doubleValue);
        return Double.parseDouble(scaled);
    }

}
