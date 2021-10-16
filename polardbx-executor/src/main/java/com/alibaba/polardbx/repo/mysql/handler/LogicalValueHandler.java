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

import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalValues;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.LogicalValueCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;

import java.util.List;

/**
 * Created by chuanqin on 17/7/13.
 */
public class LogicalValueHandler extends HandlerCommon {

    public LogicalValueHandler() {
        super(null);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        LogicalValues logicalValues = (LogicalValues) logicalPlan;
        int count = ExecUtils.getTupleCount(logicalValues, executionContext);
        List<ColumnMeta> columns = CalciteUtils.buildColumnMeta(logicalPlan, "Value");
        return new LogicalValueCursor(count, columns);
    }
}
