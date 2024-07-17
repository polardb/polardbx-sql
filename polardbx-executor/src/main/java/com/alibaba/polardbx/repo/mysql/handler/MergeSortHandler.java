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

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.GatherCursor;
import com.alibaba.polardbx.executor.cursor.impl.MergeSortCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.OrderByOption;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.MergeSort;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.weakref.jmx.internal.guava.collect.Lists;

import java.util.List;
import java.util.Map;

/**
 * Created by chuanqin on 17/8/1.
 */
public class MergeSortHandler extends HandlerCommon {

    public MergeSortHandler() {
        super(null);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        MergeSort mergeSort = (MergeSort) logicalPlan;
        long skip = 0;
        long fetch = Long.MAX_VALUE;
        List<RelFieldCollation> sortList = mergeSort.getCollation().getFieldCollations();
        List<OrderByOption> orderBys = ExecUtils.convertFrom(sortList);
        Map<Integer, ParameterContext> params = executionContext.getParams().getCurrentParameter();
        if (mergeSort.fetch != null) {
            if (mergeSort.fetch instanceof RexDynamicParam) {
                fetch = Long.parseLong(
                    String.valueOf(params.get(((RexDynamicParam) mergeSort.fetch).getIndex() + 1).getValue()));
            } else {
                fetch = DataTypes.LongType.convertFrom(((RexLiteral) mergeSort.fetch).getValue());
            }

            if (mergeSort.offset != null) {
                if (mergeSort.offset instanceof RexDynamicParam) {
                    skip = Long.parseLong(
                        String.valueOf(params.get(((RexDynamicParam) mergeSort.offset).getIndex() + 1).getValue()));
                } else {
                    skip = DataTypes.LongType.convertFrom(((RexLiteral) mergeSort.offset).getValue());
                }
            }
        }

        Cursor inputCursor = ExecutorHelper.executeByCursor(mergeSort.getInput(), executionContext, false);

        if (inputCursor instanceof GatherCursor) {
            return new MergeSortCursor(
                executionContext, ((GatherCursor) inputCursor).getCursors(), orderBys, skip, fetch);
        } else {
            return new MergeSortCursor(
                executionContext, Lists.newArrayList(inputCursor), orderBys, skip, fetch);
        }
    }
}
