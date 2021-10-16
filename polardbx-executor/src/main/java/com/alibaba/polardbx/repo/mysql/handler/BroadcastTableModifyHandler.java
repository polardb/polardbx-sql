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

import java.util.ArrayList;
import java.util.List;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ExecutorHelper;
import org.apache.calcite.rel.RelNode;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.core.rel.BroadcastTableModify;

public class BroadcastTableModifyHandler extends HandlerCommon {

    public BroadcastTableModifyHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        BroadcastTableModify broadcastTableModify = (BroadcastTableModify) logicalPlan;
        List<RelNode> inputs = broadcastTableModify.getInputs(executionContext);
        List<Cursor> inputCursors = new ArrayList<>(inputs.size());
        boolean partialFinished = false;
        try {
            for (RelNode inputNode : inputs) {
                inputCursors.add(ExecutorHelper.execute(inputNode, executionContext));
                partialFinished = true;
            }

            int affectRows = 0;
            for (Cursor inputCursor : inputCursors) {
                affectRows = inputCursor.next().getInteger(0);
            }
            return new AffectRowCursor(affectRows);
        } catch (Exception e) {
            if (partialFinished) {
                executionContext.getTransaction().setCrucialError(ErrorCode.ERR_TRANS_CONTINUE_AFTER_WRITE_FAIL);
            }
            throw new TddlNestableRuntimeException(e);
        }
    }
}
