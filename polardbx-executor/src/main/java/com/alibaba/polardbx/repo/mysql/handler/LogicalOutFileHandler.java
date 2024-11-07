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

import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.OutFileCursor;
import com.alibaba.polardbx.executor.cursor.impl.OutFileStatisticsCursor;
import com.alibaba.polardbx.executor.cursor.impl.OutOrcFileCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.operator.spill.AsyncFileSingleBufferSpiller;
import com.alibaba.polardbx.executor.operator.spill.OrcWriter;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalOutFile;
import org.apache.calcite.sql.OutFileParams;

public class LogicalOutFileHandler extends HandlerCommon {
    public LogicalOutFileHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        OutFileParams outFileParams = ((LogicalOutFile) logicalPlan).getOutFileParams();
        if (logicalPlan.getInputs().size() != 1) {
            throw new TddlRuntimeException(ErrorCode.ERR_DATA_OUTPUT,
                "OutFileCursor cannot be implemented when the inputs more than one");
        }

        if (!executionContext.isGod()) {
            if (!InstConfUtil.getValBool(TddlConstants.ENABLE_SELECT_INTO_OUTFILE)) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPERATION_NOT_ALLOWED,
                    "Selecting into outfile is not enabled!");
            }
        }

        if (((LogicalOutFile) logicalPlan).getOutFileParams().getStatistics()) {
            return new OutFileStatisticsCursor(executionContext,
                ServiceProvider.getInstance().getServer().getSpillerFactory(),
                ((LogicalOutFile) logicalPlan).getOutFileParams());
        }

        RelNode inputRelNode = logicalPlan.getInput(0);
        Cursor cursor = ExecutorHelper.execute(inputRelNode, executionContext, false);

        String orcSuffix = ".orc";
        if (orcSuffix.equals(
            outFileParams.getFileName()
                .substring(Math.max(0, outFileParams.getFileName().length() - orcSuffix.length())))) {
            return new OutOrcFileCursor(executionContext, cursor, outFileParams);
        }

        return new OutFileCursor(executionContext, ServiceProvider.getInstance().getServer().getSpillerFactory(),
            cursor, outFileParams);
    }
}
