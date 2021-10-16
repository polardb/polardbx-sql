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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalSequenceDdl;
import com.alibaba.polardbx.repo.mysql.handler.SequenceSingleHandler;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SequenceBean;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSequence;

public class LogicalSequenceDdlHandler extends HandlerCommon {

    public LogicalSequenceDdlHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalSequenceDdl sequenceDdl = (LogicalSequenceDdl) logicalPlan;

        SqlNode sqlNode = sequenceDdl.relDdl.sqlNode;
        if (sqlNode instanceof SqlSequence) {
            SequenceBean sequenceBean = ((SqlSequence) sqlNode).getSequenceBean();

            PhyDdlTableOperation phyDdlTableOperation = new PhyDdlTableOperation(sequenceDdl.relDdl);
            phyDdlTableOperation.setKind(sqlNode.getKind());
            phyDdlTableOperation.setSequence(sequenceBean);

            return new SequenceSingleHandler(repo).handle(phyDdlTableOperation, executionContext);
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_SEQUENCE,
                "unsupported sequence operation: " + sqlNode.getKind());
        }
    }

}
