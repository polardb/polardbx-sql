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

package com.alibaba.polardbx.repo.mysql.handler.ddl.newengine;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import org.apache.calcite.sql.SqlRollbackDdlJob;

import static com.alibaba.polardbx.common.ddl.newengine.DdlPlanState.SUCCESS;
import static com.alibaba.polardbx.common.ddl.newengine.DdlPlanState.TERMINATED;
import static com.alibaba.polardbx.common.ddl.newengine.DdlType.ALTER_TABLEGROUP;
import static com.alibaba.polardbx.common.ddl.newengine.DdlType.MOVE_DATABASE;
import static com.alibaba.polardbx.common.ddl.newengine.DdlType.REBALANCE;

public class DdlEngineRollbackJobsHandler extends DdlEngineCancelJobsHandler {

    public DdlEngineRollbackJobsHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor doHandle(final LogicalDal logicalPlan, ExecutionContext executionContext) {
        SqlRollbackDdlJob command = (SqlRollbackDdlJob) logicalPlan.getNativeSqlNode();

        if (command.isAll()) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, "Operation on multi ddl jobs is not allowed");
        }

        if (command.getJobIds() == null || command.getJobIds().isEmpty()) {
            return new AffectRowCursor(0);
        }

        if (command.getJobIds().size() > 1) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, "Operation on multi ddl jobs is not allowed");
        }

        return doCancel(command.getJobIds().get(0), executionContext);
    }

}
