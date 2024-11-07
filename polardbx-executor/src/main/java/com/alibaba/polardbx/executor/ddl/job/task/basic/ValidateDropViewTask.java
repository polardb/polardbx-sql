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

package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.ttl.TtlInfoAccessor;
import com.alibaba.polardbx.gms.ttl.TtlInfoRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.SystemTableView;
import lombok.Getter;

import java.sql.Connection;

@TaskName(name = "ValidateDropViewTask")
@Getter
public class ValidateDropViewTask extends BaseValidateTask {

    final private String viewName;
    protected Boolean ifExists;

    @JSONCreator
    public ValidateDropViewTask(String schemaName, String viewName, boolean ifExists) {
        super(schemaName);
        this.viewName = viewName;
        this.ifExists = ifExists;
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        if (!ifExists) {
            SystemTableView.Row row = OptimizerContext.getContext(schemaName).getViewManager().select(viewName);
            if (row == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_VIEW, "Unknown view " + viewName);
            }

            /**
             * Check if the view is created by  archive table based on cci of ttl2.0
             */
            try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
                TableInfoManager tableInfoManager = new TableInfoManager();
                tableInfoManager.setConnection(metaDbConn);
                TtlInfoRecord ttlInfoRec = tableInfoManager.getTtlInfoRecordByArchiveTable(schemaName, viewName);
                if (ttlInfoRec != null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_TTL,
                        String.format("Dropping a view of archive table `%s` is not allowed", viewName));
                }
            } catch (Throwable ex) {
                throw GeneralUtil.nestedException(ex);
            }
        }
    }

    @Override
    protected String remark() {
        return "|schema: " + schemaName + " viewName: " + viewName + " ifExists: " + ifExists;
    }
}
