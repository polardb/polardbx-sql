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

package com.alibaba.polardbx.executor.ddl.job.task.storagepool;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.job.validator.StoragePoolValidator;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;

@Getter
@TaskName(name = "StorageInstValidateIdleTask")
public class StorageInstValidateIdleTask extends BaseValidateTask {

    private String instId;
    private List<String> validStorageInsts;
    private String schemaName;

    @JSONCreator
    public StorageInstValidateIdleTask(String schemaName, String instId, List<String> validStorageInsts) {
        super(schemaName);
        this.schemaName = schemaName;
        this.instId = instId;
        this.validStorageInsts = validStorageInsts;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        if (GeneralUtil.isEmpty(validStorageInsts)) {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, "the valid storage insts can't be empty");
        }
        StoragePoolValidator.validateStoragePool(instId, validStorageInsts);
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {

    }
}
