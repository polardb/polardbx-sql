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
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Properties;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
@Getter
@TaskName(name = "ChangeInstanceReadonlyStatusTask")
public class ChangeInstanceReadonlyStatusTask extends BaseDdlTask {

    private final static Logger logger = LoggerFactory.getLogger(ChangeInstanceReadonlyStatusTask.class);

    protected boolean readonly;

    @JSONCreator
    public ChangeInstanceReadonlyStatusTask(String schemaName, boolean readonly) {
        super(schemaName);
        this.readonly = readonly;
        onExceptionTryRecoveryThenPause();
    }

    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        try {
            setGlobal(readonly);
        } catch (SQLException e) {
            logger.error(MessageFormat.format("set instance readonly {0} failed", readonly), e);
            throw new TddlRuntimeException(ErrorCode.ERR_INSTANCE_READ_ONLY_OPTION_SET_FAILED, readonly + "");
        }
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        try {
            setGlobal(!readonly);
        } catch (SQLException e) {
            logger.error(MessageFormat.format("rollback instance_read_only to {0} failed!", !readonly), e);
        }
    }

    private void setGlobal(boolean value) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(ConnectionProperties.INSTANCE_READ_ONLY, String.valueOf(value));
        MetaDbUtil.setGlobal(properties);
        // Wait until global value propagates to all CN
        String error = ExecUtils.waitVarChange("instanceReadOnly", String.valueOf(value), 10);
        if (null != error) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "set global readonly failed, caused by " + error);
        }
    }
}
