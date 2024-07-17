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

package com.alibaba.polardbx.executor.ddl.job.task.columnar;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import lombok.Setter;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.sql.Connection;

@TaskName(name = "CreateMockColumnarIndexTask")
@Getter
@Setter
public class CreateMockColumnarIndexTask extends BaseDdlTask {

    private final static String HANDLER_CLASS =
        "com.alibaba.polardbx.columnar.core.ddl.handler.CreateMockColumnarIndexHandle";

    private final static String HANDLER_METHOD = "handle";

    private String tableName;

    private String mciFormat;

    private long ddlId;

    @JSONCreator
    public CreateMockColumnarIndexTask(String schemaName, String tableName, long ddlId) {
        super(schemaName);
        this.tableName = tableName;
        this.ddlId = ddlId;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        Method method = null;
        Object handler = null;
        try {
            Class clazz = Class.forName(HANDLER_CLASS);
            Constructor constructor = clazz.getConstructor(String.class, String.class, long.class, String.class);
            handler = constructor.newInstance(schemaName, tableName, ddlId, mciFormat);
            method = clazz.getMethod(HANDLER_METHOD);
        } catch (Exception e) {
            throw new UnsupportedOperationException("create mock columnar index is unsupported", e);
        }

        try {
            method.invoke(handler);
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
    }

}
