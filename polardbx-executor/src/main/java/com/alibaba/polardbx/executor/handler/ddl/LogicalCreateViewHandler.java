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
import com.alibaba.polardbx.executor.ddl.job.factory.CreateViewJobFactory;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateView;
import com.alibaba.polardbx.optimizer.view.ViewManager;

import java.io.UnsupportedEncodingException;

import static com.alibaba.polardbx.common.properties.ConnectionParams.ENABLE_CREATE_VIEW;

/**
 * @author dylan
 */
public class LogicalCreateViewHandler extends LogicalCommonDdlHandler {
    private static final int MAX_VIEW_NAME_LENGTH = 64;

    public static final int MAX_VIEW_NUMBER = 10000;

    public LogicalCreateViewHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        return new CreateViewJobFactory((LogicalCreateView) logicalDdlPlan, executionContext).create();
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        if (!InstConfUtil.getBool(ENABLE_CREATE_VIEW)) {
            throw new TddlRuntimeException(ErrorCode.ERR_VIEW, "CREATE VIEW is not ENABLED");
        }

        LogicalCreateView logicalCreateView = (LogicalCreateView) logicalDdlPlan;
        String schemaName = logicalCreateView.getSchemaName();
        String viewName = logicalCreateView.getTableName();
        boolean isReplace = logicalCreateView.isReplace();

        // Notice, since we reused the logic of new ddl engine, so we should validate view name as table name
        TableValidator.validateTableName(viewName);
        TableValidator.validateTableNameLength(viewName);

        if (!checkUtf8(viewName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_VIEW,
                "View name should be encoded as utf8");
        } else if (viewName.length() > MAX_VIEW_NAME_LENGTH) {
            throw new TddlRuntimeException(ErrorCode.ERR_VIEW,
                "View name length " + viewName.length() + " > MAX_VIEW_NAME_LENGTH " + MAX_VIEW_NAME_LENGTH);
        }

        ViewManager viewManager = OptimizerContext.getContext(schemaName).getViewManager();
        if (viewManager.count(schemaName) > MAX_VIEW_NUMBER) {
            throw new TddlRuntimeException(ErrorCode.ERR_VIEW, "View number at most " + MAX_VIEW_NUMBER);
        }
        // check view name
        TableMeta tableMeta;
        try {
            tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(viewName);
        } catch (Throwable throwable) {
            // pass
            tableMeta = null;
        }

        if (tableMeta != null) {
            if (isReplace) {
                throw new TddlRuntimeException(ErrorCode.ERR_VIEW, "'" + viewName + "' is not VIEW ");
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_VIEW, "table '" + viewName + "' already exists ");
            }
        }
        return super.validatePlan(logicalDdlPlan, executionContext);
    }

    private boolean checkUtf8(String s) {
        try {
            s.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            return false;
        }
        return true;
    }
}

