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

package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineRequester;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.pl.ProcedureStatus;
import com.alibaba.polardbx.executor.pl.RuntimeFunction;
import com.alibaba.polardbx.executor.pl.RuntimeFunctionManager;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.gms.privilege.PolarPrivUtil;
import com.alibaba.polardbx.matrix.jdbc.TConnection;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.NIOProcessor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.handler.pl.RuntimeProcedure;
import com.alibaba.polardbx.server.handler.pl.RuntimeProcedureManager;

import java.util.List;

/**
 * @author mengshi.sunmengshi 2015年5月12日 下午1:28:16
 * @since 5.1.0
 */
public class KillSyncAction implements ISyncAction {

    private static final Logger logger = LoggerFactory.getLogger(KillSyncAction.class);

    private String user;
    private long id;
    private boolean killQuery;
    private boolean skipValidation;
    private ErrorCode cause;

    public ErrorCode getCause() {
        return cause;
    }

    public void setCause(ErrorCode cause) {
        this.cause = cause;
    }

    public KillSyncAction(String user, long id, boolean killQuery) {
        this(user, id, killQuery, ErrorCode.ERR_USER_CANCELED);
    }

    public KillSyncAction(String user, long id, boolean killQuery, ErrorCode cause) {
        this.user = user;
        this.id = id;
        this.killQuery = killQuery;
        this.cause = cause;
    }

    public KillSyncAction(String user, long id, boolean killQuery, boolean skipValidation, ErrorCode cause) {
        this.user = user;
        this.id = id;
        this.killQuery = killQuery;
        this.cause = cause;
        this.skipValidation = skipValidation;
    }

    public KillSyncAction() {

    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public boolean isKillQuery() {
        return killQuery;
    }

    public void setKillQuery(boolean killQuery) {
        this.killQuery = killQuery;
    }

    public void setSkipValidation(boolean skipValidation) {
        this.skipValidation = skipValidation;
    }

    public boolean isSkipValidation() {
        return skipValidation;
    }

    @Override
    public ResultCursor sync() {

        int count = 0;
        NIOProcessor[] processors = CobarServer.getInstance().getProcessors();
        FrontendConnection fc;
        String traceId = null;
        boolean found = false;
        for (NIOProcessor p : processors) {
            if ((fc = p.getFrontends().get(id)) != null) {
                found = true;
                if (fc instanceof ServerConnection && hasAccess(fc)) {
                    TConnection tc = ((ServerConnection) fc).getTddlConnection();
                    if (tc != null) {
                        pauseDdlJobIfNecessary(tc);
                        ExecutionContext executionContext = tc.getExecutionContext();
                        traceId = executionContext.getTraceId();
                        if (ServiceProvider.getInstance().getServer() != null
                            && traceId != null) {
                            ServiceProvider.getInstance().getServer().getQueryManager()
                                .cancelQuery(traceId);
                        }
                    }
                    if (killQuery) {
                        ((ServerConnection) fc).cancelQuery(cause);
                    } else {
                        fc.close();
                    }
                    count++;
                }
                killProcedure(fc.getId());
                killFunction(traceId);
                break;
            }
        }
        if (!found) {
            logger.info(String.format("To kill ConnectionId-%d is not found", id));
        }

        ArrayResultCursor result = new ArrayResultCursor("KILL");
        result.addColumn(ResultCursor.AFFECT_ROW, DataTypes.IntegerType);
        result.initMeta();

        result.addRow(new Object[] {count});

        return result;
    }

    private void killProcedure(long connId) {
        RuntimeProcedure runtimeProcedure = RuntimeProcedureManager.getInstance().search(connId);
        if (runtimeProcedure != null) {
            runtimeProcedure.getPlContext().setStatus(ProcedureStatus.KILLED);
        }
    }

    private void killFunction(String traceId) {
        List<RuntimeFunction> runtimeFunctions = RuntimeFunctionManager.getInstance().searchAllRelatedFunction(traceId);
        for (RuntimeFunction function : runtimeFunctions) {
            function.getPlContext().setStatus(ProcedureStatus.KILLED);
        }
    }

    private boolean hasAccess(FrontendConnection fc) {
        // Case 0: Skip access validation, target can be killed.
        if (skipValidation) {
            return true;
        }

        // Case 1: For drds, if kill.user == target.schema, target can be killed.
        if (((ServerConnection) fc).isAdministrator(user)) {
            return true;
        }

        // Case 2: If kill.user == target.user, target can be killed.
        if (TStringUtil.equals(user, fc.getUser())) {
            return true;
        }

        // Case 3: If kill.user == polardbx_root, target can be killed.
        if (TStringUtil.equals(PolarPrivUtil.POLAR_ROOT, user)) {
            return true;
        }

        // None of the above cases, target can not be killed.
        logger.warn(String.format("User: %s has no privilege kill ConnectionId-%d",
            user, id));
        return false;
    }

    private void pauseDdlJobIfNecessary(TConnection conn) {
        if (conn == null || conn.getExecutionContext() == null) {
            return;
        }
        if (!conn.isDdlStatement()) {
            return;
        }
        Long jobId = conn.getExecutionContext().getDdlJobId();
        if (jobId == null) {
            return;
        }
        DdlEngineRequester.pauseJob(jobId, conn.getExecutionContext());
    }

}
