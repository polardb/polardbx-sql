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
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.matrix.jdbc.TConnection;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.NIOProcessor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.server.ServerConnection;

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

    @Override
    public ResultCursor sync() {

        int count = 0;
        NIOProcessor[] processors = CobarServer.getInstance().getProcessors();
        FrontendConnection fc;
        for (NIOProcessor p : processors) {
            if ((fc = p.getFrontends().get(id)) != null) {
                if (fc instanceof ServerConnection && hasAccess(fc)) {
                    TConnection tc = ((ServerConnection) fc).getTddlConnection();
                    if (tc != null) {
                        ExecutionContext executionContext = tc.getExecutionContext();
                        if (ServiceProvider.getInstance().getServer() != null
                            && executionContext.getTraceId() != null) {
                            ServiceProvider.getInstance().getServer().getQueryManager()
                                .cancelQuery(executionContext.getTraceId());
                        }
                    }
                    if (killQuery) {
                        ((ServerConnection) fc).cancelQuery(cause);
                    } else {
                        fc.close();
                    }
                    count++;
                }
                break;
            }
        }

        ArrayResultCursor result = new ArrayResultCursor("KILL");
        result.addColumn(ResultCursor.AFFECT_ROW, DataTypes.IntegerType);
        result.initMeta();

        result.addRow(new Object[] {count});

        return result;
    }

    private boolean hasAccess(FrontendConnection fc){
        if(skipValidation){
            return true;
        }
        return TStringUtil.equals(user, fc.getUser()) || ((ServerConnection) fc).isAdministrator(user);
    }

}
