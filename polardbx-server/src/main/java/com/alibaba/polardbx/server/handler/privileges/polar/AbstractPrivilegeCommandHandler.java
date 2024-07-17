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

package com.alibaba.polardbx.server.handler.privileges.polar;

import com.alibaba.polardbx.cdc.SQLHelper;
import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.common.cdc.DdlScope;
import com.alibaba.polardbx.common.cdc.ICdcManager;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSetStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.expr.MySqlUserName;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.privilege.AccountType;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.server.ServerConnection;
import com.google.common.collect.Maps;
import org.apache.calcite.sql.SqlKind;

import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.common.cdc.ICdcManager.POLARDBX_SERVER_ID;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_GRANTER_NO_GRANT_PRIV;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_OPERATION_NOT_ALLOWED;
import static com.alibaba.polardbx.server.handler.privileges.polar.PolarHandlerCommon.checkMasterInstance;

/**
 * @author bairui.lrj
 * @since 5.4.9
 */
public abstract class AbstractPrivilegeCommandHandler implements PrivilegeCommandHandler {
    private final ByteString sql;
    private final ServerConnection serverConn;
    private final PolarAccountInfo granter;
    private final PolarPrivManager privManager;

    public AbstractPrivilegeCommandHandler(ByteString sql,
                                           ServerConnection serverConn,
                                           PolarAccountInfo granter,
                                           PolarPrivManager polarPrivManager) {
        this.sql = sql;
        this.serverConn = serverConn;
        this.granter = granter;
        this.privManager = polarPrivManager;
    }

    static void checkAllAreRoles(List<PolarAccountInfo> inputs) {
        boolean allInputsAreRole = inputs.stream()
            .allMatch(r -> r.getAccountType() == AccountType.ROLE);
        if (!allInputsAreRole) {
            throw new TddlRuntimeException(ERR_OPERATION_NOT_ALLOWED, "All grant role inputs must be role!");
        }
    }

    @Override
    public void handle(boolean hasMore) {
        beforeHandle();
        doHandle();
        markDdlForCdc(getSqlKind());
        afterHandle();
    }

    public ByteString getSql() {
        return sql;
    }

    public ServerConnection getServerConn() {
        return serverConn;
    }

    public PolarAccountInfo getGranter() {
        return granter;
    }

    public PolarPrivManager getPrivManager() {
        return privManager;
    }

    protected void beforeHandle() {
        checkMasterInstance();
    }

    protected abstract void doHandle();

    protected abstract SqlKind getSqlKind();

    protected void afterHandle() {
        privManager.triggerReload();
    }

    protected PolarAccountInfo getAndCheckExactUser(MySqlUserName mysqlUsername) {
        return privManager.getAndCheckExactUser(mysqlUsername.getUserName(),
            mysqlUsername.getHost());
    }

    void checkGranterCanGrantOrRevokeRoles(List<PolarAccountInfo> inputs) {
        for (PolarAccountInfo role : inputs) {
            if (!granter.canGrantOrRevokeRole(role)) {
                throw new TddlRuntimeException(ERR_GRANTER_NO_GRANT_PRIV, "ROLE", role.getIdentifier());
            }
        }
    }

    //TODO cdc@jinwu
    private void markDdlForCdc(SqlKind sqlKind) {
        Map<String, Object> param = Maps.newHashMap();
        param.put(ICdcManager.CDC_DDL_SCOPE, DdlScope.Instance);

        final Map<String, Object> extraVariables = serverConn.getExtraServerVariables();
        if (null != extraVariables && extraVariables.containsKey(POLARDBX_SERVER_ID)) {
            Object serverId = extraVariables.get(POLARDBX_SERVER_ID);
            param.put(POLARDBX_SERVER_ID, serverId);
        }

        SQLStatement statement = SQLHelper.parseSql(getSql().toString());
        if (statement instanceof SQLSetStatement) {
            SQLSetStatement sqlSetStatement = (SQLSetStatement) statement;
            if (sqlSetStatement.getOption() != null && StringUtils.equalsIgnoreCase(
                sqlSetStatement.getOption().name(), "PASSWORD")) {
                for (SQLAssignItem item : ((SQLSetStatement) statement).getItems()) {
                    if (item.getTarget() == null) {
                        return;
                    }
                }
            }
        }

        CdcManagerHelper.getInstance().notifyDdlNew(
            SystemDbHelper.DEFAULT_DB_NAME,
            "*",
            sqlKind.name(),
            getSql().toString(),
            DdlType.UNSUPPORTED,
            null,
            null,
            CdcDdlMarkVisibility.Protected,
            param);
    }
}
