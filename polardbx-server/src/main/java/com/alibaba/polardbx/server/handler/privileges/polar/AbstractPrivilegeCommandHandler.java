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

import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.OkPacket;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.expr.MySqlUserName;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.gms.privilege.AccountType;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;

import java.util.List;

import static com.alibaba.polardbx.server.handler.privileges.polar.PolarHandlerCommon.checkMasterInstance;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_GRANTER_NO_GRANT_PRIV;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_OPERATION_NOT_ALLOWED;

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
}
