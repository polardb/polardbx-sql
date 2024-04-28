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

import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlRevokeRoleStatement;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.common.audit.AuditAction;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.gms.privilege.PolarRolePrivilege;
import com.alibaba.polardbx.gms.privilege.audit.AuditPrivilege;
import org.apache.calcite.sql.SqlKind;

import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_SERVER;

/**
 * Revoke roles from users or roles. If the role to be revoked has not been granted to user/role yet, it's ignored.
 *
 * @author bairui.lrj
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/revoke.html">Revoke Role</a>
 * @since 5.4.9
 */
public class PolarRevokeRoleHandler extends AbstractPrivilegeCommandHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(PolarRevokeRoleHandler.class);
    private final MySqlRevokeRoleStatement stmt;

    public PolarRevokeRoleHandler(ByteString sql,
                                  ServerConnection serverConn,
                                  PolarAccountInfo granter,
                                  PolarPrivManager polarPrivManager,
                                  MySqlRevokeRoleStatement stmt) {
        super(sql, serverConn, granter, polarPrivManager);
        this.stmt = stmt;
    }

    @Override
    protected void doHandle() {
        List<PolarAccountInfo> roles = checkAndGetInputRoles();

        List<PolarAccountInfo> fromAccounts = stmt.getFromAccounts()
            .stream()
            .map(this::getAndCheckExactUser)
            .collect(Collectors.toList());
        getPrivManager().checkModifyReservedAccounts(getGranter(), fromAccounts, false);

        for (PolarAccountInfo fromUser : fromAccounts) {
            for (PolarAccountInfo role : roles) {
                fromUser.checkRoleGranted(role.getAccount());
            }
        }

        getPrivManager().runWithMetaDBConnection(
            conn -> {
                try {
                    PolarRolePrivilege.syncRevokeRolePrivilegesToDb(conn, roles, fromAccounts);
                    conn.commit();
                    getPrivManager().reloadAccounts(conn, fromAccounts.stream()
                        .map(PolarAccountInfo::getAccount)
                        .collect(Collectors.toList()));
                } catch (Throwable t) {
                    LOGGER.error("Failed to revoke roles.", t);
                    throw new TddlRuntimeException(ERR_SERVER, "Failed to persist account data!", t);
                }
            });
        AuditPrivilege.polarAudit(getServerConn().getConnectionInfo(), getSql().toString(), AuditAction.REVOKE_ROLE);
        getPrivManager().triggerReload();
    }

    @Override
    protected SqlKind getSqlKind() {
        return SqlKind.REVOKE_ROLE;
    }

    private List<PolarAccountInfo> checkAndGetInputRoles() {
        List<PolarAccountInfo> inputInfoList = stmt.getRevokedAccounts()
            .stream()
            .map(r -> getPrivManager().getExactUser(r.getUserName(), r.getHost()))
            .collect(Collectors.toList());

        checkAllAreRoles(inputInfoList);
        checkGranterCanGrantOrRevokeRoles(inputInfoList);
        getPrivManager().checkModifyReservedAccounts(getGranter(), inputInfoList, false);
        return inputInfoList;
    }
}
