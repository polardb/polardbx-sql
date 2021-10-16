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

package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlSetDefaultRoleStatement;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.privilege.Permission;
import com.alibaba.polardbx.gms.privilege.PermissionCheckContext;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.gms.privilege.PolarRolePrivilege;
import com.alibaba.polardbx.gms.privilege.PrivilegeKind;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlSetDefaultRole;
import org.apache.calcite.sql.SqlUserName;

import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_SERVER;

/**
 * Handle set default role statements.
 *
 * @author bairui.lrj
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/set-default-role.html">Set default role</a>
 * @see MySqlSetDefaultRoleStatement
 * @since 5.4.9
 */
public class LogicalSetDefaultRoleHandler extends HandlerCommon {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogicalSetDefaultRoleHandler.class);

    public LogicalSetDefaultRoleHandler(IRepository repo) {
        super(repo);
    }

    private static void checkCanSetDefaultRole(ExecutionContext executionContext, List<PolarAccountInfo> receivers) {
        PolarPrivManager manager = PolarPrivManager.getInstance();
        manager
            .checkModifyReservedAccounts(executionContext.getPrivilegeContext().getPolarUserInfo(), receivers, false);
        PermissionCheckContext permissionCheckContext = executionContext.getPrivilegeContext()
            .toPermissionCheckContext(Permission.instancePermission(PrivilegeKind.CREATE_USER));

        if (manager.checkPermission(permissionCheckContext)) {
            return;
        }

        PolarAccountInfo currentUser = executionContext.getPrivilegeContext().getPolarUserInfo();
        long currentUserId = currentUser.getAccountId();

        if (receivers.stream().anyMatch(r -> r.getAccountId() != currentUserId)) {
            throw new TddlRuntimeException(ErrorCode.ERR_CHECK_PRIVILEGE_FAILED, "Create User",
                currentUser.getUsername(), currentUser.getHost());
        }
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        LogicalDal logicalDal = (LogicalDal) logicalPlan;
        SqlSetDefaultRole sqlNode = (SqlSetDefaultRole) logicalDal.getNativeSqlNode();

        PolarPrivManager manager = PolarPrivManager.getInstance();

        List<PolarAccountInfo> toUsers = sqlNode.getUsers()
            .stream()
            .map(SqlUserName::toPolarAccount)
            .map(manager::getAndCheckExactUser)
            .collect(Collectors.toList());

        checkCanSetDefaultRole(executionContext, toUsers);

        List<PolarAccountInfo> roles = sqlNode.getRoles()
            .stream()
            .map(SqlUserName::toPolarAccount)
            .map(manager::getAndCheckExactUser)
            .collect(Collectors.toList());

        toUsers
            .forEach(
                user -> user.getRolePrivileges().checkRolesGranted(roles.stream().map(PolarAccountInfo::getAccount)));

        PolarRolePrivilege.DefaultRoleState defaultRoleState =
            PolarRolePrivilege.DefaultRoleState.from(sqlNode.getDefaultRoleSpec());

        manager.runWithMetaDBConnection(
            conn -> {
                try {
                    PolarRolePrivilege.syncDefaultRolesToDb(conn, toUsers, defaultRoleState, roles);
                    conn.commit();
                    manager.reloadAccounts(conn,
                        toUsers.stream().map(PolarAccountInfo::getAccount).collect(Collectors.toList()));
                } catch (Throwable t) {
                    LOGGER.error("Failed to set default roles!", t);
                    throw new TddlRuntimeException(ERR_SERVER, "Failed to persist account data!", t);
                }
            }
        );

        manager.triggerReload();

        return new AffectRowCursor(toUsers.size() * roles.size());
    }

}
