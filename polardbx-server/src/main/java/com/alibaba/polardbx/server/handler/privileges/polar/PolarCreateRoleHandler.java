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

import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateRoleStatement;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.common.audit.AuditAction;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.privilege.UserPasswdChecker;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.privilege.PolarAccount;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.gms.privilege.audit.AuditPrivilege;
import com.google.common.collect.Maps;
import org.apache.calcite.sql.SqlKind;

import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.server.handler.privileges.polar.PolarHandlerCommon.checkDrdsRoot;

/**
 * Creating a role.
 *
 * @author bairui.lrj
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/create-role.html">Creating Role</a>
 * @see PolarCreateUserHandler
 * @since 5.4.9
 */
public class PolarCreateRoleHandler extends AbstractPrivilegeCommandHandler {
    private static final Logger logger = LoggerFactory.getLogger(PolarCreateRoleHandler.class);

    private final MySqlCreateRoleStatement stmt;

    public PolarCreateRoleHandler(ByteString sql,
                                  ServerConnection serverConn,
                                  PolarAccountInfo granter,
                                  PolarPrivManager polarPrivManager,
                                  MySqlCreateRoleStatement stmt) {
        super(sql, serverConn, granter, polarPrivManager);
        this.stmt = stmt;
    }

    @Override
    public void doHandle() {
        List<PolarAccountInfo> grantees = checkAndGetGrantees();

        PolarAccountInfo granter = getGranter();

        logger.info(String.format("CREATE ROLE succeed, sql: %s, granter: %s", getSql(), granter.getIdentifier()));
        PolarPrivManager.getInstance().createAccount(granter, getServerConn().getActiveRoles(),
            grantees, stmt.isIfNotExists());
        AuditPrivilege.polarAudit(getServerConn().getConnectionInfo(), getSql().toString(), AuditAction.CREATE_ROLE);
    }

    @Override
    protected SqlKind getSqlKind() {
        return SqlKind.CREATE_ROLE;
    }

    private List<PolarAccountInfo> checkAndGetGrantees() {
        List<PolarAccountInfo> accounts = stmt.getRoleSpecs()
            .stream()
            .map(PolarAccount::fromRoleSpec)
            .map(PolarAccountInfo::new)
            .collect(Collectors.toList());

        checkGrantees(accounts);
        return accounts;
    }

    private static void checkGrantees(List<PolarAccountInfo> grantees) {
        checkDrdsRoot(grantees);

        for (PolarAccountInfo user : grantees) {

            if (!UserPasswdChecker.verifyUsername(user.getUsername())) {
                throw new TddlRuntimeException(ErrorCode.ERR_INVALID_USERNAME);
            }
        }
    }
}
