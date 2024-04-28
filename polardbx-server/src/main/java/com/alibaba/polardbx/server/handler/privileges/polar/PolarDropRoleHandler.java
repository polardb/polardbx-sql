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
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlDropRoleStatement;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.common.audit.AuditAction;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.privilege.PolarAccount;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.gms.privilege.audit.AuditPrivilege;
import org.apache.calcite.sql.SqlKind;

import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.server.handler.privileges.polar.PolarHandlerCommon.checkDrdsRoot;

/**
 * Drop role.
 *
 * @author bairui.lrj
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/drop-role.html">Drop Role</a>
 * @see PolarDropUserHandler
 * @since 5.4.9
 */
public class PolarDropRoleHandler extends AbstractPrivilegeCommandHandler {
    private static final Logger logger = LoggerFactory.getLogger(PolarCreateUserHandler.class);
    private final MySqlDropRoleStatement stmt;

    public PolarDropRoleHandler(ByteString sql,
                                ServerConnection serverConn,
                                PolarAccountInfo granter,
                                PolarPrivManager polarPrivManager,
                                MySqlDropRoleStatement stmt) {
        super(sql, serverConn, granter, polarPrivManager);
        this.stmt = stmt;
    }

    protected static void checkGrantees(List<PolarAccountInfo> grantees) {
        checkDrdsRoot(grantees);
    }

    private List<PolarAccountInfo> getGrantees() {
        return stmt.getRoleSpecs()
            .stream()
            .map(PolarAccount::fromRoleSpec)
            .map(PolarAccountInfo::new)
            .collect(Collectors.toList());
    }

    @Override
    protected void doHandle() {
        List<PolarAccountInfo> grantees = getGrantees();
        checkGrantees(grantees);

        PolarAccountInfo granter = getGranter();

        logger.info(String.format("DROP ROLE succeed, sql: %s, granter: %s", getSql(), granter.getIdentifier()));
        PolarPrivManager.getInstance().dropAccount(granter, getServerConn().getActiveRoles(), grantees, true);
        AuditPrivilege.polarAudit(getServerConn().getConnectionInfo(), getSql().toString(), AuditAction.DROP_ROLE);
    }

    @Override
    protected SqlKind getSqlKind() {
        return SqlKind.DROP_ROLE;
    }
}
