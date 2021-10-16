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
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLRevokeStatement;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.common.audit.AuditAction;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.gms.privilege.PrivilegeKind;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.server.handler.privileges.polar.PolarHandlerCommon.checkDrdsRoot;
import static com.alibaba.polardbx.gms.privilege.audit.AuditPrivilege.polarAudit;

/**
 * @author bairui.lrj
 */
public class PolarRevokePrivilegeHandler extends AbstractPrivilegeCommandHandler {
    private static final Logger logger = LoggerFactory.getLogger(PolarRevokePrivilegeHandler.class);

    private final SQLRevokeStatement stmt;

    public PolarRevokePrivilegeHandler(ByteString sql,
                                       ServerConnection serverConn,
                                       PolarAccountInfo granter,
                                       PolarPrivManager polarPrivManager,
                                       SQLRevokeStatement stmt) {
        super(sql, serverConn, granter, polarPrivManager);
        this.stmt = stmt;
    }

    private List<PolarAccountInfo> getGrantees(ServerConnection c) {
        List<PolarAccountInfo> grantees;
        try {
            SQLExprTableSource sqlExprTableSource = (SQLExprTableSource) stmt.getResource();
            grantees = PolarHandlerCommon.getGrantees(sqlExprTableSource, stmt.getUsers(),
                stmt.getPrivileges(), c);
            if (stmt.isGrantOption()) {
                for (PolarAccountInfo grantee : grantees) {
                    if (grantee.getFirstDbPriv() != null) {
                        grantee.getFirstDbPriv().grantPrivilege(PrivilegeKind.GRANT_OPTION);
                    } else if (grantee.getFirstTbPriv() != null) {
                        grantee.getFirstTbPriv().grantPrivilege(PrivilegeKind.GRANT_OPTION);
                    } else {
                        grantee.getInstPriv().grantPrivilege(PrivilegeKind.GRANT_OPTION);
                    }
                }
            }
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }

        return grantees;
    }

    private void checkGrantees(List<PolarAccountInfo> grantees) {
        checkDrdsRoot(grantees);
    }

    @Override
    protected void doHandle() {
        ByteString sql = getSql();
        ServerConnection c = getServerConn();
        List<PolarAccountInfo> grantees = getGrantees(c);

        checkGrantees(grantees);

        PolarAccountInfo granter = getGranter();

        // Revoke
        PolarPrivManager.getInstance().revokePrivileges(granter, c.getActiveRoles(), grantees);
        polarAudit(getServerConn().getConnectionInfo(), getSql().toString(), AuditAction.REVOKE);
        logger.info(String.format("REVOKE succeed, sql: %s, granter: %s", sql, granter.getIdentifier()));
    }
}
