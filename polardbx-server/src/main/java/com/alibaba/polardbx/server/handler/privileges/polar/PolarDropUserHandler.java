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
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropUserStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.expr.MySqlUserName;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.common.audit.AuditAction;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.privilege.PolarAccount;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.server.handler.privileges.polar.PolarHandlerCommon.checkDrdsRoot;
import static com.alibaba.polardbx.gms.privilege.audit.AuditPrivilege.polarAudit;

/**
 * @author shicai.xsc 2020/3/5 20:49
 * @since 5.0.0.0
 */
public class PolarDropUserHandler extends AbstractPrivilegeCommandHandler {
    private static final Logger logger = LoggerFactory.getLogger(PolarCreateUserHandler.class);

    private final SQLDropUserStatement stmt;

    public PolarDropUserHandler(ByteString sql,
                                ServerConnection serverConn,
                                PolarAccountInfo granter,
                                PolarPrivManager polarPrivManager,
                                SQLDropUserStatement stmt) {
        super(sql, serverConn, granter, polarPrivManager);
        this.stmt = stmt;
    }

    private List<PolarAccountInfo> getGrantees() {

        List<PolarAccountInfo> grantees = new ArrayList<>();
        for (SQLExpr spec : stmt.getUsers()) {
            MySqlUserName user = MySqlUserName.fromExpr(spec);
            PolarAccountInfo userInfo = new PolarAccountInfo(PolarAccount.newBuilder()
                .setUsername(user.getUserName())
                .setHost(user.getHost())
                .build());
            grantees.add(userInfo);
        }

        return grantees;
    }

    protected static void checkGrantees(List<PolarAccountInfo> grantees) {
        checkDrdsRoot(grantees);
    }

    @Override
    protected void doHandle() {
        List<PolarAccountInfo> grantees = getGrantees();
        checkGrantees(grantees);

        PolarAccountInfo granter = getGranter();

        PolarPrivManager.getInstance().dropAccount(granter, getServerConn().getActiveRoles(), grantees, true);
        polarAudit(getServerConn().getConnectionInfo(), getSql().toString(), AuditAction.DROP_USER);
        logger.info(String.format("DROP USER succeed, sql: %s, granter: %s", getSql(), granter.getIdentifier()));
    }
}
