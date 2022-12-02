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

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSetStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.expr.MySqlUserName;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.common.audit.AuditAction;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.privilege.UserPasswdChecker;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.privilege.PolarAccount;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.gms.privilege.audit.AuditPrivilege;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import org.apache.commons.lang.StringUtils;

import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.server.handler.privileges.polar.PolarHandlerCommon.PASSWORD;
import static com.alibaba.polardbx.server.handler.privileges.polar.PolarHandlerCommon.checkDrdsRoot;
import static com.alibaba.polardbx.server.handler.privileges.polar.PolarHandlerCommon.encryptPassword;
import static com.alibaba.polardbx.server.handler.privileges.polar.PolarHandlerCommon.getMatchGranter;

/**
 * @author shicai.xsc 2020/3/5 20:50
 * @since 5.0.0.0
 */
public class PolarSetPasswordHandler extends AbstractPrivilegeCommandHandler {
    private static final Logger logger = LoggerFactory.getLogger(PolarSetPasswordHandler.class);

    private final SQLSetStatement stmt;

    public PolarSetPasswordHandler(ByteString sql,
                                   ServerConnection serverConn,
                                   PolarAccountInfo granter,
                                   PolarPrivManager polarPrivManager,
                                   SQLSetStatement stmt) {
        super(sql, serverConn, granter, polarPrivManager);
        this.stmt = stmt;
    }

    private static List<PolarAccountInfo> getGrantees(ByteString sql, PolarAccountInfo granter) {
        List<PolarAccountInfo> userInfos = new ArrayList<>();

        try {
            SQLSetStatement statement =
                (SQLSetStatement) FastsqlUtils.parseSql(sql, SQLParserFeature.IgnoreNameQuotes).get(0);
            SQLAssignItem assignItem = statement.getItems().get(0);
            MySqlUserName userName = MySqlUserName.fromIdentifier((SQLIdentifierExpr) assignItem.getTarget());
            String password = "";

            if (assignItem.getValue() instanceof SQLCharExpr) {
                password = assignItem.getValue().toString();
            } else if (assignItem.getValue() instanceof SQLMethodInvokeExpr) {
                SQLMethodInvokeExpr methodInvokeExpr = (SQLMethodInvokeExpr) assignItem.getValue();
                if (!StringUtils.equalsIgnoreCase(PASSWORD, methodInvokeExpr.getMethodName())) {
                    throw new SQLSyntaxErrorException("set password syntax error");
                }
                password = methodInvokeExpr.getArguments().get(0).toString();
            } else {
                throw new SQLSyntaxErrorException("set password syntax error");
            }

            if (isQuoted(password, "'") || isQuoted(password, "\"")) {
                password = password.substring(1, password.length() - 1);
            }

            PolarAccountInfo grantee;
            if (userName == null) {
                grantee = new PolarAccountInfo(PolarAccount.copyBuilder(granter.getAccount())
                    .setPassword(password)
                    .build());
            } else {
                grantee = new PolarAccountInfo(
                    PolarAccount.copyBuilder(PolarAccount.fromMySqlUsername(userName))
                        .setPassword(password)
                        .build());
            }

            userInfos.add(grantee);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }

        return userInfos;
    }

    private static void checkGrantees(List<PolarAccountInfo> grantees) {
        checkDrdsRoot(grantees);

        for (PolarAccountInfo user : grantees) {
            if (!UserPasswdChecker.verifyPassword(user.getPassword(),
                CobarServer.getInstance().getConfig().getSystem().getPasswordRuleConfig())) {
                throw new TddlRuntimeException(ErrorCode.ERR_INVALID_PASSWORD);
            }
        }
    }

    private static boolean isQuoted(String password, String quoter) {
        return password.startsWith(quoter) && password.endsWith(quoter);
    }

    @Override
    protected void doHandle() {
        ByteString sql = getSql();
        ServerConnection c = getServerConn();
        PolarAccountInfo granter = getMatchGranter(c);

        List<PolarAccountInfo> grantees = getGrantees(sql, granter);
        checkGrantees(grantees);
        encryptPassword(grantees);
        PolarPrivManager.getInstance().setPassword(granter, c.getActiveRoles(), grantees.get(0));
        AuditPrivilege.polarAudit(getServerConn().getConnectionInfo(),
            grantees.stream()
                .map(PolarAccountInfo::getIdentifier)
                .collect(Collectors.joining()),
            AuditAction.SET_PASSWORD);
    }
}
