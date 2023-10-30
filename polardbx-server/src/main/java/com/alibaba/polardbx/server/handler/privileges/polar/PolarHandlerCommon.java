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

import com.alibaba.polardbx.PolarPrivileges;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLPrivilegeItem;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.expr.MySqlUserName;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.privilege.PrivilegeErrorMsg;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.privilege.PolarAccount;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.gms.privilege.PolarDbPriv;
import com.alibaba.polardbx.gms.privilege.PolarInstPriv;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.gms.privilege.PolarPrivUtil;
import com.alibaba.polardbx.gms.privilege.PolarTbPriv;
import com.alibaba.polardbx.gms.privilege.PrivManageCode;
import com.alibaba.polardbx.gms.privilege.PrivManageResult;
import com.alibaba.polardbx.gms.privilege.PrivManageType;
import org.apache.commons.lang.StringUtils;

import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author shicai.xsc 2020/3/5 20:51
 * @since 5.0.0.0
 */
public class PolarHandlerCommon {

    protected static final String CREATE_USER_NO_PRIVILEGE =
        "Access denied; you need (at least one of) the CREATE USER privilege(s) for this operation";
    protected static final String CREATE_USER_FAILED = "Operation CREATE USER failed for %s";

    protected static final String DROP_USER_FAILED = "Operation DROP USER failed for %s";

    protected static final String GRANT_NO_PRIVILEGE = "Access denied for user %s";

    protected static final String SET_PASSWORD_NO_PRIVILEGE = "Access denied for user %s";

    protected static final String GRANT_FAILED = "Operation GRANT failed for %s";

    protected static final String REVOKE_FAILED = "Operation REVOKE failed for %s";

    protected static final String SET_PASSWORD_FAILED = "Operation SET PASSWORD failed for %s";

    protected static final String SQL_SYNTAX_ERROR = "You have an error in your SQL syntax";

    protected static final String NO_DATABASE_SELECTED = "No database selected";

    protected static final String PASSWORD = "password";

    protected static final String POLAR_ROOT = PolarPrivUtil.POLAR_ROOT;

    protected static void handleUserError(PrivManageResult res, ServerConnection c, PrivManageType type) {
        if (res.getResultCodes().get(0) == PrivManageCode.NO_PRIVILEGE) {
            c.writeErrMessage(ErrorCode.ER_NO, CREATE_USER_NO_PRIVILEGE);
            return;
        }

        List<String> users = new ArrayList<>();
        for (PolarAccountInfo user : res.getFailedGrantees()) {
            users.add(user.getIdentifier());
        }

        String error;
        if (type == PrivManageType.CREATE_ACCOUNT) {
            error = String.format(CREATE_USER_FAILED, String.join(",", users));
        } else {
            error = String.format(DROP_USER_FAILED, String.join(",", users));
        }
        c.writeErrMessage(ErrorCode.ER_NO, error);
    }

    protected static void handleGrantError(PrivManageResult res, ServerConnection c, PrivManageType type) {
        if (res.getResultCodes().get(0) == PrivManageCode.NO_PRIVILEGE) {
            String error = String.format(GRANT_NO_PRIVILEGE, res.getGranter().getIdentifier());
            c.writeErrMessage(ErrorCode.ER_NO, error);
            return;
        }

        List<String> users = new ArrayList<>();
        for (PolarAccountInfo user : res.getFailedGrantees()) {
            users.add(user.getIdentifier());
        }

        String error;
        if (type == PrivManageType.GRANT_PRIVILEGE) {
            error = String.format(GRANT_FAILED, String.join(",", users));
        } else {
            error = String.format(REVOKE_FAILED, String.join(",", users));
        }
        c.writeErrMessage(ErrorCode.ER_NO, error);
    }

    protected static void handleSetPasswordError(PrivManageResult res, ServerConnection c) {
        if (res.getResultCodes().get(0) == PrivManageCode.NO_PRIVILEGE) {
            String error = String.format(SET_PASSWORD_NO_PRIVILEGE, res.getGranter().getIdentifier());
            c.writeErrMessage(ErrorCode.ER_NO, error);
            return;
        }

        List<String> users = new ArrayList<>();
        for (PolarAccountInfo user : res.getFailedGrantees()) {
            users.add(user.getIdentifier());
        }

        String error = String.format(SET_PASSWORD_FAILED, String.join(",", users));
        c.writeErrMessage(ErrorCode.ER_NO, error);
    }

    protected static void checkMasterInstance() {
        // Can't do GRANT/REVOKE/CREATE/DROP/SET on slave instance
        if (!ConfigDataMode.isMasterMode()) {
            throw new TddlRuntimeException(ErrorCode.ERR_OPERATION_NOT_ALLOWED,
                PrivilegeErrorMsg.AUTHORIZE_RESTRICTED_TO_MASTER_INSTANCE);
        }
    }

    protected static void checkDrdsRoot(List<PolarAccountInfo> grantees) {
        for (PolarAccountInfo grantee : grantees) {
            if (StringUtils.equalsIgnoreCase(grantee.getUsername(), POLAR_ROOT)) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPERATION_NOT_ALLOWED,
                    String.format("Can not modify %s since it is reserved for system", POLAR_ROOT));
            }
        }
    }

    protected static List<PolarAccountInfo> getGrantees(SQLExprTableSource sqlExprTableSource, List<SQLExpr> users,
                                                        List<SQLPrivilegeItem> privilegeItems, ServerConnection c)
        throws Exception {
        List<PolarAccountInfo> grantees = new ArrayList<>();
        PolarInstPriv instPriv = null;
        PolarTbPriv tbPriv = null;
        PolarDbPriv dbPriv = null;

        // Get priv level
        if (sqlExprTableSource.getExpr() instanceof SQLIdentifierExpr) {
            // Tb priv
            if (StringUtils.isBlank(c.getSchema())) {
                throw new Exception(NO_DATABASE_SELECTED);
            }
            tbPriv = new PolarTbPriv();
            tbPriv.setDbName(c.getSchema());
            tbPriv.setTbName(sqlExprTableSource.getExpr().toString());
        } else if (sqlExprTableSource.getExpr() instanceof SQLPropertyExpr) {
            SQLPropertyExpr propertyExpr = (SQLPropertyExpr) sqlExprTableSource.getExpr();
            if (propertyExpr.getOwner().toString().equals("*")) {
                if (propertyExpr.getName().equals("*")) {
                    // Inst priv
                    instPriv = new PolarInstPriv();
                } else {
                    new SQLSyntaxErrorException(SQL_SYNTAX_ERROR);
                }
            } else {
                if (propertyExpr.getName().equals("*")) {
                    // Db priv
                    dbPriv = new PolarDbPriv();
                    dbPriv.setDbName(propertyExpr.getOwner().toString());
                } else {
                    // Tb priv
                    tbPriv = new PolarTbPriv();
                    tbPriv.setDbName(propertyExpr.getOwner().toString());
                    tbPriv.setTbName(propertyExpr.getName());
                }
            }
        } else if (sqlExprTableSource.getExpr() instanceof SQLAllColumnExpr) {
            // Inst priv
            instPriv = new PolarInstPriv();
        } else {
            throw new SQLSyntaxErrorException(SQL_SYNTAX_ERROR);
        }

        // set privs
        for (SQLExpr u : users) {
            MySqlUserName user = MySqlUserName.fromExpr(u);
            PolarAccountInfo userInfo = new PolarAccountInfo(PolarAccount.newBuilder()
                .setUsername(user.getUserName())
                .setHost(user.getHost())
                .build());

            if (instPriv != null) {
                PolarInstPriv tmp = instPriv.deepCopy();
                tmp.setUserName(userInfo.getUsername());
                tmp.setHost(userInfo.getHost());
                userInfo.setInstPriv(tmp);
            } else if (dbPriv != null) {
                PolarDbPriv tmp = dbPriv.deepCopy();
                tmp.setUserName(userInfo.getUsername());
                tmp.setHost(userInfo.getHost());
                userInfo.addDbPriv(tmp);
            }
            if (tbPriv != null) {
                PolarTbPriv tmp = tbPriv.deepCopy();
                tmp.setUserName(userInfo.getUsername());
                tmp.setHost(userInfo.getHost());
                userInfo.addTbPriv(tmp);
            }
            grantees.add(userInfo);
        }

        for (SQLPrivilegeItem privItem : privilegeItems) {
            String privStr = privItem.getAction().toString().toUpperCase();
            for (PolarAccountInfo grantee : grantees) {
                if (grantee.getFirstDbPriv() != null) {
                    grantee.getFirstDbPriv().updatePrivilege(privStr, true);
                } else if (grantee.getFirstTbPriv() != null) {
                    grantee.getFirstTbPriv().updatePrivilege(privStr, true);
                } else {
                    grantee.getInstPriv().updatePrivilege(privStr, true);
                }
            }
        }

        return grantees;
    }

    public static void encryptPassword(List<PolarAccountInfo> users) {
        for (PolarAccountInfo user : users) {
            String password = PolarPrivManager.getInstance().encryptPassword(user.getPassword());
            user.setPassword(password);
        }
    }

    public static PolarAccountInfo getMatchGranter(ServerConnection c) {
        PolarAccountInfo userInfo =
            ((PolarPrivileges) c.getPrivileges()).checkAndGetMatchUser(c.getUser(), c.getHost());
        c.setMatchPolarUserInfo(userInfo);
        return userInfo;
    }
}
