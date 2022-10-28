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

package com.alibaba.polardbx.net.handler;

import com.alibaba.polardbx.Capabilities;
import com.alibaba.polardbx.Commands;
import com.alibaba.polardbx.ErrorCode;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.packet.AuthPacket;
import com.alibaba.polardbx.net.packet.AuthSwitchResponsePacket;
import com.alibaba.polardbx.net.packet.QuitPacket;
import com.alibaba.polardbx.common.audit.AuditAction;
import com.alibaba.polardbx.net.util.AuditUtil;
import com.taobao.tddl.common.privilege.EncrptPassword;
import com.alibaba.polardbx.common.utils.encrypt.SecurityUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;

import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Set;

/**
 * 前端授权认证处理器
 *
 * @author arnkore 2016-11-14 19:51
 */
public class FrontendAuthorityAuthenticator extends FrontendAuthenticator implements NIOHandler {

    private static final Logger logger = LoggerFactory.getLogger(FrontendAuthorityAuthenticator.class);

    public FrontendAuthorityAuthenticator(FrontendConnection source) {
        super(source);
    }

    @Override
    public void handle(byte[] data) {
        // check quit packet
        if (data.length == QuitPacket.QUIT.length && data[4] == Commands.COM_QUIT) {
            source.close();
            return;
        }

        if (isAuthSwitchResponsePacket(data)) {
            AuthSwitchResponsePacket authSwitchResponse = new AuthSwitchResponsePacket();
            authSwitchResponse.read(data);
            auth.password = authSwitchResponse.password;
        } else {
            auth.read(data);
            if (!FrontendAuthenticator.authMethod.equalsIgnoreCase(auth.authMethod)
                && (auth.clientFlags & Capabilities.CLIENT_PLUGIN_AUTH) != 0) {
                sendAuthSwitch(source.getNewPacketId());
                return;
            }
        }

        // 用户匹配，且为免登机器, 此时允许直接登录；
        boolean isTrustedIp = isTrustedIp(source.getHost(), auth.user);
        if (!isTrustedIp) { // 非免登机器需要走接下来的校验逻辑。
            final String host = source.getHost();
            final String user = auth.user;
            final boolean checkUserLoginMaxCount = checkUserLoginMaxCount(user, host);
            if (!checkUserLoginMaxCount) {
                failure(ErrorCode.ER_PASSWORD_NOT_ALLOWED,
                    "The maximum number of login is exceeded, and the login is refused,the allowed maximum number is "
                        + PolarPrivManager.getInstance().getPolarLoginErrConfig().getPasswordMaxErrorCount(user), null);
                return;
            }
            final boolean checkPasswordExpire = checkUserPasswordExpire(user, host);
            if (!checkPasswordExpire) {
                failure(ErrorCode.ER_PASSWORD_NOT_ALLOWED,
                    "The password has been expired since "
                        + PolarPrivManager.getInstance().getPolarLoginErrConfig().getPasswordExpireDateString(user),
                    null);
                return;
            }
            if (!isAllowAuthentication(auth)) {
                // 不允许授权直接返回
                return;
            }
        }

        // set schema added by leiwen.zh
        // JDBC and non-java driver may send some sqls after authentication
        // succeed, for non-java driver, this action may cause failure because
        // of null schema
        setSchema(auth, isTrustedIp);

        // check schema
        switch (checkSchema(auth.database, auth.user, source.getHost(), isTrustedIp)) {
        case ErrorCode.ER_BAD_DB_ERROR:
            failure(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + auth.database + "'", null);
            break;
        case ErrorCode.ER_DBACCESS_DENIED_ERROR:
            String s = "Access denied for user '" + auth.user + "'@'" + source.getHost() + "' to database '"
                + auth.database + "'";
            failure(ErrorCode.ER_DBACCESS_DENIED_ERROR, s, null);
            break;
        default:
            success(auth, isTrustedIp);
        }
    }

    private int checkSchema(String schema, String user, String host, boolean trustLogin) {
        if (schema == null) {
            return 0;
        }

        Privileges privileges = source.getPrivileges();
        if (!privileges.schemaExists(schema)) {
            return ErrorCode.ER_BAD_DB_ERROR;
        }

        if (trustLogin || ConfigDataMode.isFastMock()) {
            return 0;
        }

        Set<String> schemas = privileges.getUserSchemas(user, host);
        if (schemas != null && schemas.contains(schema)) {
            return 0;
        } else {
            return ErrorCode.ER_DBACCESS_DENIED_ERROR;
        }
    }

    /**
     * 校验一下看看是否允许授权
     */
    protected boolean isAllowAuthentication(AuthPacket auth) {
        // 管理员端口的登录权限如果不开放，不允许登录。
        if (source.isManaged() && !source.isAllowManagerLogin()) {
            failure(ErrorCode.ER_ACCESS_DENIED_ERROR,
                "Access denied for user '" + auth.user + "'@'" + source.getHost() + "' "
                    + "because manager login is not allowed",
                "checkManageLogin");
            return false;
        }

        // 检查一下看看连接数是不是过多！
        if (!source.checkConnectionCount()) {
            failure(ErrorCode.ER_CON_COUNT_ERROR, "Too many connections", null);
            return false;
        }

        // 校验用户是否匹配
        if (!checkUserMatches(auth.user, source.getHost())) {
            failure(ErrorCode.ER_ACCESS_DENIED_ERROR, "Access denied for user '" + auth.user + "'@'" + source.getHost()
                + "' because user doesn't match", "checkUserMatches");
            return false;
        }

        // 校验白名单、黑名单、隔离区等
        if (!checkQuarantine(auth.user, source.getHost())) {
            String cause = new StringBuilder().append("checkQuarantine")
                .append("[host=")
                .append(source.getHost())
                .append(",user=")
                .append(auth.user)
                .append(']')
                .toString();
            failure(ErrorCode.ER_ACCESS_DENIED_ERROR, "Access denied for user '" + auth.user + "'@'" + source.getHost()
                + "' because host '" + source.getHost()
                + "' is not in the white list", cause);
            return false;
        }

        // 校验密码
        if (!checkPassword(auth.password, auth.user, source.getHost())) {
            failure(ErrorCode.ER_ACCESS_DENIED_ERROR, "Access denied for user '" + auth.user + "'@'" + source.getHost()
                + "' because password is not correct", "checkPassword");
            return false;
        }

        return true;
    }

    protected boolean checkUserMatches(String user, String host) {
        if (ConfigDataMode.isFastMock()) {
            return true;
        }
        return source.getPrivileges().userMatches(user, host);
    }

    protected boolean checkPassword(byte[] password, String user, String host) {
        EncrptPassword pass = source.getPrivileges().getPassword(user, host);
        // check null
        if (pass == null || pass.getPassword() == null || pass.getPassword().length() == 0) {
            if (password == null || password.length == 0) {
                return true;
            } else {
                return false;
            }
        }

        if (password == null || password.length == 0) {
            return false;
        }

        // encrypt
        byte[] mysqlUserPassword = null;
        try {
            if (pass.isEnc()) {
                // 密码已经是经过sha-1混淆保存的
                mysqlUserPassword = SecurityUtil.sha1Pass(SecurityUtil.hexStr2Bytes(pass.getPassword()));
                SecurityUtil.verify(password, mysqlUserPassword, source.getSeed());
            } else {
                if (pass.getPassword() != null) {
                    mysqlUserPassword = SecurityUtil.calcMysqlUserPassword(pass.getPassword().getBytes());
                }
            }
        } catch (Throwable e) {
            logger.warn(e);
            return false;
        }

        try {
            return SecurityUtil.verify(password, mysqlUserPassword, source.getSeed());
        } catch (NoSuchAlgorithmException e) {
            logger.debug(e.getMessage(), e);
            return false;
        }
    }

    @Override
    protected void setSchema(AuthPacket auth, boolean trustLogin) {
        if (ConfigDataMode.isFastMock()) {
            auth.database = auth.user;
            source.setManaged(false);
            return;
        }
        String user = auth.user;
        if (trustLogin) {
            source.setManaged(true);
        }

        if (user != null) {
            Set<String> schemas = source.getPrivileges().getUserSchemas(user, source.getHost());
            if (schemas != null && auth.database == null) {
                if (schemas.size() == 0) {
                    auth.database = SystemDbHelper.DEFAULT_DB_NAME;
                    source.setManaged(false);
                } else if (schemas.size() == 1) {
                    auth.database = SystemDbHelper.DEFAULT_DB_NAME;
                    source.setManaged(false);
                } else if (schemas.size() > 1) {
                    Object[] array = schemas.toArray();
                    Arrays.sort(array);
                    auth.database = SystemDbHelper.DEFAULT_DB_NAME;
                    source.setManaged(false);
                }
            }
        }
    }

    @Override
    protected void success(AuthPacket auth, boolean trustLogin) {
        source.setAuthenticated(true);
        source.setTrustLogin(trustLogin);
        source.setUser(auth.user);
        source.setSchema(auth.database);
        source.setAuthSchema(auth.database);
        source.setCharsetIndex(auth.charsetIndex);
        source.setClientFlags(auth.clientFlags);
        source.setHandler(new FrontendCommandHandler(source));
        source.addConnectionCount();
        source.updateMDC();
        if (logger.isInfoEnabled()) {
            StringBuilder s = new StringBuilder();
            s.append(source).append('\'').append(auth.user).append("' login success");
            byte[] extra = auth.extra;
            if (extra != null && extra.length > 0) {
                s.append(",extra:").append(new String(extra));
            }
            logger.info(s.toString());
        }
        AuditUtil.logAuditInfo(source.getInstanceId(), auth.database, auth.user, source.getHost(), source.getPort(),
            AuditAction.LOGIN);
        /**
         * 表示server接受此链接，如果不接受压缩就报错，通过抓包看 不论压缩还是非压缩都是 Login
         * Request(带clientFlags) + OK Packet Handshake
         * packet返回强制不能压缩，否则php客户端不认，但mysql console可以
         */
        ByteBufferHolder buffer = source.allocate();
        sendOk(buffer);
    }
}
