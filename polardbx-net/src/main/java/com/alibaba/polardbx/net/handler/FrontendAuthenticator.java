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
import com.alibaba.polardbx.common.audit.AuditAction;
import com.alibaba.polardbx.common.audit.AuditUtils;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.encrypt.SecurityUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.metadb.audit.AuditLogAccessor;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.AuthPacket;
import com.alibaba.polardbx.net.packet.AuthSwitchRequestPacket;
import com.alibaba.polardbx.net.packet.AuthSwitchResponsePacket;
import com.alibaba.polardbx.net.packet.QuitPacket;
import com.taobao.tddl.common.privilege.EncrptPassword;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

/**
 * 前端认证处理器
 *
 * @author xianmao.hexm
 */
public class FrontendAuthenticator implements NIOHandler {

    private static final Logger logger = LoggerFactory.getLogger(FrontendAuthenticator.class);
    public static final byte[] AUTH_OK = new byte[] {7, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0};
    public static final byte[] AUTH_AFTER_SWITCH_OK = new byte[] {7, 0, 0, 4, 0, 0, 0, 2, 0, 0, 0};
    public static final byte[] SSL_AUTH_OK = new byte[] {7, 0, 0, 3, 0, 0, 0, 2, 0, 0, 0};
    public static final byte[] SSL_AUTH_AFTER_SWITCH_OK = new byte[] {7, 0, 0, 5, 0, 0, 0, 2, 0, 0, 0};
    protected final FrontendConnection source;
    public static final String authMethod = "mysql_native_password";
    protected AuthPacket auth = new AuthPacket();
    protected boolean authMethodSwitched = false;

    public FrontendAuthenticator(FrontendConnection source) {
        this.source = source;
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

        if (ConfigDataMode.isFastMock()) {
            setSchema(auth, true);
            success(auth, true);
            return;
        }

        // check if host is in trusted ip, otherwise check password
        boolean trustLogin = isTrustedIp(source.getHost(), auth.user);
        if (!trustLogin) {
            // manage port , but not allow user login
            if (source.isManaged() && !source.isAllowManagerLogin()) {
                failure(ErrorCode.ER_ACCESS_DENIED_ERROR,
                    "Access denied for user '" + auth.user + "'@'" + source.getHost() + "'",
                    "checkManageLogin");
                return;
            }

            if (!source.checkConnectionCount()) {
                failure(ErrorCode.ER_CON_COUNT_ERROR, "Too many connections", null);
                return;
            }

            // check user
            if (!checkUser(auth.user)) {
                failure(ErrorCode.ER_ACCESS_DENIED_ERROR,
                    "Access denied for user '" + auth.user + "'@'" + source.getHost() + "' because user '" + auth.user
                        + "' doesn't exist",
                    "checkUser");
                return;
            }

            // check quarantine
            if (!checkQuarantine(auth.user, source.getHost())) {
                String cause = new StringBuilder().append("checkQuarantine")
                    .append("[host=")
                    .append(source.getHost())
                    .append(",user=")
                    .append(auth.user)
                    .append(']')
                    .toString();
                failure(ErrorCode.ER_ACCESS_DENIED_ERROR,
                    "Access denied for user '" + auth.user + "'@'" + source.getHost() + "' because host '"
                        + source.getHost() + "' is not in the white list",
                    cause);
                return;
            }

            // check password is not null
            if (StringUtils.isEmpty(new String(auth.password))) {
                failure(ErrorCode.ER_ACCESS_DENIED_ERROR,
                    "Access denied for user '" + auth.user + "'@'" + source.getHost() + "' because password is empty",
                    "checkPassword");
                return;
            }
            // check password
            if (!checkPassword(auth.password, auth.user)) {
                failure(ErrorCode.ER_ACCESS_DENIED_ERROR,
                    "Access denied for user '" + auth.user + "'@'" + source.getHost()
                        + "' because password is not correct",
                    "checkPassword");
                return;
            }
        }

        // set schema added by leiwen.zh
        // JDBC and non-java driver may send some sqls after authentication
        // succeed, for non-java driver, this action may cause failure because
        // of null schema
        setSchema(auth, trustLogin);

        // check schema
        switch (checkSchema(auth.database, auth.user, trustLogin)) {
        case ErrorCode.ER_BAD_DB_ERROR:
            failure(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + auth.database + "'", null);
            break;
        case ErrorCode.ER_DBACCESS_DENIED_ERROR:
            String s = "Access denied for user '" + auth.user + "'@'" + source.getHost() + "' to database '"
                + auth.database + "'";
            failure(ErrorCode.ER_DBACCESS_DENIED_ERROR, s, null);
            break;
        default:
            success(auth, trustLogin);
        }
    }

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
            Set<String> schemas = source.getPrivileges().getUserSchemas(user);
            if (schemas != null && schemas.size() == 1 && auth.database == null) {
                auth.database = schemas.iterator().next();
                source.setManaged(false);// not managerd
            }
        }
    }

    protected boolean checkUser(String user) {
        return source.getPrivileges().userExists(user);
    }

    protected boolean checkQuarantine(String user, String host) {
        return source.getPrivileges().checkQuarantine(user, host);
    }

    protected boolean isTrustedIp(String host, String user) {
        return source.getPrivileges().isTrustedIp(host, user);
    }

    protected boolean checkPassword(byte[] password, String user) {
        EncrptPassword pass = source.getPrivileges().getPassword(user);
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
        byte[] encryptPass = null;
        try {
            if (pass.isEnc()) {
                // 密码已经是经过sha-1混淆保存的
                byte[] passbytes = SecurityUtil.hexStr2Bytes(pass.getPassword());
                encryptPass = SecurityUtil.scramble411BySha1Pass(passbytes, source.getSeed());
            } else {
                encryptPass = SecurityUtil.scramble411(pass.getPassword().getBytes(), source.getSeed());
            }
        } catch (Throwable e) {
            logger.warn(e);
            return false;
        }

        return checkBytes(encryptPass, password);
    }

    private boolean checkBytes(byte[] encryptPass, byte[] password) {
        if (encryptPass != null && (encryptPass.length == password.length)) {
            int i = encryptPass.length;
            while (i-- != 0) {
                if (encryptPass[i] != password[i]) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    protected int checkSchema(String schema, String user, boolean trustLogin) {
        if (schema == null) {
            return 0;
        }

        Privileges privileges = source.getPrivileges();
        if (!privileges.schemaExists(schema)) {
            return ErrorCode.ER_BAD_DB_ERROR;
        }

        if (trustLogin) {
            return 0;
        }

        Set<String> schemas = privileges.getUserSchemas(user);
        if (schemas == null || schemas.size() == 0 || schemas.contains(schema)) {
            return 0;
        } else {
            return ErrorCode.ER_DBACCESS_DENIED_ERROR;
        }
    }

    protected boolean isAuthSwitchResponsePacket(byte[] data) {
        // response of mysql_native_password always comes with a total packet
        // length of 24 and a packet id of 3 (if ssl enabled the packet id would
        // be 4).
        boolean lengthCheckPassed = data.length == AuthSwitchResponsePacket.totalPacketLength
            || data.length == AuthSwitchResponsePacket.emptyPacketLength;
        if (source.isSslEnable()) {
            return lengthCheckPassed && (data[3] == AuthSwitchResponsePacket.sslRefPacketId);
        } else {
            return lengthCheckPassed && (data[3] == AuthSwitchResponsePacket.refPacketId);
        }
    }

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

        /**
         * 表示server接受此链接，如果不接受压缩就报错，通过抓包看 不论压缩还是非压缩都是 Login
         * Request(带clientFlags) + OK Packet Handshake
         * packet返回强制不能压缩，否则php客户端不认，但mysql console可以
         */
        ByteBufferHolder buffer = source.allocate();
        sendOk(buffer);

    }

    protected void sendOk(ByteBufferHolder buffer) {
        byte[] ok = AUTH_OK;
        if (source.isSslEnable()) {
            if (authMethodSwitched) {
                ok = SSL_AUTH_AFTER_SWITCH_OK;
            } else {
                ok = SSL_AUTH_OK;
            }
        } else {
            if (authMethodSwitched) {
                ok = AUTH_AFTER_SWITCH_OK;
            }
        }
        PacketOutputProxyFactory.getInstance().createProxy(source, buffer, false).writeArrayAsPacket(ok);

    }

    protected void sendAuthSwitch(byte packetId) {
        IPacketOutputProxy proxy = PacketOutputProxyFactory.getInstance().createProxy(source);
        AuthSwitchRequestPacket authSwitchRequestPacket = new AuthSwitchRequestPacket(FrontendAuthenticator.authMethod,
            source.getSeed(),
            packetId);
        authMethodSwitched = true;
        authSwitchRequestPacket.write(proxy);
    }

    protected void failure(int errno, String info, String cause) {
        source.clearMDC();
        if (errno == ErrorCode.ER_DBACCESS_DENIED_ERROR || errno == ErrorCode.ER_ACCESS_DENIED_ERROR) {
            incrementLoginErrorCount(auth.user, source.getHost());
            logAuditInfo(auth.database, auth.user, source.getHost(), source.getPort(), AuditAction.LOGIN_ERR);
        }
        if (errno == ErrorCode.ER_PASSWORD_NOT_ALLOWED) {
            logAuditInfo(auth.database, auth.user, source.getHost(), source.getPort(), AuditAction.LOGIN_ERR);
        }
        if (cause != null) {
            logger.error(info + " caused by " + cause);
        } else {
            logger.error(info);
        }
        if (source.isSslEnable()) {
            if (authMethodSwitched) {
                source.writeErrMessage((byte) 5, errno, null, info);
            } else {
                source.writeErrMessage((byte) 3, errno, null, info);
            }
        } else {
            if (authMethodSwitched) {
                source.writeErrMessage((byte) 4, errno, null, info);
            } else {
                source.writeErrMessage((byte) 2, errno, null, info);
            }
        }
    }

    public boolean checkUserLoginMaxCount(String userName, String host) {
        return PolarPrivManager.getInstance().checkUserLoginErrMaxCount(userName, host);
    }

    public void incrementLoginErrorCount(String userName, String host) {
        PolarPrivManager.getInstance().incrementLoginErrorCount(userName, host);
    }

    public void logAuditInfo(String database, String user, String host, int port, AuditAction action) {
        if (!Boolean.valueOf(System.getProperty(ConnectionProperties.ENABLE_LOGIN_AUDIT_CONFIG))) {
            return;
        }
        // `gmt_create`, `instance_name`, `user_name`, `db_name`, `host`, `action`
        AuditUtils.logAuditInfo(String.valueOf(source.getInstanceId()) + ',' + String.valueOf(user)
            + ',' + String.valueOf(database) + ',' + String.valueOf(host), action);
        AuditLogAccessor auditLogAccessor = new AuditLogAccessor();
        try (Connection conn = MetaDbUtil.getConnection()) {
            auditLogAccessor.setConnection(conn);
            auditLogAccessor.addAuditLog(user, host, port, database, null, action, null);
        } catch (SQLException e) {
            //ignore;
        }
    }
}
