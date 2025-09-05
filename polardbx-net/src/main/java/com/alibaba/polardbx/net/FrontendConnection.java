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

package com.alibaba.polardbx.net;

import com.alibaba.polardbx.Capabilities;
import com.alibaba.polardbx.Commands;
import com.alibaba.polardbx.Versions;
import com.alibaba.polardbx.common.audit.AuditAction;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.gms.privilege.PolarPrivUtil;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.handler.FrontendAuthenticator;
import com.alibaba.polardbx.net.handler.FrontendAuthorityAuthenticator;
import com.alibaba.polardbx.net.handler.LoadDataHandler;
import com.alibaba.polardbx.net.handler.NIOHandler;
import com.alibaba.polardbx.net.handler.Privileges;
import com.alibaba.polardbx.net.handler.QueryHandler;
import com.alibaba.polardbx.net.handler.StatementHandler;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.ErrorPacket;
import com.alibaba.polardbx.net.packet.HandshakePacket;
import com.alibaba.polardbx.net.packet.OkPacket;
import com.alibaba.polardbx.net.util.AuditUtil;
import com.alibaba.polardbx.net.util.CharsetUtil;
import com.alibaba.polardbx.net.util.MySQLMessage;
import com.alibaba.polardbx.net.util.RandomUtil;
import com.alibaba.polardbx.net.util.TimeUtil;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author xianmao.hexm
 */
public abstract class FrontendConnection extends AbstractConnection {

    private static final Logger logger = LoggerFactory.getLogger(FrontendConnection.class);

    protected String instanceId;

    protected long id;
    protected String host;                                     // ip
    protected int port;
    protected int localPort;
    protected long idleTimeout;
    protected String resultSetCharset;
    protected int resultSetCharsetIndex;
    protected String connectionCharset;
    protected int connectionCharsetIndex;
    protected byte[] seed;
    protected String user;
    protected boolean isPolardbxRoot;
    protected String schema;
    protected boolean trustLogin = false;
    protected NIOHandler handler;
    protected Privileges privileges;
    protected QueryHandler queryHandler;
    protected StatementHandler stmtHandler;
    protected boolean isAccepted;
    protected boolean isAuthenticated;
    protected boolean isManaged = false;
    protected boolean isAllowManagerLogin = true;
    private boolean needReconnect = false;
    protected String buildMDCCache;

    // added by chenghui.lch
    protected long clientFlags = 0;
    protected boolean clientMultiStatements = false;
    protected String authSchema;
    protected long packetCompressThreshold = 16000000L;

    protected LoadDataHandler loadDataHandler;

    private long lastActiveTime = System.nanoTime();
    private long sqlBeginTimestamp = System.currentTimeMillis();

    private byte[] bigPackData;
    private int bigPackLength;

    protected final AtomicReference<Future<?>> executingFuture = new AtomicReference<>();
    protected volatile boolean isBinlogDumpConn = false;
    protected com.alibaba.polardbx.common.exception.code.ErrorCode futureCancelErrorCode;

    private PolarAccountInfo matchPolarUserInfo = null;

    protected volatile boolean rescheduled;
    private final boolean isLoopAddress;
    /**
     * 一个Mysql 数据包上限,mysql 版本4.0.8 以上
     */
    private int packageLimit = 16777215;

    public long getLastActiveTime() {
        return lastActiveTime;
    }

    public void setLastActiveTime(long lastActiveTime) {
        this.lastActiveTime = lastActiveTime;
    }

    public long getPacketCompressThreshold() {
        return packetCompressThreshold;
    }

    public byte getNewPacketId() {
        return ++packetId;
    }

    public void setPacketId(byte packetId) {
        this.packetId = packetId;
    }

    public static String getServerVersion() {
        return InstanceVersion.getFullVersion();
    }

    public FrontendConnection(SocketChannel channel) {
        super(channel);
        Socket socket = channel.socket();
        this.host = socket.getInetAddress().getHostAddress();
        this.isLoopAddress = socket.getInetAddress().isLoopbackAddress();
        this.port = socket.getPort();
        this.localPort = socket.getLocalPort();
        this.handler = createFrontendAuthenticator(this);
        this.id = genConnId();
    }

    protected abstract long genConnId();

    public FrontendAuthenticator createFrontendAuthenticator(FrontendConnection conn) {
        if (isPrivilegeMode()) {
            return new FrontendAuthorityAuthenticator(conn);
        } else {
            return new FrontendAuthenticator(conn);
        }
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getLocalPort() {
        return localPort;
    }

    public void setLocalPort(int localPort) {
        this.localPort = localPort;
    }

    public long getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public boolean isIdleTimeout() {
        return TimeUtil.currentTimeMillis() > Math.max(lastWriteTime, lastReadTime) + idleTimeout;
    }

    public void setAccepted(boolean isAccepted) {
        this.isAccepted = isAccepted;
    }

    public void setProcessor(NIOProcessor processor) {
        this.processor = processor;
        this.readBuffer = allocate();
        processor.addFrontend(this);
    }

    public void setHandler(NIOHandler handler) {
        this.handler = handler;
    }

    public void setQueryHandler(QueryHandler queryHandler) {
        this.queryHandler = queryHandler;
    }

    public QueryHandler getQueryHandler() {
        return this.queryHandler;
    }

    public void setStmtHandler(StatementHandler stmtHandler) {
        this.stmtHandler = stmtHandler;
    }

    public void setAuthenticated(boolean isAuthenticated) {
        this.isAuthenticated = isAuthenticated;
    }

    public boolean isLoopAddress() {
        return isLoopAddress;
    }

    public boolean isTrustLogin() {
        return trustLogin;
    }

    public void setTrustLogin(boolean trustLogin) {
        this.trustLogin = trustLogin;
    }

    public Privileges getPrivileges() {
        return privileges;
    }

    public void setPrivileges(Privileges privileges) {
        this.privileges = privileges;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
        this.isPolardbxRoot = PolarPrivUtil.isPolarxRootUser(user);
    }

    public boolean isPolardbxRoot() {
        return this.isPolardbxRoot;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public byte[] getSeed() {
        return seed;
    }

    public int getResultSetCharsetIndex() {
        return resultSetCharsetIndex;
    }

    public long getClientFlags() {
        return clientFlags;
    }

    public void setClientFlags(long clientFlags) {
        this.clientFlags = clientFlags;

        // 根据clientFlags的upper 2 bytes 的值来设置clientMultiStatements的值
        clientMultiStatements = ((this.clientFlags >>> 16) & 0x01) != 0;
        compressProto = (this.clientFlags & Capabilities.CLIENT_COMPRESS) != 0;
    }

    public boolean isEofDeprecated() {
        return (clientFlags & Capabilities.CLIENT_DEPRECATE_EOF) > 0;
    }

    public boolean isManaged() {
        return isManaged;
    }

    public void setManaged(boolean isManaged) {
        this.isManaged = isManaged;
    }

    public boolean isAllowManagerLogin() {
        return isAllowManagerLogin;
    }

    public void setAllowManagerLogin(boolean isAllowManagerLogin) {
        this.isAllowManagerLogin = isAllowManagerLogin;
    }

    public String getResultSetCharset() {
        return resultSetCharset;
    }

    public String getConnectionCharset() {
        return connectionCharset;
    }

    public synchronized void setIsBinlogDumpConn(boolean b) {
        this.isBinlogDumpConn = b;
    }

    public boolean setCharsetIndex(int ci) {
        String charset = CharsetUtil.getCharset(ci);
        if (charset != null) {
            this.resultSetCharset = charset;
            this.resultSetCharsetIndex = ci;
            this.connectionCharset = charset;
            this.connectionCharsetIndex = ci;
            return true;
        } else {
            return false;
        }
    }

    public boolean setResultSetCharsetIndex(int ci) {
        String charset = CharsetUtil.getCharset(ci);
        if (charset != null) {
            this.resultSetCharset = charset;
            this.resultSetCharsetIndex = ci;
            return true;
        } else {
            return false;
        }
    }

    public boolean setConnectionCharsetIndex(int ci) {
        String charset = CharsetUtil.getCharset(ci);
        if (charset != null) {
            this.connectionCharset = charset;
            this.connectionCharsetIndex = ci;
            return true;
        } else {
            return false;
        }
    }

    public boolean setCharset(String charset) {
        int ci = CharsetUtil.getIndex(charset);
        if (ci > 0) {
            this.resultSetCharset = charset;
            this.resultSetCharsetIndex = ci;
            this.connectionCharset = charset;
            this.connectionCharsetIndex = ci;
            return true;
        } else {
            return false;
        }
    }

    public boolean setResultSetCharset(String charset) {
        int ci = CharsetUtil.getIndex(charset);
        if (ci > 0) {
            this.resultSetCharset = charset;
            this.resultSetCharsetIndex = ci;
            return true;
        } else {
            return false;
        }
    }

    public boolean setConnectionCharset(String charset) {
        int ci = CharsetUtil.getIndex(charset);
        if (ci > 0) {
            this.connectionCharset = charset;
            this.connectionCharsetIndex = ci;
            return true;
        } else {
            return false;
        }
    }

    public boolean isNeedReconnect() {
        return needReconnect;
    }

    public void setNeedReconnect(boolean needReconnect) {
        this.needReconnect = needReconnect;
    }

    public PolarAccountInfo getMatchPolarUserInfo() {
        return this.matchPolarUserInfo;
    }

    public void setMatchPolarUserInfo(PolarAccountInfo matchPolarUserInfo) {
        this.matchPolarUserInfo = matchPolarUserInfo;
    }

    public void writeErrMessage(ErrorCode errorCode, String msg) {
        writeErrMessage(this.getNewPacketId(), errorCode.getCode(), null, msg);
    }

    public void writeErrMessage(int errno, String sqlState, String msg) {
        writeErrMessage(this.getNewPacketId(), errno, sqlState, msg);
    }

    public void writeErrMessage(byte id, int errno, String sqlState, String msg) {
        ErrorPacket err = new ErrorPacket();
        err.packetId = id;
        err.errno = errno;
        err.sqlState = encodeString(sqlState, resultSetCharset);
        err.message = encodeString(msg, resultSetCharset);
        err.write(PacketOutputProxyFactory.getInstance().createProxy(this));
    }

    // commands --------------------------------------------------------------
    public void initDB(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        mm.position(5);
        String db = mm.readString();

        // 检查schema的有效性
        if (db == null || !privileges.schemaExists(db)) {
            writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + db + "'");
            return;
        }

        if (trustLogin) {
            this.schema = db;
            this.updateMDC();
            PacketOutputProxyFactory.getInstance().createProxy(this).writeArrayAsPacket(OkPacket.OK);
            return;
        }

        boolean userExists = privileges.userExists(user, host);
        if (!userExists || !privileges.checkQuarantine(user, this.getHost())) {
            writeErrMessage(ErrorCode.ER_ACCESS_DENIED_ERROR,
                "Access denied for user '" + user + "'@'" + this.getHost() + "'");
            return;
        }

        Set<String> schemas;
        if (isPrivilegeMode()) {
            schemas = privileges.getUserSchemas(user, host);
        } else {
            schemas = privileges.getUserSchemas(user);
        }
        if (schemas != null && schemas.contains(db)) {
            this.schema = db;
            this.updateMDC();
            PacketOutputProxyFactory.getInstance().createProxy(this).writeArrayAsPacket(OkPacket.OK);
        } else {
            this.schema = db;
            PacketOutputProxyFactory.getInstance().createProxy(this).writeArrayAsPacket(OkPacket.OK);
        }
    }

    public abstract void fieldList(byte[] data);

    public void query(byte[] data) {
        // 取得查询语句
        MySQLMessage mm = new MySQLMessage(data);
        mm.position(5);

        String javaCharset = CharsetUtil.getJavaCharset(connectionCharset);
        Charset cs = null;
        if (Charset.isSupported(javaCharset)) {
            try {
                cs = Charset.forName(javaCharset);
            } catch (Exception ex) {
                // do nothing
            }
        }
        if (cs == null) {
            writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown charset '" + connectionCharset + "'");
            return;
        }
        if (mm.position() == mm.length()) {
            writeErrMessage(ErrorCode.ER_NOT_ALLOWED_COMMAND, "Empty SQL");
            return;
        }

        // 执行查询
        if (queryHandler != null) {
            queryHandler.queryRaw(mm.bytes(), mm.position(), mm.length() - mm.position(), cs);
        } else {
            writeErrMessage(ErrorCode.ER_YES, "Empty QueryHandler");
        }
    }

    public void ping() {
        PacketOutputProxyFactory.getInstance().createProxy(this).writeArrayAsPacket(OkPacket.OK);
    }

    public void kill(byte[] data) {
        writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
    }

    public void stmtPrepare(byte[] data) {
        if (stmtHandler != null) {
            stmtHandler.prepare(data);
        } else {
            writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
        }
    }

    public void stmtExecute(byte[] data) {
        if (stmtHandler != null) {
            stmtHandler.execute(data);
        } else {
            writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
        }
    }

    public void stmtReset(byte[] data) {
        if (stmtHandler != null) {
            stmtHandler.reset(data);
        } else {
            writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
        }
    }

    public void stmtClose(byte[] data) {
        if (stmtHandler != null) {
            stmtHandler.close(data);
        } else {
            writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
        }
    }

    public void stmtFetch(byte[] data) {
        if (stmtHandler != null) {
            stmtHandler.fetchData(data);
        } else {
            writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
        }
    }

    public void stmtSendLongData(byte[] data) {
        if (stmtHandler != null) {
            stmtHandler.send_long_data(data);
        } else {
            writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
        }
    }

    public void setOption(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        mm.position(5);
        int option = 0;
        try {
            option = mm.readUB2();
            if (option == 0) {
                clientMultiStatements = true;
            } else if (option == 1) {
                clientMultiStatements = false;
            }
            // return eof packet
            EOFPacket eof = new EOFPacket();
            eof.packetId = 1;
            eof.warningCount = option;
            eof.write(PacketOutputProxyFactory.getInstance().createProxy(this, allocate()));
        } catch (Throwable e) {
            writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Com Set Option Error");
            return;
        }
    }

    public void unknown(byte[] data) {
        writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
    }

    @Override
    protected void idleCheck() {
        buildMDC();
        if (isIdleTimeout()) {
            logger.warn("idle timeout");
            close();
        }
    }

    @Override
    public void register(Selector selector) throws IOException {
        super.register(selector);
        if (!isClosed.get()) {
            // 生成认证数据
            byte[] rand1 = RandomUtil.randomBytes(8);
            byte[] rand2 = RandomUtil.randomBytes(12);

            // 保存认证数据
            byte[] seed = new byte[rand1.length + rand2.length];
            System.arraycopy(rand1, 0, seed, 0, rand1.length);
            System.arraycopy(rand2, 0, seed, rand1.length, rand2.length);
            this.seed = seed;

            // 发送握手数据包
            HandshakePacket hs = new HandshakePacket();
            hs.packetId = 0;
            hs.protocolVersion = Versions.PROTOCOL_VERSION;
            hs.serverVersion = InstanceVersion.getVersion().getBytes();
            hs.threadId = id;
            hs.seed = rand1;
            hs.serverCapabilities = getServerCapabilities();
            if (sslHandler != null) {
                hs.serverCapabilities |= Capabilities.CLIENT_SSL;
            }
            hs.serverCharsetIndex = (byte) (resultSetCharsetIndex & 0xff);
            hs.serverStatus = 2;
            hs.restOfScrambleBuff = rand2;
            hs.write(PacketOutputProxyFactory.getInstance().createProxy(this));
        }
    }

    protected void showCloseInfo(Throwable t) {
        logger.warn("Connection force closed.", t);
    }

    @Override
    public void handleData(byte[] data) {

        if (data.length < 4) {
            throw new IllegalAccessError("impossible packet length, packet:" + data);
        }

        this.setPacketId(data[3]);
        int length = data[0] & 0xff;
        length |= (data[1] & 0xff) << 8;
        length |= (data[2] & 0xff) << 16;

        /**
         * 这里直接通过内存操作，不使用bytebuffer的原因，bytebuffer 对于大包会直接释放，第二，由于数据处理是异步过程，会多一次数据拷贝。
         */
        if (length >= packageLimit) {
            if (bigPackData == null) {
                bigPackData = new byte[packageLimit + 4];
                bigPackLength = 4;
            }

            if (bigPackData.length < bigPackLength + length) {
                byte[] tmp = new byte[bigPackData.length + length];
                System.arraycopy(bigPackData, 0, tmp, 0, bigPackData.length);
                bigPackData = tmp;
            }
            System.arraycopy(data, 4, bigPackData, bigPackLength, length);
            bigPackLength += length;
        } else {
            byte[] tmpData = null;
            if (bigPackData != null) {
                if (length > 0) {
                    byte[] tmp = new byte[bigPackData.length + length];
                    System.arraycopy(bigPackData, 0, tmp, 0, bigPackData.length);
                    bigPackData = tmp;
                    System.arraycopy(data, 4, bigPackData, bigPackLength, length);
                    bigPackLength += length;
                }
                tmpData = bigPackData;
                tmpData[3] = this.packetId;
                bigPackData = null;

            } else {
                tmpData = data;
            }
            final byte[] finalData = tmpData;

            if (loadDataHandler != null) {
                if (loadDataHandler.isStart()) {
                    // load data 最后会发一个end包，byte[4]: 0,0,0,packId
                    if (finalData.length == 4 && finalData[0] == 0 && finalData[1] == 0 && finalData[2] == 0) {
                        loadDataHandler.setPacketId(finalData[3]);
                        loadDataHandler.end();
                        return;
                    } else if (finalData[3] == (byte) (loadDataHandler.getPacketId() + (byte) 1)) {
                        // 必须是load data 数据流，否则走 handler.handle(finalData)逻辑，packetId每次递增
                        loadDataHandler.setPacketId(finalData[3]);
                        loadDataHandler.putData(finalData);
                        return;
                    }
                } else if (loadDataHandler.throwError() != null) {
                    handleError(ErrorCode.ERR_HANDLE_DATA, loadDataHandler.throwError());
                    close();
                }
            }

            // schema maybe null
            final Future<?> previousFuture = this.executingFuture.get();
            // Ensure futureCancelErrorCode is reset
            this.futureCancelErrorCode = null;
            final Future<?> task = processor.getHandler().submit(this.schema, null, processor.getIndex(), () -> {
                // move all waits into try block, and return error when any exception occurs
                boolean closeConnection = false;
                try {
                    // 如果当前connection中上一个请求是binlog dump请求，则不要等待以及处理请求，因为binlog dump请求无意外情况不会结束
                    // 如果等待，会造成线程泄漏
                    if (previousFuture != null) {
                        if (isBinlogDumpConn) {
                            logger.warn("command type:" + finalData[4]
                                + ", previous future is binlog dump, will not handle this request!");
                            return;
                        } else {
                            try {
                                previousFuture.get();
                            } catch (Throwable ex) {
                                logger.warn("error during waiting for previous command", ex);
                            }
                        }
                    }

                    if (rescheduled) {
                        closeConnection = true;
                        if (data.length >= 5 && data[4] == Commands.COM_QUIT) {
                            throw new TddlNestableRuntimeException("Connection closed.");
                        } else {
                            throw new TddlNestableRuntimeException(
                                "The query is cancelled because the previous query is being rescheduling on this connection.");
                        }
                    }
                    handler.handle(finalData);
                } catch (Throwable e) {
                    if (closeConnection) {
                        close();
                        showCloseInfo(e);
                    } else {
                        handleError(ErrorCode.ERR_HANDLE_DATA, e);
                    }
                }
            });
            this.executingFuture.compareAndSet(previousFuture, task);
        }
    }

    public abstract boolean prepareLoadInfile(String sql);

    public abstract void binlogDump(byte[] data);

    protected int getServerCapabilities() {
        int flag = 0;
        flag |= Capabilities.CLIENT_LONG_PASSWORD;
        flag |= Capabilities.CLIENT_FOUND_ROWS;
        flag |= Capabilities.CLIENT_LONG_FLAG;
        flag |= Capabilities.CLIENT_CONNECT_WITH_DB;
        // flag |= Capabilities.CLIENT_NO_SCHEMA;
        flag |= Capabilities.CLIENT_COMPRESS;
        flag |= Capabilities.CLIENT_ODBC;
        // flag |= Capabilities.CLIENT_LOCAL_FILES;
        flag |= Capabilities.CLIENT_IGNORE_SPACE;
        flag |= Capabilities.CLIENT_PROTOCOL_41;
        flag |= Capabilities.CLIENT_INTERACTIVE;
        // flag |= Capabilities.CLIENT_SSL;
        flag |= Capabilities.CLIENT_IGNORE_SIGPIPE;
        flag |= Capabilities.CLIENT_TRANSACTIONS;
        // flag |= ServerDefs.CLIENT_RESERVED;
        flag |= Capabilities.CLIENT_SECURE_CONNECTION;

        // modified by chenghui.lch for
        flag |= Capabilities.CLIENT_MULTI_STATEMENTS;
        flag |= Capabilities.CLIENT_MULTI_RESULTS;
        // flag |= Capabilities.CLIENT_PS_MULTI_RESULTS;
        flag |= Capabilities.CLIENT_PLUGIN_AUTH;
        if (DynamicConfig.getInstance().enableDeprecateEof()) {
            flag |= Capabilities.CLIENT_DEPRECATE_EOF;
        }
        return flag;
    }

    @Override
    public String toString() {
        return new StringBuilder().append("[host=")
            .append(host)
            .append(",port=")
            .append(port)
            .append(",schema=")
            .append(schema)
            .append(']')
            .toString();
    }

    public void updateMDC() {
        StringBuilder builder = new StringBuilder();
        builder.append("user=")
            .append(user)
            .append(",host=")
            .append(host)
            .append(",port=")
            .append(port)
            .append(",schema=")
            .append(schema);
        buildMDCCache = builder.toString();

        if (schema != null && !ConfigDataMode.isFastMock()) {
            // Avoid printing too many log in mock mode
            MDC.put(MDC.MDC_KEY_APP, schema.toLowerCase()); // 设置schema上下文
        }
        MDC.put(MDC.MDC_KEY_CON, buildMDCCache);
    }

    public void buildMDC() {
        if (buildMDCCache == null) {
            StringBuilder builder = new StringBuilder();
            builder.append("user=")
                .append(user)
                .append(",host=")
                .append(host)
                .append(",port=")
                .append(port)
                .append(",schema=")
                .append(schema);
            buildMDCCache = builder.toString();
        }
        if (schema != null && !ConfigDataMode.isFastMock()) {
            // Avoid printing too many log in mock mode
            MDC.put(MDC.MDC_KEY_APP, schema.toLowerCase()); // 设置schema上下文
        }
        MDC.put(MDC.MDC_KEY_CON, buildMDCCache);
    }

    public void clearMDC() {
        MDC.remove(MDC.MDC_KEY_APP);
    }

    public final static byte[] encodeString(String src, String charset) {
        if (src == null) {
            return null;
        }

        charset = CharsetUtil.getJavaCharset(charset);
        if (charset == null) {
            return src.getBytes();
        }

        try {
            return src.getBytes(charset);
        } catch (UnsupportedEncodingException e) {
            return src.getBytes();
        }
    }

    protected boolean isConnectionReset(Throwable t) {
        if (t instanceof IOException) {
            String msg = t.getMessage();
            return (msg != null && msg.contains("Connection reset by peer"));
        }
        return false;
    }

    protected boolean isColumnNotFount(Throwable t) {
        String msg = t.getMessage();
        if (msg != null && msg.contains("not found in any table") && msg.contains("Column")) {
            return true;
        }
        return false;
    }

    protected boolean isTableNotFount(Throwable t) {
        String msg = t.getMessage();
        if (msg != null && msg.contains("doesn't exist") && msg.contains("Table")) {
            return true;
        }
        return false;
    }

    public void refresh() {

    }

    public abstract boolean checkConnectionCount();

    @Override
    public boolean close() {
        if (super.close()) {
            this.getProcessor().getFrontends().remove(this.getId());
            this.cleanup();
            return true;
        } else {
            return false;
        }
    }

    @Override
    protected void logout() {
        AuditUtil.logAuditInfo(getInstanceId(), getSchema(),
            getUser(), getHost(), getPort(), AuditAction.LOGOUT);
    }

    public abstract void addConnectionCount();

    public void setAuthSchema(String authSchema) {
        this.authSchema = authSchema;
    }

    public abstract boolean isPrivilegeMode();

    public long getSqlBeginTimestamp() {
        return sqlBeginTimestamp;
    }

    public void setSqlBeginTimestamp(long sqlBeginTimestamp) {
        this.sqlBeginTimestamp = sqlBeginTimestamp;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

}
