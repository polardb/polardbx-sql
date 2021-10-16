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

package com.alibaba.polardbx.net.reverse;

import com.alibaba.polardbx.Capabilities;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.AuthPacket;
import com.alibaba.polardbx.net.packet.BinaryPacket;
import com.alibaba.polardbx.net.packet.CommandPacket;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.ErrorPacket;
import com.alibaba.polardbx.net.packet.HandshakePacket;
import com.alibaba.polardbx.net.packet.MySQLPacket;
import com.alibaba.polardbx.net.packet.OkPacket;
import com.alibaba.polardbx.net.packet.QuitPacket;
import com.alibaba.polardbx.net.packet.Reply323Packet;
import com.alibaba.polardbx.net.reverse.exception.ErrorPacketException;
import com.alibaba.polardbx.net.reverse.exception.UnknownPacketException;
import com.alibaba.polardbx.net.util.CharsetUtil;
import com.alibaba.polardbx.common.utils.encrypt.SecurityUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ManagerChannel {

    private static final Logger logger = LoggerFactory.getLogger(ManagerChannel.class);
    private static final int INPUT_STREAM_BUFFER = 16 * 1024;
    private static final int OUTPUT_STREAM_BUFFER = 8 * 1024;
    private static final long CLIENT_FLAGS = getClientFlags();
    private static final long MAX_PACKET_SIZE = 1024 * 1024 * 16;

    private Socket socket;
    private InputStream in;
    private OutputStream out;
    private long threadId;
    private int charsetIndex;
    private String charset;
    private volatile boolean autocommit;
    private volatile boolean isRunning;
    private final AtomicBoolean isClosed;
    private String userName;
    private String password;

    public ManagerChannel(Socket socket) throws IOException {
        this.autocommit = true;
        this.isRunning = false;
        this.isClosed = new AtomicBoolean(false);

        // 网络IO参数设置
        in = new BufferedInputStream(socket.getInputStream(), INPUT_STREAM_BUFFER);
        out = new BufferedOutputStream(socket.getOutputStream(), OUTPUT_STREAM_BUFFER);
        // 完成连接和初始化
        handshake();
    }

    public String getCharset() {
        return charset;
    }

    public boolean isAutocommit() {
        return autocommit;
    }

    public boolean isRunning() {
        return isRunning;
    }

    public void setRunning(boolean running) {
        this.isRunning = running;
    }

    public long getThreadId() {
        return threadId;
    }

    public BinaryPacket execute(String sql) throws IOException {
        // 生成执行数据包
        CommandPacket packet = new CommandPacket();
        packet.packetId = 0;
        packet.command = MySQLPacket.COM_QUERY;
        packet.arg = sql.getBytes(charset);
        // 递交执行数据包并等待执行返回
        // proxy已经在合理的时候调用flush了
        packet.write(PacketOutputProxyFactory.getInstance().createProxy(out));
        BinaryPacket bin = receive();
        // 记录执行结束时间
        return bin;
    }

    public BinaryPacket receive() throws IOException {
        BinaryPacket bin = new BinaryPacket();
        bin.read(in);
        return bin;
    }

    public boolean isClosed() {
        return isClosed.get();
    }

    public void closeNoActive() {
        if (isClosed.compareAndSet(false, true)) {
            mysqlClose();
        }
    }

    public String getErrMessage(BinaryPacket bin) {
        String message = null;
        ErrorPacket err = new ErrorPacket();
        err.read(bin);
        if (err.message != null) {
            message = decode(err.message, charset);
        }
        return message;
    }

    public String toString() {
        return "";
    }

    private ManagerChannel handshake() throws IOException {
        // 读取握手数据包
        BinaryPacket initPacket = new BinaryPacket();
        initPacket.read(in);
        HandshakePacket hsp = new HandshakePacket();
        hsp.read(initPacket);

        // 设置通道参数
        this.threadId = hsp.threadId;
        int ci = hsp.serverCharsetIndex & 0xff;
        if ((charset = CharsetUtil.getCharset(ci)) != null) {
            this.charsetIndex = ci;
        } else {
            throw new RuntimeException("charset:" + ci);
        }

        // 发送认证数据包
        BinaryPacket bin = null;
        try {
            bin = sendAuth411(hsp);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
        switch (bin.data[0]) {
        case OkPacket.FIELD_COUNT:
            afterSuccess();
            break;
        case ErrorPacket.FIELD_COUNT:
            ErrorPacket err = new ErrorPacket();
            err.read(bin);
            throw new ErrorPacketException(new String(err.message, charset));
        case EOFPacket.FIELD_COUNT:
            auth323(bin.packetId, hsp.seed);
            break;
        default:
            throw new UnknownPacketException(bin.toString());
        }

        return this;
    }

    /**
     * 发送411协议的认证数据包
     */
    private BinaryPacket sendAuth411(HandshakePacket hsp) throws IOException, NoSuchAlgorithmException {
        AuthPacket ap = new AuthPacket();
        ap.packetId = 1;
        ap.clientFlags = CLIENT_FLAGS;
        ap.maxPacketSize = MAX_PACKET_SIZE;
        ap.charsetIndex = charsetIndex;
        ap.user = userName;
        String passwd = password;
        if (passwd != null && passwd.length() > 0) {
            byte[] password = passwd.getBytes(charset);
            byte[] seed = hsp.seed;
            byte[] restOfScramble = hsp.restOfScrambleBuff;
            byte[] authSeed = new byte[seed.length + restOfScramble.length];
            System.arraycopy(seed, 0, authSeed, 0, seed.length);
            System.arraycopy(restOfScramble, 0, authSeed, seed.length, restOfScramble.length);
            ap.password = SecurityUtil.scramble411(password, authSeed);
        }
        ap.write(PacketOutputProxyFactory.getInstance().createProxy(out));
        return receive();
    }

    /**
     * 323协议认证
     */
    private void auth323(byte packetId, byte[] seed) throws IOException {
        Reply323Packet r323 = new Reply323Packet();
        r323.packetId = ++packetId;
        String passwd = password;
        if (passwd != null && passwd.length() > 0) {
            r323.seed = SecurityUtil.scramble323(passwd, new String(seed)).getBytes();
        }
        r323.write(PacketOutputProxyFactory.getInstance().createProxy(out));
        BinaryPacket bin = receive();
        switch (bin.data[0]) {
        case OkPacket.FIELD_COUNT:
            afterSuccess();
            break;
        case ErrorPacket.FIELD_COUNT:
            ErrorPacket err = new ErrorPacket();
            err.read(bin);
            throw new ErrorPacketException(new String(err.message, charset));
        default:
            throw new UnknownPacketException(bin.toString());
        }
    }

    /**
     * 连接和验证成功以后
     */
    private void afterSuccess() throws IOException {
        // 为防止握手阶段字符集编码交互无效，连接成功之后做一次字符集编码同步。
        sendCharset(charsetIndex);
    }

    /**
     * 发送字符集设置
     */
    private void sendCharset(int ci) throws IOException {
        CommandPacket cmd = getCharsetCommand(ci);
        cmd.write(PacketOutputProxyFactory.getInstance().createProxy(out));
        BinaryPacket bin = receive();
        switch (bin.data[0]) {
        case OkPacket.FIELD_COUNT:
            this.charsetIndex = ci;
            this.charset = CharsetUtil.getCharset(ci);
            break;
        case ErrorPacket.FIELD_COUNT:
            ErrorPacket err = new ErrorPacket();
            err.read(bin);
            throw new ErrorPacketException(new String(err.message, charset));
        default:
            throw new UnknownPacketException(bin.toString());
        }
    }

    private CommandPacket getCharsetCommand(int ci) {
        String charset = CharsetUtil.getCharset(ci);
        StringBuilder s = new StringBuilder();
        s.append("SET names ").append(charset);
        CommandPacket cmd = new CommandPacket();
        cmd.packetId = 0;
        cmd.command = MySQLPacket.COM_QUERY;
        cmd.arg = s.toString().getBytes();
        return cmd;
    }

    /**
     * 关闭连接之前先尝试发送quit数据包
     */
    private void mysqlClose() {
        try {
            if (out != null) {
                out.write(QuitPacket.QUIT);
                out.flush();
            }
        } catch (IOException e) {
            logger.error(toString(), e);
        } finally {
            try {
                socket.close();
            } catch (Throwable e) {
                logger.error(toString(), e);
            }
        }
    }

    /**
     * 与MySQL连接时的一些特性指定
     */
    private static long getClientFlags() {
        int flag = 0;
        flag |= Capabilities.CLIENT_LONG_PASSWORD;
        flag |= Capabilities.CLIENT_FOUND_ROWS;
        flag |= Capabilities.CLIENT_LONG_FLAG;
        flag |= Capabilities.CLIENT_CONNECT_WITH_DB;
        // flag |= Capabilities.CLIENT_NO_SCHEMA;
        // flag |= Capabilities.CLIENT_COMPRESS;
        flag |= Capabilities.CLIENT_ODBC;
        // flag |= Capabilities.CLIENT_LOCAL_FILES;
        flag |= Capabilities.CLIENT_IGNORE_SPACE;
        flag |= Capabilities.CLIENT_PROTOCOL_41;
        flag |= Capabilities.CLIENT_INTERACTIVE;
        // flag |= Capabilities.CLIENT_SSL;
        flag |= Capabilities.CLIENT_IGNORE_SIGPIPE;
        flag |= Capabilities.CLIENT_TRANSACTIONS;
        // flag |= Capabilities.CLIENT_RESERVED;
        flag |= Capabilities.CLIENT_SECURE_CONNECTION;
        // client extension
        // flag |= Capabilities.CLIENT_MULTI_STATEMENTS;
        // flag |= Capabilities.CLIENT_MULTI_RESULTS;
        return flag;
    }

    public static String decode(byte[] src, String charset) {
        return decode(src, 0, src.length, charset);
    }

    public static String decode(byte[] src, int offset, int length, String charset) {
        try {
            return new String(src, offset, length, charset);
        } catch (UnsupportedEncodingException e) {
            return new String(src, offset, length);
        }
    }
}
