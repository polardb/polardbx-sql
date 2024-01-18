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

package com.alibaba.polardbx.server.util;

import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.net.packet.ErrorPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.util.CharsetUtil;

/**
 * @author xianmao.hexm
 */
public class PacketUtil {

    private static final String LAGIN1 = "latin1";

    public static final ResultSetHeaderPacket getHeader(int fieldCount) {
        ResultSetHeaderPacket packet = new ResultSetHeaderPacket();
        packet.packetId = 1;
        packet.fieldCount = fieldCount;
        return packet;
    }

    public static final FieldPacket getField(String name, String orgName, int type) {
        FieldPacket packet = new FieldPacket();
        packet.charsetIndex = CharsetUtil.getIndex(LAGIN1);
        packet.name = StringUtil.encode(name, LAGIN1);
        packet.orgName = StringUtil.encode(orgName, LAGIN1);
        packet.type = (byte) type;
        return packet;
    }

    public static final FieldPacket getField(String name, int type) {
        FieldPacket packet = new FieldPacket();
        packet.charsetIndex = CharsetUtil.getIndex(LAGIN1);
        packet.name = StringUtil.encode(name, LAGIN1);
        packet.type = (byte) type;
        return packet;
    }

    public static final ErrorPacket getShutdown() {
        ErrorPacket error = new ErrorPacket();
        error.sqlState = "08S01".getBytes();
        error.packetId = 1;
        error.errno = ErrorCode.ER_SERVER_SHUTDOWN.getCode();
        error.message = "The server has been shutdown".getBytes();
        return error;
    }

    public static final ErrorPacket getLock() {
        ErrorPacket error = new ErrorPacket();
        error.sqlState = "08S01".getBytes();
        error.packetId = 1;
        error.errno = ErrorCode.ER_SERVER_SHUTDOWN.getCode();
        error.message = "The server has been locked".getBytes();
        return error;
    }

}
