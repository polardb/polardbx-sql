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

package com.alibaba.polardbx.net.sample.handle;

import com.alibaba.polardbx.Fields;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.net.sample.net.SampleConnection;
import com.alibaba.polardbx.net.util.CharsetUtil;

import java.io.UnsupportedEncodingException;

/**
 * 基于MySQL协议的返回数据包[header|field,field,...|eof|row,row,...|eof]
 *
 * @author xianmao.hexm
 */
public class SampleResponse {

    public static void response(SampleConnection c) {
        byte packetId = 0;
        ByteBufferHolder buffer = c.allocate();
        IPacketOutputProxy proxy;

        proxy = PacketOutputProxyFactory.getInstance().createProxy(c, buffer);

        // header
        ResultSetHeaderPacket header = new ResultSetHeaderPacket();
        header.packetId = ++packetId;
        header.fieldCount = 1;
        header.write(proxy);

        // fields
        FieldPacket[] fields = new FieldPacket[header.fieldCount];
        for (FieldPacket field : fields) {
            field = new FieldPacket();
            field.packetId = ++packetId;
            field.charsetIndex = CharsetUtil.getIndex("Cp1252");
            field.name = "SampleServer".getBytes();
            field.type = Fields.FIELD_TYPE_VAR_STRING;
            field.write(proxy);
        }

        // eof
        if (!c.isEofDeprecated()) {
            EOFPacket eof = new EOFPacket();
            eof.packetId = ++packetId;
            proxy = eof.write(proxy);
        }

        // rows
        RowDataPacket row = new RowDataPacket(header.fieldCount);
        row.add(encode("HelloWorld!", c.getResultSetCharset()));
        row.packetId = ++packetId;
        proxy = row.write(proxy);

        // write lastEof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        proxy = lastEof.write(proxy);

        // write buffer
        c.write(buffer);
    }

    private static byte[] encode(String src, String charset) {
        if (src == null) {
            return null;
        }

        charset = CharsetUtil.getJavaCharset(charset);
        try {
            return src.getBytes(charset);
        } catch (UnsupportedEncodingException e) {
            // log something
            return src.getBytes();
        }
    }

}
