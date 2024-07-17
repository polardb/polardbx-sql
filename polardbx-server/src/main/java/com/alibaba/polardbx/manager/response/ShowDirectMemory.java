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

package com.alibaba.polardbx.manager.response;

import com.alibaba.polardbx.Fields;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.manager.ManagerConnection;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.server.util.LongUtil;
import com.alibaba.polardbx.server.util.PacketUtil;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicLong;

public class ShowDirectMemory {
    private static final Logger logger = LoggerFactory.getLogger(ShowDirectMemory.class);

    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();

    private static Field directMemoryField = null;

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("DIRECT_MEMORY_SIZE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        eof.packetId = ++packetId;

        try {
            Class<?> bitsClass = Class.forName("java.nio.Bits");
            // jdk8
            directMemoryField = bitsClass.getDeclaredField("totalCapacity");
            directMemoryField.setAccessible(true);
        } catch (NoSuchFieldException | ClassNotFoundException e) {
            logger.warn("Failed to get java.nio.Bits.totalCapacity: " + e.getMessage());
        }
        if (directMemoryField == null) {
            try {
                Class<?> bitsClass = Class.forName("java.nio.Bits");
                // jdk11
                directMemoryField = bitsClass.getDeclaredField("TOTAL_CAPACITY");
                directMemoryField.setAccessible(true);
            } catch (NoSuchFieldException | ClassNotFoundException e) {
                logger.warn("Failed to get java.nio.Bits.TOTAL_CAPACITY: " + e.getMessage());
            }
        }
    }

    public static void execute(ManagerConnection c) {
        ByteBufferHolder buffer = c.allocate();
        IPacketOutputProxy proxy = PacketOutputProxyFactory.getInstance().createProxy(c, buffer);
        proxy.packetBegin();

        // write header
        proxy = header.write(proxy);

        // write fields
        for (FieldPacket field : fields) {
            proxy = field.write(proxy);
        }

        // write eof
        proxy = eof.write(proxy);

        // write rows
        byte packetId = eof.packetId;
        RowDataPacket row = getRow();
        row.packetId = ++packetId;
        proxy = row.write(proxy);

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        proxy = lastEof.write(proxy);

        // write buffer
        proxy.packetEnd();
    }

    private static RowDataPacket getRow() {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(LongUtil.toBytes(getDirectMemorySize()));
        return row;
    }

    public static long getDirectMemorySize() {
        if (directMemoryField != null) {
            try {
                AtomicLong directMemory = (AtomicLong) directMemoryField.get(null);
                return directMemory.get();
            } catch (IllegalAccessException e) {
                logger.warn("Failed to get java.nio.Bits.totalCapacity: " + e.getMessage());
            }
        }
        return 0;
    }
}
