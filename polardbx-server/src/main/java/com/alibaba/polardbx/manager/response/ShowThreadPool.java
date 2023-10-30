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

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.Fields;
import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.mpp.execution.PriorityExecutorInfo;
import com.alibaba.polardbx.executor.mpp.execution.TaskExecutor;
import com.alibaba.polardbx.manager.ManagerConnection;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.server.util.IntegerUtil;
import com.alibaba.polardbx.server.util.LongUtil;
import com.alibaba.polardbx.server.util.PacketUtil;
import com.alibaba.polardbx.server.util.StringUtil;

import java.util.LinkedList;
import java.util.List;

/**
 * 查看线程池状态
 *
 * @author xianmao.hexm 2010-9-26 下午04:08:39
 * @author wenfeng.cenwf 2011-4-25
 */
public final class ShowThreadPool {

    private static final int FIELD_COUNT = 6;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("NAME", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("POOL_SIZE", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("ACTIVE_COUNT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("TASK_QUEUE_SIZE", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("COMPLETED_TASK", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("TOTAL_TASK", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        eof.packetId = ++packetId;
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
        List<ServerThreadPool> executors = getExecutors();
        for (ServerThreadPool exec : executors) {
            if (exec != null) {
                RowDataPacket row = getRow(exec, c.getCharset());
                row.packetId = ++packetId;
                proxy = row.write(proxy);
            }
        }

        if (ServiceProvider.getInstance().getServer() != null) {
            TaskExecutor priorityExecutor =
                ServiceProvider.getInstance().getServer().getTaskExecutor();
            RowDataPacket lowRow = getPriorityExecutorInfo(priorityExecutor.getLowPriorityInfo(), c.getCharset());
            lowRow.packetId = ++packetId;
            proxy = lowRow.write(proxy);

            RowDataPacket highRow = getPriorityExecutorInfo(priorityExecutor.getHighPriorityInfo(), c.getCharset());
            highRow.packetId = ++packetId;
            proxy = highRow.write(proxy);
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        proxy = lastEof.write(proxy);

        // write buffer
        proxy.packetEnd();
    }

    private static RowDataPacket getRow(ServerThreadPool exec, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(exec.getPoolName(), charset));
        row.add(IntegerUtil.toBytes(exec.getPoolSize()));
        row.add(IntegerUtil.toBytes(exec.getActiveCount()));
        row.add(IntegerUtil.toBytes(exec.getQueuedCount()));
        row.add(LongUtil.toBytes(exec.getCompletedTaskCount()));
        row.add(LongUtil.toBytes(exec.getTaskCount()));
        return row;
    }

    private static RowDataPacket getPriorityExecutorInfo(PriorityExecutorInfo priorityExecutorInfo, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(priorityExecutorInfo.getName(), charset));
        row.add(IntegerUtil.toBytes(priorityExecutorInfo.getPoolSize()));
        // active split = pending split(waiting for schedule) + running split
        row.add(
            IntegerUtil.toBytes(priorityExecutorInfo.getPendingSplitsSize() + priorityExecutorInfo.getActiveCount()));
        row.add(IntegerUtil.toBytes(priorityExecutorInfo.getBlockedSplitSize()));
        row.add(LongUtil.toBytes(priorityExecutorInfo.getCompletedTaskCount()));
        row.add(LongUtil.toBytes(priorityExecutorInfo.getTotalTask()));
        return row;
    }

    private static List<ServerThreadPool> getExecutors() {
        List<ServerThreadPool> list = new LinkedList<ServerThreadPool>();
        CobarServer server = CobarServer.getInstance();
        list.add(server.getSyncExecutor());
        list.add(server.getManagerExecutor());
        list.add(server.getServerExecutor());
        list.add(server.getKillExecutor());
        return list;
    }
}
