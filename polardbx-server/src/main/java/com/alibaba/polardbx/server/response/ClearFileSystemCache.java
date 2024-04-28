package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.executor.ddl.job.meta.CommonMetaChanger;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.OkPacket;
import com.alibaba.polardbx.server.ServerConnection;
import org.jetbrains.annotations.Nullable;

public final class ClearFileSystemCache {

    public static boolean response(ServerConnection c, @Nullable Engine engine, boolean all, boolean hasMore) {

        if (all) {
            CommonMetaChanger.clearFileSystemCache(null, true);
            CommonMetaChanger.invalidateBufferPool();
        } else {
            CommonMetaChanger.clearFileSystemCache(engine, false);
        }

        PacketOutputProxyFactory.getInstance().createProxy(c)
            .writeArrayAsPacket(hasMore ? OkPacket.OK_WITH_MORE : OkPacket.OK);
        return true;
    }
}
