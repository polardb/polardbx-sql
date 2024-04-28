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

package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.OkPacket;
import com.alibaba.polardbx.server.ServerConnection;

/**
 * @author yuehan.wcf
 */
public class ResizeProcedureCache {
    public static boolean response(ByteString stmt, ServerConnection c,
                                   int offset, boolean hasMore) {
        String newSizeStr = stmt.substring(offset).trim();
        try {
            int newSize = Integer.parseInt(newSizeStr);
            SyncManagerHelper.sync(new ResizeProcedureCacheSyncAction(newSize), TddlConstants.INFORMATION_SCHEMA,
                SyncScope.NOT_COLUMNAR_SLAVE);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(String.format("'%s is illegal'", newSizeStr));
        }
        PacketOutputProxyFactory.getInstance().createProxy(c)
            .writeArrayAsPacket(hasMore ? OkPacket.OK_WITH_MORE : OkPacket.OK);
        return true;
    }
}
